using System.Text.Json;

namespace Lotta.Tests;

public class AutoQueryableTests : IClassFixture<LottaDBFixture>
{
    // === POCO AutoQueryable tests ===

    [Queryable]
    public class AutoPerson
    {
        [Key]
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public string[] Tags { get; set; } = [];
        public bool Active { get; set; }
    }

    public class NonAutoPerson
    {
        [Key]
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    [Fact]
    public async Task AutoQueryable_TypedEntity_AllSimplePropertiesIndexed()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<AutoPerson>();
        }, cancellationToken: ct);

        await db.SaveAsync(new AutoPerson { Id = "p1", Name = "Alice", Age = 30, Active = true, Tags = ["dev", "lead"] }, ct);
        db.ReloadSearcher();

        // Search by name
        var results = db.Search<AutoPerson>("Alice").ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].Name);
    }

    [Fact]
    public async Task AutoQueryable_StringArray_SearchableByElement()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<AutoPerson>();
        }, cancellationToken: ct);

        await db.SaveAsync(new AutoPerson { Id = "p1", Name = "Bob", Tags = ["engineer", "backend"] }, ct);
        db.ReloadSearcher();

        var results = db.Search<AutoPerson>("engineer").ToList();
        Assert.Single(results);
        Assert.Equal("Bob", results[0].Name);
    }

    [Fact]
    public async Task AutoQueryable_EnabledByDefault_ForPOCO()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<NonAutoPerson>();
        }, cancellationToken: ct);

        await db.SaveAsync(new NonAutoPerson { Id = "p1", Name = "Charlie", Age = 25 }, ct);
        db.ReloadSearcher();

        // Search finds by name — AutoQueryable is the default for zero-config POCOs
        var results = db.Search<NonAutoPerson>("Charlie").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task AutoQueryable_StillOnWithExplicitKeyAttribute()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<NonAutoPerson>();
        }, cancellationToken: ct);

        // NonAutoPerson has [Key] on Id but no other config — AutoQueryable is still on
        await db.SaveAsync(new NonAutoPerson { Id = "p1", Name = "SearchMe", Age = 99 }, ct);
        db.ReloadSearcher();

        var results = db.Search<NonAutoPerson>("SearchMe").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task AutoQueryable_ExplicitlyDisabled()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<NonAutoPerson>(s => s.AutoQueryable(false));
        }, cancellationToken: ct);

        await db.SaveAsync(new NonAutoPerson { Id = "p1", Name = "Diana", Age = 40 }, ct);
        db.ReloadSearcher();

        // AutoQueryable explicitly disabled — nothing indexed
        var results = db.Search<NonAutoPerson>("Diana").ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task AutoQueryable_KeyPropertySkipped()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<AutoPerson>();
        }, cancellationToken: ct);

        await db.SaveAsync(new AutoPerson { Id = "unique-key-123", Name = "Test" }, ct);
        db.ReloadSearcher();

        // Searching for the key value in _content_ should NOT find it
        var results = db.Search<AutoPerson>("unique-key-123").ToList();
        Assert.Empty(results);
    }

    // === POJO (JsonSchema) AutoQueryable tests ===

    [Fact]
    public async Task AutoQueryable_JsonSchema_AutoDiscovery()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Define a schema with AutoQueryable=true (default) and no explicit properties
        var schema = new JsonSchema { Name = "people", KeyMode = KeyMode.Auto };
        await db.SaveAsync(schema, ct);

        // Save a document
        var doc = JsonDocument.Parse("""{"name":"Eve","age":28}""");
        doc.SetSchema("people");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        // Search should find by name (auto-indexed)
        var results = db.Search<object>("Eve").OfType<JsonDocument>().ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task AutoQueryable_EnabledByDefault_ForPOJO()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Schema with all defaults (AutoQueryable=true, no properties)
        var schema = new JsonSchema { Name = "items" };
        await db.SaveAsync(schema, ct);

        var doc = JsonDocument.Parse("""{"title":"Hello World","count":5}""");
        doc.SetSchema("items");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        var results = db.Search<object>("Hello").OfType<JsonDocument>().ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task AutoQueryable_POJO_ExplicitProperties_OverrideButAutoStillOn()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Schema with explicit property for "sku" (NotAnalyzed) — AutoQueryable still applies to other fields
        var schema = new JsonSchema
        {
            Name = "products",
            KeyMode = KeyMode.Auto,
            Properties = [new QueryableProperty { Name = "sku", Type = "string", Mode = QueryableMode.NotAnalyzed }]
        };
        await db.SaveAsync(schema, ct);

        var doc = JsonDocument.Parse("""{"sku":"ABC-123","description":"Great product"}""");
        doc.SetSchema("products");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        // "description" IS searchable because AutoQueryable is on by default
        var results = db.Search<object>("Great").OfType<JsonDocument>().ToList();
        Assert.Single(results);

        // "sku" uses explicit NotAnalyzed mode — exact match only, not tokenized
        var exactResults = db.Search(j => j["sku"] == "ABC-123").ToList();
        Assert.Single(exactResults);
    }

    [Fact]
    public async Task AutoQueryable_POJO_ExplicitlyDisabled()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var schema = new JsonSchema
        {
            Name = "items",
            KeyMode = KeyMode.Auto,
            AutoQueryable = false,
        };
        await db.SaveAsync(schema, ct);

        var doc = JsonDocument.Parse("""{"name":"Hidden","value":42}""");
        doc.SetSchema("items");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        // AutoQueryable explicitly off — nothing indexed
        var results = db.Search<object>("Hidden").OfType<JsonDocument>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task AutoQueryable_JsonSchema_StringArray()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var schema = new JsonSchema { Name = "articles", KeyMode = KeyMode.Auto };
        await db.SaveAsync(schema, ct);

        var doc = JsonDocument.Parse("""{"title":"Rust Guide","tags":["rust","programming","systems"]}""");
        doc.SetSchema("articles");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        // Should be searchable by individual tag element
        var results = db.Search<object>("programming").OfType<JsonDocument>().ToList();
        Assert.Single(results);
    }

    // === Schemaless API tests ===

    [Fact]
    public async Task Schemaless_SaveAndGet_ById()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var doc = JsonDocument.Parse("""{"id":"abc","name":"Alice","age":30}""");
        await db.SaveAsync(doc, ct);

        var retrieved = await db.GetAsync("abc", ct);
        Assert.NotNull(retrieved);
        var jsonDoc = Assert.IsType<JsonDocument>(retrieved);
        Assert.Equal("Alice", jsonDoc.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task Schemaless_AutoSearch()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","name":"Frank","city":"Portland"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","name":"Grace","city":"Seattle"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search<object>("Portland").ToList();
        Assert.Single(results);
        var doc = Assert.IsType<JsonDocument>(results[0]);
        Assert.Equal("Frank", doc.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task Schemaless_KeyConvention_id()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"my-key","data":"test"}"""), ct);

        var retrieved = await db.GetAsync("my-key", ct);
        Assert.NotNull(retrieved);
    }

    [Fact]
    public async Task Schemaless_KeyConvention_underscore_id()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"_id":"mongo-style","name":"Test"}"""), ct);

        var retrieved = await db.GetAsync("mongo-style", ct);
        Assert.NotNull(retrieved);
    }

    [Fact]
    public async Task Schemaless_KeyConvention_CaseInsensitive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"ID":"upper-case","value":42}"""), ct);

        var retrieved = await db.GetAsync("upper-case", ct);
        Assert.NotNull(retrieved);
    }

    [Fact]
    public async Task Schemaless_NoKey_AutoGeneratesULID()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var doc = JsonDocument.Parse("""{"name":"NoKeyDoc","value":99}""");
        await db.SaveAsync(doc, ct);

        // The doc should have a key assigned
        var key = doc.GetKey();
        Assert.NotNull(key);
        Assert.NotEmpty(key!);

        var retrieved = await db.GetAsync(key, ct);
        Assert.NotNull(retrieved);
    }
}
