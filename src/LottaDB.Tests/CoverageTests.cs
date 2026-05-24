using System.Text.Json;

namespace Lotta.Tests;

/// <summary>
/// Tests covering gaps in the test suite: untyped search, mixed POCO/POJO operations,
/// JsonExpression operators, metadata consistency, and configurable key conventions.
/// </summary>
public class CoverageTests : LottaTestBase
{
    // === Search<object> returning mixed POCOs + POJOs ===

    [Fact]
    public async Task SearchObject_ReturnsMixedTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Actor" }, ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"bob","name":"Bob Json"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search<object>("Alice").ToList();
        Assert.Single(results);
        Assert.IsType<Actor>(results[0]);

        var allResults = db.Search<object>().ToList();
        Assert.True(allResults.Count >= 2, $"Expected at least 2, got {allResults.Count}");
        Assert.Contains(allResults, r => r is Actor);
        Assert.Contains(allResults, r => r is JsonDocument);
    }

    // === GetAsync(key) returning POCO ===

    [Fact]
    public async Task GetAsyncUntyped_ReturnsPOCO()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        var result = await db.GetAsync("alice", ct);
        Assert.NotNull(result);
        Assert.IsType<Actor>(result);
        Assert.Equal("Alice", ((Actor)result!).DisplayName);
    }

    [Fact]
    public async Task GetAsyncUntyped_ReturnsJsonDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"doc1","value":42}"""), ct);

        var result = await db.GetAsync("doc1", ct);
        Assert.NotNull(result);
        Assert.IsType<JsonDocument>(result);
    }

    // === GetManyAsync(JsonExpression) with field filters ===

    [Fact]
    public async Task GetManyAsync_JsonExpression_FieldFilter()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","name":"Alice","age":30}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","name":"Bob","age":20}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","name":"Charlie","age":40}"""), ct);

        var results = new List<JsonDocument>();
        await foreach (var doc in db.GetManyAsync(j => j["name"] == "Bob", cancellationToken: ct))
            results.Add(doc);

        Assert.Single(results);
        Assert.Equal("Bob", results[0].RootElement.GetProperty("name").GetString());
    }

    // === AutoKeyProperties custom configuration ===

    [Fact]
    public async Task AutoKeyProperties_CustomConfiguration()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config =>
        {
            config.AutoKeyProperties = ["recordId", "entityId"];
        }, cancellationToken: ct);

        var doc = JsonDocument.Parse("""{"recordId":"custom-key","name":"Custom"}""");
        await db.SaveAsync(doc, ct);

        Assert.Equal("custom-key", doc.GetKey());

        var retrieved = await db.GetAsync("custom-key", ct);
        Assert.NotNull(retrieved);
    }

    // === GetSchema() on POCOs returns type name ===

    [Fact]
    public async Task GetSchema_OnPOCO_ReturnsTypeName()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        var actor = new Actor { Username = "alice", DisplayName = "Alice" };
        await db.SaveAsync(actor, ct);

        Assert.Equal("Actor", actor.GetSchema());
    }

    [Fact]
    public async Task GetSchema_OnJsonDocument_ReturnsSchemaName()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Person", Match = "$.type == 'person'" }, ct);

        var doc = JsonDocument.Parse("""{"id":"1","type":"person","name":"Alice"}""");
        await db.SaveAsync(doc, ct);

        Assert.Equal("Person", doc.GetSchema());
    }

    [Fact]
    public async Task GetSchema_OnSchemalessJsonDocument_IsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        var doc = JsonDocument.Parse("""{"id":"1","name":"NoSchema"}""");
        await db.SaveAsync(doc, ct);

        // Schemaless docs use DefaultSchema which is not exposed
        Assert.Null(doc.GetSchema());
    }

    // === Schema column round-trip via Table Storage ===

    [Fact]
    public async Task Schema_RoundTrips_ForPOCO()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        var retrieved = await db.GetAsync<Actor>("alice", ct);
        Assert.NotNull(retrieved);
        Assert.Equal("Actor", retrieved!.GetSchema());
    }

    [Fact]
    public async Task Schema_RoundTrips_ForPOJO()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Item", Match = "$.kind == 'item'" }, ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","kind":"item","value":42}"""), ct);

        var retrieved = await db.GetAsync("1", ct);
        Assert.NotNull(retrieved);
        Assert.IsType<JsonDocument>(retrieved);
        Assert.Equal("Item", retrieved!.GetSchema());
    }

    // === SaveManyAsync with mixed POCO and POJO ===

    [Fact]
    public async Task SaveManyAsync_MixedPocoAndPojo()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        var actor = new Actor { Username = "alice", DisplayName = "Alice" };
        var note = new Note { NoteId = "n1", Content = "Hello", AuthorId = "alice" };
        var jsonDoc = JsonDocument.Parse("""{"id":"doc1","title":"My Doc"}""");

        var entities = new object[] { actor, note, jsonDoc };
        var result = await db.SaveManyAsync(entities, ct);

        Assert.Equal(3, result.Changes.Count);

        // Verify all retrievable
        var a = await db.GetAsync<Actor>("alice", ct);
        Assert.NotNull(a);
        var n = await db.GetAsync<Note>("n1", ct);
        Assert.NotNull(n);
        var d = await db.GetAsync("doc1", ct);
        Assert.NotNull(d);
        Assert.IsType<JsonDocument>(d);
    }

    // === JsonField operators: !=, >=, <= ===

    [Fact]
    public async Task Search_NotEqual()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","status":"active"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","status":"inactive"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","status":"active"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["status"] != "active").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task Search_GreaterThanOrEqual()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","score":10}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","score":20}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","score":30}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["score"] >= 20).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Search_LessThanOrEqual()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","score":10}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","score":20}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","score":30}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["score"] <= 20).ToList();
        Assert.Equal(2, results.Count);
    }

    // === Match discriminator with != ===

    [Fact]
    public async Task Match_NotEqual_Discriminator()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "NonPerson", Match = "$.type != 'person'" }, ct);

        var doc = JsonDocument.Parse("""{"id":"1","type":"product","name":"Widget"}""");
        await db.SaveAsync(doc, ct);
        Assert.Equal("NonPerson", doc.GetSchema());

        var personDoc = JsonDocument.Parse("""{"id":"2","type":"person","name":"Alice"}""");
        await db.SaveAsync(personDoc, ct);
        Assert.Null(personDoc.GetSchema()); // doesn't match != 'person'
    }

    // === Search<object> with string query across all types ===

    [Fact]
    public async Task SearchObject_StringQuery_FindsBothTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "portland-actor", DisplayName = "Portland Actor" }, ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"portland-doc","city":"Portland"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search<object>("Portland").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r is Actor);
        Assert.Contains(results, r => r is JsonDocument);
    }

    // === #4: DeleteManyAsync for JsonDocuments via JsonExpression ===

    [Fact]
    public async Task DeleteManyAsync_JsonExpression()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","status":"draft","title":"A"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","status":"published","title":"B"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","status":"draft","title":"C"}"""), ct);

        var result = await db.DeleteManyAsync(j => j["status"] == "draft", ct);
        Assert.Equal(2, result.Changes.Count);

        // Verify only "published" remains
        var remaining = new List<JsonDocument>();
        await foreach (var doc in db.GetManyAsync(j => j["status"] == "published", cancellationToken: ct))
            remaining.Add(doc);
        Assert.Single(remaining);

        // Verify deleted from Lucene too
        db.ReloadSearcher();
        var searchResults = db.Search(j => j["status"] == "draft").ToList();
        Assert.Empty(searchResults);
    }

    [Fact]
    public async Task DeleteManyAsync_JsonExpression_BySchema()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "TempData", Match = "$.kind == 'temp'" }, ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","kind":"temp","value":1}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","kind":"temp","value":2}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","kind":"permanent","value":3}"""), ct);

        var result = await db.DeleteManyAsync(j => j.GetSchema() == "TempData", ct);
        Assert.Equal(2, result.Changes.Count);
    }

    // === #5: Search with OR operator ===

    [Fact]
    public async Task Search_WithOrOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","city":"Portland"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","city":"Seattle"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","city":"Denver"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["city"] == "Portland" || j["city"] == "Seattle").ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Search_WithOrAndCombined()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","city":"Portland","active":true}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","city":"Seattle","active":true}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","city":"Portland","active":false}"""), ct);
        db.ReloadSearcher();

        // (Portland OR Seattle) AND active=true — note: booleans stored as "true"/"false" strings
        var results = db.Search(j =>
            (j["city"] == "Portland" || j["city"] == "Seattle") && j["active"] == true
        ).ToList();
        Assert.Equal(2, results.Count);
    }

    // === #6: GetManyAsync<object>() — untyped list across all types ===

    [Fact]
    public async Task GetManyAsync_Untyped_ReturnsAllTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", Content = "Hello", AuthorId = "alice" }, ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"doc1","title":"My Doc"}"""), ct);

        var all = new List<object>();
        await foreach (var entity in db.GetManyAsync(ct))
            all.Add(entity);

        // Should contain at least the 3 we saved (plus JsonSchema for default schema)
        Assert.True(all.Count >= 3, $"Expected at least 3, got {all.Count}");
        Assert.Contains(all, e => e is Actor);
        Assert.Contains(all, e => e is Note);
        Assert.Contains(all, e => e is JsonDocument);
    }

    [Fact]
    public async Task GetManyAsync_Untyped_SetsSchemaOnAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"doc1","title":"My Doc"}"""), ct);

        var all = new List<object>();
        await foreach (var entity in db.GetManyAsync(ct))
            all.Add(entity);

        // Every entity should have Schema set
        foreach (var entity in all)
        {
            var schema = entity.GetSchema();
            Assert.NotNull(schema);
        }
    }
}
