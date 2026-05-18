using System.Text.Json;

namespace Lotta.Tests;

public class JsonExpressionTests : IClassFixture<LottaDBFixture>
{
    // === JsonExpression Search tests ===

    [Fact]
    public async Task Search_ByStringField()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","name":"Alice","city":"Portland"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","name":"Bob","city":"Seattle"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["name"] == "Alice").ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task Search_ByNumericComparison()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","name":"Young","age":20}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","name":"Old","age":60}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["age"] > 30).ToList();
        Assert.Single(results);
        Assert.Equal("Old", results[0].RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task Search_WithAndOperator()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","name":"Alice","city":"Portland"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","name":"Bob","city":"Portland"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","name":"Alice","city":"Seattle"}"""), ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["name"] == "Alice" && j["city"] == "Portland").ToList();
        Assert.Single(results);
    }

    // === Schema matching via Match expression ===

    [Fact]
    public async Task Match_AutoClassifiesDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Define schema with match expression
        await db.SaveAsync(new JsonSchema
        {
            Name = "Person",
            Match = "$.type == 'Person'",
        }, ct);

        // Save doc — should auto-match "Person" schema
        var doc = JsonDocument.Parse("""{"id":"1","type":"Person","name":"Alice"}""");
        await db.SaveAsync(doc, ct);

        Assert.Equal("Person", doc.GetSchema());
    }

    [Fact]
    public async Task Search_BySchema()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Person", Match = "$.type == 'person'" }, ct);
        await db.SaveAsync(new JsonSchema { Name = "Product", Match = "$.type == 'product'" }, ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","type":"person","name":"Alice"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","type":"product","name":"Widget"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","type":"person","name":"Bob"}"""), ct);
        db.ReloadSearcher();

        var people = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Equal(2, people.Count);

        var products = db.Search(j => j.GetSchema() == "Product").ToList();
        Assert.Single(products);
    }

    [Fact]
    public async Task SetSchema_ExplicitOverridesMatch()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Person", Match = "$.type == 'person'" }, ct);
        await db.SaveAsync(new JsonSchema { Name = "VIP" }, ct);

        // Explicitly set schema — should override discriminator matching
        var doc = JsonDocument.Parse("""{"id":"1","type":"person","name":"Alice"}""");
        doc.SetSchema("VIP");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        var vips = db.Search(j => j.GetSchema() == "VIP").ToList();
        Assert.Single(vips);

        var people = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Empty(people);
    }

    // === GetManyAsync with JsonExpression ===

    [Fact]
    public async Task GetManyAsync_WithJsonExpression()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Person", Match = "$.type == 'person'" }, ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","type":"person","name":"Alice"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","type":"person","name":"Bob"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"3","type":"other","name":"Widget"}"""), ct);

        var people = new List<JsonDocument>();
        await foreach (var doc in db.GetManyAsync(j => j.GetSchema() == "Person", cancellationToken: ct))
            people.Add(doc);

        Assert.Equal(2, people.Count);
    }

    // === No schemaName needed ===

    [Fact]
    public async Task SchemalessWorkflow_EndToEnd()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Just save — no schema definition needed
        await db.SaveAsync(JsonDocument.Parse("""{"id":"abc","name":"Alice","age":30}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"def","name":"Bob","age":25}"""), ct);

        // Get by key
        var alice = await db.GetAsync("abc", ct);
        Assert.NotNull(alice);
        Assert.IsType<JsonDocument>(alice);

        // Search
        db.ReloadSearcher();
        var results = db.Search(j => j["name"] == "Bob").ToList();
        Assert.Single(results);
    }
}
