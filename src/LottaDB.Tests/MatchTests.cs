using System.Text.Json;

namespace Lotta.Tests;

/// <summary>
/// Tests for JsonDocumentType.Match discriminator — auto-classification of documents on save.
/// </summary>
public class MatchTests : IClassFixture<LottaDBFixture>
{
    [Fact]
    public async Task Match_FirstMatchWins()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Both schemas could match a "person" doc, but Person is registered first
        await db.SaveAsync(new JsonDocumentType { Name = "Person", Match = "$.type == 'person'" }, ct);
        await db.SaveAsync(new JsonDocumentType { Name = "Human", Match = "$.type == 'person'" }, ct);

        var doc = JsonDocument.Parse("""{"id":"1","type":"person","name":"Alice"}""");
        await db.SaveAsync(doc, ct);

        // First registered schema with matching discriminator wins
        Assert.Equal("Person", doc.GetSchema());
    }

    [Fact]
    public async Task Match_NoMatchFallsToDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "Person", Match = "$.type == 'person'" }, ct);

        // This doc doesn't match any discriminator
        var doc = JsonDocument.Parse("""{"id":"1","type":"widget","name":"Gadget"}""");
        await db.SaveAsync(doc, ct);

        // No match → default schema → GetSchema() returns null (default is not exposed)
        Assert.Null(doc.GetSchema());
    }

    [Fact]
    public async Task Match_NestedJsonPath()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "USAddress", Match = "$.address.country == 'US'" }, ct);

        var usDoc = JsonDocument.Parse("""{"id":"1","address":{"country":"US","city":"Portland"}}""");
        await db.SaveAsync(usDoc, ct);
        Assert.Equal("USAddress", usDoc.GetSchema());

        var ukDoc = JsonDocument.Parse("""{"id":"2","address":{"country":"UK","city":"London"}}""");
        await db.SaveAsync(ukDoc, ct);
        Assert.Null(ukDoc.GetSchema());
    }

    [Fact]
    public async Task Match_CaseInsensitiveComparison()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "Person", Match = "$.type == 'Person'" }, ct);

        // Match is case-insensitive on the value comparison
        var doc = JsonDocument.Parse("""{"id":"1","type":"person","name":"Alice"}""");
        await db.SaveAsync(doc, ct);
        Assert.Equal("Person", doc.GetSchema());
    }

    [Fact]
    public async Task Match_NumericValue()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "V2Doc", Match = "$.version == '2'" }, ct);

        var doc = JsonDocument.Parse("""{"id":"1","version":2,"data":"test"}""");
        await db.SaveAsync(doc, ct);
        Assert.Equal("V2Doc", doc.GetSchema());
    }

    [Fact]
    public async Task Match_BooleanValue()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "PublishedDoc", Match = "$.published == 'true'" }, ct);

        var doc = JsonDocument.Parse("""{"id":"1","published":true,"title":"My Post"}""");
        await db.SaveAsync(doc, ct);
        Assert.Equal("PublishedDoc", doc.GetSchema());

        var draft = JsonDocument.Parse("""{"id":"2","published":false,"title":"Draft"}""");
        await db.SaveAsync(draft, ct);
        Assert.Null(draft.GetSchema());
    }

    [Fact]
    public async Task Match_PersistsAfterRoundTrip()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "Order", Match = "$.type == 'order'" }, ct);

        var doc = JsonDocument.Parse("""{"id":"ord-1","type":"order","total":99.50}""");
        await db.SaveAsync(doc, ct);
        Assert.Equal("Order", doc.GetSchema());

        // Read back from Table Storage
        var retrieved = await db.GetAsync("ord-1", ct);
        Assert.NotNull(retrieved);
        Assert.IsType<JsonDocument>(retrieved);
        Assert.Equal("Order", retrieved!.GetSchema());
    }

    [Fact]
    public async Task Match_PersistsInLuceneSearch()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "Order", Match = "$.type == 'order'" }, ct);

        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","type":"order","item":"Widget"}"""), ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","type":"invoice","item":"Widget"}"""), ct);
        db.ReloadSearcher();

        // Search by schema via Lucene
        var orders = db.Search(j => j.GetSchema() == "Order").ToList();
        Assert.Single(orders);
        Assert.Equal("Widget", orders[0].RootElement.GetProperty("item").GetString());
    }

    [Fact]
    public async Task Match_WithExplicitProperties()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Schema with both Match and explicit Properties
        await db.SaveAsync(new JsonDocumentType
        {
            Name = "Contact",
            Match = "$.type == 'contact'",
            AutoQueryable = false,
            Properties =
            [
                new QueryableProperty { Name = "email", Type = "string", Mode = QueryableMode.NotAnalyzed }
            ]
        }, ct);

        var doc = JsonDocument.Parse("""{"id":"1","type":"contact","name":"Alice","email":"alice@test.com"}""");
        await db.SaveAsync(doc, ct);

        Assert.Equal("Contact", doc.GetSchema());

        db.ReloadSearcher();

        // email is explicitly NotAnalyzed — exact match works
        var results = db.Search(j => j["email"] == "alice@test.com").ToList();
        Assert.Single(results);

        // "name" is not in Properties and AutoQueryable is off — not searchable
        var nameResults = db.Search(j => j["name"] == "Alice").ToList();
        Assert.Empty(nameResults);
    }

    [Fact]
    public async Task Match_NotEqual_DoesNotMatchTarget()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "NonAdmin", Match = "$.role != 'admin'" }, ct);

        var user = JsonDocument.Parse("""{"id":"1","role":"user","name":"Bob"}""");
        await db.SaveAsync(user, ct);
        Assert.Equal("NonAdmin", user.GetSchema());

        var admin = JsonDocument.Parse("""{"id":"2","role":"admin","name":"Admin"}""");
        await db.SaveAsync(admin, ct);
        Assert.Null(admin.GetSchema()); // != 'admin' does NOT match admin
    }

    [Fact]
    public async Task Match_MissingProperty_DoesNotMatch()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "Tagged", Match = "$.type == 'tagged'" }, ct);

        // Doc has no "type" property at all
        var doc = JsonDocument.Parse("""{"id":"1","name":"NoType"}""");
        await db.SaveAsync(doc, ct);
        Assert.Null(doc.GetSchema());
    }

    [Fact]
    public async Task Match_MissingProperty_NotEqual_Matches()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonDocumentType { Name = "Untyped", Match = "$.type != 'special'" }, ct);

        // Doc has no "type" property — != 'special' should match (absence != 'special')
        var doc = JsonDocument.Parse("""{"id":"1","name":"NoType"}""");
        await db.SaveAsync(doc, ct);
        Assert.Equal("Untyped", doc.GetSchema());
    }
}
