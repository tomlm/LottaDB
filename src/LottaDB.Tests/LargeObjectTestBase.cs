using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public abstract class LargeObjectTestBase : LottaTestBase
{
    protected LargeObjectTestBase(string provider) : base(provider) { }
    [Fact]
    public async Task LargeObject_SplitsAcrossProperties_RoundTrips()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<LargeDocument>();
        }, ct);
        await db.ResetDatabaseAsync(ct);

        // Create a payload larger than 63KB to force splitting across table storage properties
        var largePayload = new string('X', 100_000); // ~100KB
        var doc = new LargeDocument
        {
            Id = "large1",
            Title = "Large Document",
            Payload = largePayload,
        };

        await db.SaveAsync(doc, ct);

        // Point read — verifies split property reassembly
        var loaded = await db.GetAsync<LargeDocument>("large1", cancellationToken: ct);
        Assert.NotNull(loaded);
        Assert.Equal("large1", loaded.Id);
        Assert.Equal("Large Document", loaded.Title);
        Assert.Equal(largePayload.Length, loaded.Payload.Length);
        Assert.Equal(largePayload, loaded.Payload);

        // Lucene search by queryable field still works
        db.ReloadSearcher();
        var searched = db.Search<LargeDocument>(d => d.Title == "Large Document").ToList();
        Assert.Single(searched);
        Assert.Equal("Large Document", searched[0].Title);
        Assert.Equal(largePayload, searched[0].Payload);
    }

    [Fact]
    public async Task LargeObject_Update_RoundTrips()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<LargeDocument>();
        }, ct);
        await db.ResetDatabaseAsync(ct);

        // Save initial large document
        var payload1 = new string('A', 80_000);
        await db.SaveAsync(new LargeDocument { Id = "doc1", Title = "V1", Payload = payload1 }, ct);

        // Update with different large payload
        var payload2 = new string('B', 120_000);
        await db.SaveAsync(new LargeDocument { Id = "doc1", Title = "V2", Payload = payload2 }, ct);

        var loaded = await db.GetAsync<LargeDocument>("doc1", cancellationToken: ct);
        Assert.NotNull(loaded);
        Assert.Equal("V2", loaded.Title);
        Assert.Equal(payload2, loaded.Payload);
    }
}

