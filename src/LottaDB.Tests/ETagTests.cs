using Lotta;

namespace Lotta.Tests;

public class ETagTests : IClassFixture<LottaDBFixture>
{
    // === GetAsync with ETag ===

    [Fact]
    public async Task GetAsync_ReturnsETag()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-get", DisplayName = "Test" }, TestContext.Current.CancellationToken);

        var result = await db.GetAsync<Actor>("etag-get", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal("Test", result.DisplayName);
        Assert.NotEmpty(result.GetETag()!);
    }

    [Fact]
    public async Task GetAsync_NotFound_ReturnsNull()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var result = await db.GetAsync<Actor>("nonexistent", TestContext.Current.CancellationToken);
        Assert.Null(result);
    }

    // === GetManyAsync with ETags ===

    [Fact]
    public async Task GetManyAsync_ReturnsETags()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-many-1", DisplayName = "A" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Username = "etag-many-2", DisplayName = "B" }, TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync<Actor>(cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r =>
        {
            Assert.NotEmpty(r.GetETag()!);
            Assert.NotNull(r);
        });
    }

    // === Search with ETags ===

    [Fact]
    public async Task Search_ReturnsETags()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-search", DisplayName = "Searchable" }, TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search<Actor>("DisplayName:Searchable").ToList();
        Assert.Single(results);
        Assert.Equal("Searchable", results[0].DisplayName);
        Assert.NotEmpty(results[0].GetETag()!);
    }

    // === Conditional SaveAsync ===

    [Fact]
    public async Task SaveAsync_WithETag_ConditionalWrite_Succeeds()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-save", DisplayName = "V1" }, TestContext.Current.CancellationToken);

        var result = await db.GetAsync<Actor>("etag-save", TestContext.Current.CancellationToken);
        Assert.NotNull(result);

        // result has ETag from GetAsync → SaveAsync does conditional write
        result.DisplayName = "V2";
        await db.SaveAsync(result, TestContext.Current.CancellationToken);

        var updated = await db.GetAsync<Actor>("etag-save", TestContext.Current.CancellationToken);
        Assert.Equal("V2", updated!.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_WithStaleETag_ThrowsConcurrencyException()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-conflict", DisplayName = "V1" }, TestContext.Current.CancellationToken);

        var result = await db.GetAsync<Actor>("etag-conflict", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        var staleETag = result.GetETag()!;

        // Another writer modifies the entity (this updates result's ETag)
        result.DisplayName = "V2";
        await db.SaveAsync(result, TestContext.Current.CancellationToken);

        // Simulate stale ETag — set the old one back on the object
        result.SetETag(staleETag);
        result.DisplayName = "V3";
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => db.SaveAsync(result, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ETag_UpdatedInPlaceAfterSave()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-change", DisplayName = "V1" }, TestContext.Current.CancellationToken);

        var r1 = await db.GetAsync<Actor>("etag-change", TestContext.Current.CancellationToken);
        Assert.NotNull(r1);
        var etagAfterGet = r1.GetETag()!;

        r1.DisplayName = "V2";
        await db.SaveAsync(r1, TestContext.Current.CancellationToken);

        // ETag was updated in place on the same object
        var etagAfterSave = r1.GetETag()!;
        Assert.NotEqual(etagAfterGet, etagAfterSave);

        // Fresh read returns the same ETag as the saved object
        var r2 = await db.GetAsync<Actor>("etag-change", TestContext.Current.CancellationToken);
        Assert.NotNull(r2);
        Assert.Equal(etagAfterSave, r2.GetETag());
    }

    // === ChangeAsync sets ETag for Lucene ===

    [Fact]
    public async Task ChangeAsync_SearchResultHasETag()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-change-search", DisplayName = "V1" }, TestContext.Current.CancellationToken);

        await db.ChangeAsync<Actor>("etag-change-search", a => { a.DisplayName = "V2"; }, TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search<Actor>("DisplayName:V2").ToList();
        Assert.Single(results);
        Assert.NotNull(results[0].GetETag());
        Assert.NotEmpty(results[0].GetETag()!);
    }

    // === GetManyAsync sets ETags ===

    [Fact]
    public async Task GetManyAsync_AllItemsHaveETags()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-gm-1", DisplayName = "A" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Username = "etag-gm-2", DisplayName = "B" }, TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync<Actor>(cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.All(results, r => Assert.NotEmpty(r.GetETag()!));
    }

    // === Search → conditional save round-trip ===

    [Fact]
    public async Task Search_ThenConditionalSave_Works()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "etag-search-save", DisplayName = "Original" }, TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var found = db.Search<Actor>("DisplayName:Original").First();
        Assert.NotEmpty(found.GetETag()!);

        // ETag is on the object → SaveAsync does conditional write automatically
        found.DisplayName = "Updated";
        await db.SaveAsync(found, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<Actor>("etag-search-save", TestContext.Current.CancellationToken);
        Assert.Equal("Updated", loaded!.DisplayName);
    }
}
