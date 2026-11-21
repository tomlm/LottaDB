using System.Text.Json;

namespace Lotta.Tests;

/// <summary>
/// Verifies the Lucene near-real-time (NRT) reader behaviour around saves and searches.
///
/// <para><b>Covered scenarios:</b></para>
/// <list type="bullet">
///   <item>Search immediately after SaveAsync sees the document.</item>
///   <item>Multiple consecutive saves are all visible to a following Search.</item>
///   <item>Table-Storage-only reads (GetManyAsync, GetAsync) do not interfere with the searcher.</item>
///   <item>An explicit ReloadSearcher() call always produces a fresh, consistent view.</item>
///   <item>Save → Search&lt;JsonSchema&gt;() → Search&lt;T&gt;() — all must see the document (regression).</item>
/// </list>
///
/// <para><b>Regression note:</b></para>
/// <para>
/// <c>RefreshLoopAsync</c> previously committed the <c>_indexWriter</c> on the auto-commit timer
/// but never called <c>_lucene.Refresh()</c> nor cleared <c>_indexDirty</c>, leaving the Lucene
/// LINQ searcher with a stale reader. Fixed by adding <c>_lucene.Refresh()</c> and
/// <c>_indexDirty = false</c> after every background commit.
/// </para>
/// </summary>
public abstract class NearRealTimeSearchTestBase : LottaTestBase
{
    protected NearRealTimeSearchTestBase(Action<LottaCatalog> config) : base(config) { }

    // =========================================================
    // Reliable surface
    // =========================================================

    [Fact]
    public async Task Search_ImmediatelyAfterSave_ReturnsDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "nrt1", DisplayName = "NRT One" }, ct);

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("nrt1", results[0].Username);
    }

    [Fact]
    public async Task Search_MultipleConsecutiveSaves_AllVisible()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "a", DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "b", DisplayName = "B" }, ct);
        await db.SaveAsync(new Actor { Username = "c", DisplayName = "C" }, ct);

        var results = db.Search<Actor>().ToList();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Search_AfterTableStorageRead_ReturnsDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "nrt2", DisplayName = "NRT Two" }, ct);

        // GetManyAsync reads Table Storage only — must not affect the Lucene searcher state
        var fromTable = await db.GetManyAsync<Actor>().ToListAsync(ct);
        Assert.Single(fromTable);

        var fromSearch = db.Search<Actor>().ToList();
        Assert.Single(fromSearch);
        Assert.Equal("nrt2", fromSearch[0].Username);
    }

    [Fact]
    public async Task Search_AfterGetAsync_ReturnsDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "nrt3", DisplayName = "NRT Three" }, ct);

        // Point read (Table Storage only) must not poison the searcher
        var fetched = await db.GetAsync<Actor>("nrt3", ct);
        Assert.NotNull(fetched);

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("nrt3", results[0].Username);
    }

    [Fact]
    public async Task Search_AfterExplicitReloadSearcher_ReturnsDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "nrt4", DisplayName = "NRT Four" }, ct);
        db.ReloadSearcher();

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("nrt4", results[0].Username);
    }

    /// <summary>
    /// Explicit ReloadSearcher() after any sequence always produces a fresh, consistent view.
    /// </summary>
    [Fact]
    public async Task Search_AfterSchemaSearch_ExplicitReloadSearcher_Heals()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Widget", Properties = [] }, ct);
        await db.SaveAsync(new Actor { Username = "heal1", DisplayName = "Heal One" }, ct);

        // Trigger the same sequence that causes the quirk …
        _ = db.Search<JsonSchema>().ToList();

        // … but heal it by forcing a fresh commit+refresh before searching
        db.ReloadSearcher();

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("heal1", results[0].Username);
    }
    // =========================================================
    // Regression: save → Search<JsonSchema>() → Search<Actor>() must all see the document.
    //
    // Was a known NRT bug: RefreshLoopAsync committed _indexWriter but never called
    // _lucene.Refresh() nor cleared _indexDirty, so the Lucene LINQ searcher held
    // a stale reader snapshot after the background auto-commit fired.
    // Fixed: RefreshLoopAsync now calls _lucene.Refresh() and sets _indexDirty = false.
    // =========================================================

    [Fact]
    public async Task Search_SchemaSearch_ThenDocumentSearch_BothVisible()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Widget", Properties = [] }, ct);

        // Write a document — index is now dirty
        await db.SaveAsync(new Actor { Username = "quirk1", DisplayName = "Quirk One" }, ct);

        // First Lucene read is a schema listing — previously caused a stale NRT reader
        var schemas = db.Search<JsonSchema>().ToList();
        Assert.NotEmpty(schemas);

        // Must see the Actor immediately — no staleness after the fix
        var actorResults = db.Search<Actor>().ToList();
        Assert.Single(actorResults);
    }

    // =========================================================
    // Without a commit: while this process holds the writer, readers are opened from it,
    // so a search sees what was written without the index being committed first.
    // =========================================================

    // Background commits held off for the test, so any commit seen is one a search made.
    private Task<LottaDB> CreateUncommittedDbAsync(CancellationToken ct, [System.Runtime.CompilerServices.CallerMemberName] string? testName = null) =>
        CreateDbAsync(config => config.AutoCommitDelay = 600_000, cancellationToken: ct, testName: testName);

    [Fact]
    public async Task Search_SeesEachSave_WithoutCommitting()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateUncommittedDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "One" }, ct);
        Assert.Single(db.Search<Actor>().ToList());
        var committed = db.CommittedGeneration;

        for (var i = 2; i <= 4; i++)
        {
            await db.SaveAsync(new Actor { Username = $"u{i}", DisplayName = "More" }, ct);
            Assert.Equal(i, db.Search<Actor>().ToList().Count);
            Assert.Equal(i, db.Search<object>().ToList().OfType<Actor>().Count());
        }

        Assert.Equal(committed, db.CommittedGeneration);
    }

    [Fact]
    public async Task JsonSearch_SeesEachSave_WithoutCommitting()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateUncommittedDbAsync(ct);
        await db.SaveAsync(JsonDocument.Parse("""{"id":"1","name":"Alice"}"""), ct);
        Assert.Single(db.Search(j => j["name"] == "Alice").ToList());
        var committed = db.CommittedGeneration;

        await db.SaveAsync(JsonDocument.Parse("""{"id":"2","name":"Alice"}"""), ct);

        Assert.Equal(2, db.Search(j => j["name"] == "Alice").ToList().Count);
        Assert.Equal(committed, db.CommittedGeneration);
    }

    [Fact]
    public async Task Search_DoesNotSeeADeletedDocument_WithoutCommitting()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateUncommittedDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "d1", DisplayName = "One" }, ct);
        await db.SaveAsync(new Actor { Username = "d2", DisplayName = "Two" }, ct);
        Assert.Equal(2, db.Search<Actor>().ToList().Count);

        await db.DeleteAsync<Actor>("d1", ct);

        Assert.Equal(["d2"], db.Search<Actor>().Select(a => a.Username).ToList());
    }

    [Fact]
    public async Task Search_AfterTheWriterIsGivenUpAndTakenAgain_SeesEverything()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateUncommittedDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "w1", DisplayName = "One" }, ct);
        Assert.Single(db.Search<Actor>().ToList());

        // Commits and closes the writer: searches read what is committed, which is everything.
        await db.ReleaseWriteLockAsync(ct);
        Assert.False(db.HoldsWriteLock);
        Assert.Single(db.Search<Actor>().ToList());
        Assert.Single(db.Search<object>().ToList().OfType<Actor>());

        // The next write takes the writer again, and searches read from it again.
        await db.SaveAsync(new Actor { Username = "w2", DisplayName = "Two" }, ct);
        Assert.True(db.HoldsWriteLock);
        Assert.Equal(2, db.Search<Actor>().ToList().Count);
        Assert.Equal(2, db.Search<object>().ToList().OfType<Actor>().Count());
    }

    [Fact]
    public async Task ReloadSearcher_StillCommits()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateUncommittedDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "r1", DisplayName = "One" }, ct);
        Assert.Single(db.Search<Actor>().ToList());
        var committed = db.CommittedGeneration;

        db.ReloadSearcher();

        Assert.True(db.CommittedGeneration > committed);
        Assert.Single(db.Search<Actor>().ToList());
    }
}
