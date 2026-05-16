using System.Runtime.CompilerServices;

namespace Lotta.Tests;

/// <summary>
/// Verifies that fluent-only configuration (no attributes) works identically
/// to attribute-based configuration for all major functional areas.
/// All models used here (BareActor, BareNote, BareOrder) have zero attributes.
/// </summary>
public class FluentConfigTests
{
    private static async Task<LottaDB> CreateFluentDbAsync(Action<ILottaConfiguration>? extra = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null)
    {
        testName = String.Join(String.Empty, testName!.Where(char.IsLetterOrDigit).Take(60));

        var catalog = new LottaCatalog(testName!);
        catalog.ConfigureTestStorage();
        return await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<BareActor>(s =>
            {
                s.SetKey(a => a.Username);
                s.AddQueryable(a => a.DisplayName).NotAnalyzed();
            });
            config.Store<BareNote>(s =>
            {
                s.SetKey(n => n.NoteId);
                s.AddQueryable(n => n.AuthorId);
                s.AddQueryable(n => n.Content);
            });
            extra?.Invoke(config);
        }, cancellationToken);
    }

    // === CRUD ===

    [Fact]
    public async Task Fluent_SaveAndGet()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        var loaded = await db.GetAsync<BareActor>("alice", ct);
        Assert.NotNull(loaded);
        Assert.Equal("alice", loaded.Username);
        Assert.Equal("Alice", loaded.DisplayName);
    }

    [Fact]
    public async Task Fluent_SaveOverwrites()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice Updated" }, ct);

        var loaded = await db.GetAsync<BareActor>("alice", ct);
        Assert.NotNull(loaded);
        Assert.Equal("Alice Updated", loaded.DisplayName);
    }

    [Fact]
    public async Task Fluent_GetNonExistent_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        var result = await db.GetAsync<BareActor>("nobody", ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task Fluent_DeleteByKey()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        await db.DeleteAsync<BareActor>("alice", ct);

        var loaded = await db.GetAsync<BareActor>("alice", ct);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Fluent_DeleteByObject()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        var actor = new BareActor { Username = "alice", DisplayName = "Alice" };
        await db.SaveAsync(actor, ct);

        await db.DeleteAsync(actor, ct);

        var loaded = await db.GetAsync<BareActor>("alice", ct);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Fluent_DeleteNonExistent_NoError()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        var result = await db.DeleteAsync<BareActor>("nobody", ct);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Fluent_SaveReturnsObjectResult()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        var result = await db.SaveAsync(new BareActor { Username = "alice" }, ct);

        Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Saved, result.Changes[0].Kind);
        Assert.Equal("alice", result.Changes[0].Key);
    }

    [Fact]
    public async Task Fluent_DeleteReturnsObjectResult()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice" }, ct);

        var result = await db.DeleteAsync<BareActor>("alice", ct);

        Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Deleted, result.Changes[0].Kind);
        Assert.Equal("alice", result.Changes[0].Key);
    }

    // === Query (Table Storage) ===

    [Fact]
    public async Task Fluent_Query_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new BareActor { Username = "bob", DisplayName = "Bob" }, ct);

        var all = await db.GetManyAsync<BareActor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task Fluent_Query_FilterByTag()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new BareActor { Username = "bob", DisplayName = "Bob" }, ct);

        var results = await db.GetManyAsync<BareActor>(a => a.DisplayName == "Alice", cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Fluent_Query_PredicateOverload()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);

        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "Hello" }, ct);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "bob", Content = "World" }, ct);

        var results = await db.GetManyAsync<BareNote>(cancellationToken: ct).Where(n => n.AuthorId == "alice").ToListAsync(ct);
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Fluent_Query_Take()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "a", DisplayName = "A" }, ct);
        await db.SaveAsync(new BareActor { Username = "b", DisplayName = "B" }, ct);
        await db.SaveAsync(new BareActor { Username = "c", DisplayName = "C" }, ct);

        var results = await db.GetManyAsync<BareActor>(cancellationToken: ct).Take(2).ToListAsync(ct);
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Fluent_Query_Empty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        var results = await db.GetManyAsync<BareActor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Empty(results);
    }

    // === Search (Lucene) ===

    [Fact]
    public async Task Fluent_Search_FindsAfterSave()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        var results = db.Search<BareActor>().ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Fluent_Search_RemovedAfterDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.DeleteAsync<BareActor>("alice", ct);

        var results = db.Search<BareActor>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Fluent_Search_UpdateReflected()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice Updated" }, ct);

        var results = db.Search<BareActor>().ToList();
        Assert.Single(results);
        Assert.Equal("Alice Updated", results[0].DisplayName);
    }

    [Fact]
    public async Task Fluent_Search_Take()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "a", DisplayName = "A" }, ct);
        await db.SaveAsync(new BareActor { Username = "b", DisplayName = "B" }, ct);
        await db.SaveAsync(new BareActor { Username = "c", DisplayName = "C" }, ct);

        var results = db.Search<BareActor>().Take(2).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Fluent_Search_Empty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        var results = db.Search<BareActor>().ToList();
        Assert.Empty(results);
    }

    // === Search with indexed field filters ===

    [Fact]
    public async Task Fluent_Search_FilterByIndexedField()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new BareActor { Username = "bob", DisplayName = "Bob" }, ct);

        var results = db.Search<BareActor>()
            .Where(a => a.DisplayName == "Alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Fluent_Search_FilterByIndexedField_NoMatch()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        var results = db.Search<BareActor>()
            .Where(a => a.DisplayName == "Nobody")
            .ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Fluent_Search_FilterByNotAnalyzedField()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "Hello world" }, ct);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "bob", Content = "Goodbye world" }, ct);

        var results = db.Search<BareNote>()
            .Where(n => n.AuthorId == "alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Fluent_Search_FilterByAnalyzedContent()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "Lucene is great for full text search" }, ct);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "bob", Content = "Azure table storage is fast" }, ct);

        var results = db.Search<BareNote>()
            .Where(n => n.Content.Contains("lucene"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);

        results = db.Search<BareNote>("lucene AND alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    // === ChangeAsync ===

    [Fact]
    public async Task Fluent_ChangeAsync_MutatesAndSaves()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        await db.ChangeAsync<BareActor>("alice", a =>
        {
            a.DisplayName = "Alice Changed";
            return a;
        }, ct);

        var loaded = await db.GetAsync<BareActor>("alice", ct);
        Assert.Equal("Alice Changed", loaded!.DisplayName);
    }

    [Fact]
    public async Task Fluent_ChangeAsync_NonExistent_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.ChangeAsync<BareActor>("nobody", a => a, ct));
    }

    [Fact]
    public async Task Fluent_ChangeAsync_ReturnsObjectResult()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        var result = await db.ChangeAsync<BareActor>("alice", a =>
        {
            a.DisplayName = "Changed";
            return a;
        }, ct);

        Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Saved, result.Changes[0].Kind);
    }

    // === DeleteManyAsync with predicate ===

    [Fact]
    public async Task Fluent_DeleteManyByPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "A" }, ct);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "alice", Content = "B" }, ct);
        await db.SaveAsync(new BareNote { NoteId = "n3", AuthorId = "bob", Content = "C" }, ct);

        await db.DeleteManyAsync<BareNote>(n => n.AuthorId == "alice", ct);

        var remaining = await db.GetManyAsync<BareNote>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(remaining);
        Assert.Equal("bob", remaining[0].AuthorId);
    }

    // === Multiple types in same DB ===

    [Fact]
    public async Task Fluent_MultipleTypes_Isolated()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateFluentDbAsync(cancellationToken: ct);
        await db.SaveAsync(new BareActor { Username = "alice" }, ct);
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice" }, ct);

        var actors = await db.GetManyAsync<BareActor>(cancellationToken: ct).ToListAsync(ct);
        var notes = await db.GetManyAsync<BareNote>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(actors);
        Assert.Single(notes);
    }

    // === On<T> handlers with fluent-configured types ===

    [Fact]
    public async Task Fluent_OnHandler_FiresOnSave()
    {
        var ct = TestContext.Current.CancellationToken;
        BareActor? captured = null;
        TriggerKind? capturedKind = null;

        using var db = await CreateFluentDbAsync(opts =>
        {
            opts.On<BareActor>((actor, kind, _, _) =>
            {
                captured = actor;
                capturedKind = kind;
                return Task.CompletedTask;
            });
        }, ct);

        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, ct);

        Assert.NotNull(captured);
        Assert.Equal("alice", captured!.Username);
        Assert.Equal(TriggerKind.Saved, capturedKind);
    }

    [Fact]
    public async Task Fluent_OnHandler_FiresOnDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        TriggerKind? capturedKind = null;

        using var db = await CreateFluentDbAsync(opts =>
        {
            opts.On<BareActor>((_, kind, __, ___) =>
            {
                capturedKind = kind;
                return Task.CompletedTask;
            });
        }, ct);

        await db.SaveAsync(new BareActor { Username = "alice" }, ct);
        await db.DeleteAsync<BareActor>("alice", ct);

        Assert.Equal(TriggerKind.Deleted, capturedKind);
    }

    // === Custom composite key ===

    [Fact]
    public async Task Fluent_CompositeKey()
    {
        var ct = TestContext.Current.CancellationToken;

        var catalog = new LottaCatalog("FluentCompositeKey");
        catalog.ConfigureTestStorage();
        using var db = await catalog.GetDatabaseAsync("default", options =>
        {
            options.Store<BareActor>(s =>
            {
                s.SetKey(a => $"{a.Domain}-{a.Username}");
            });
        }, ct);

        await db.SaveAsync(new BareActor { Domain = "example.com", Username = "alice", DisplayName = "Alice" }, ct);

        var loaded = await db.GetAsync<BareActor>("example.com-alice", ct);
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.DisplayName);
    }
}
