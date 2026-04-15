namespace Lotta.Tests;

/// <summary>
/// Verifies that fluent-only configuration (no attributes) works identically
/// to attribute-based configuration for all major functional areas.
/// All models used here (BareActor, BareNote, BareOrder) have zero attributes.
/// </summary>
public class FluentConfigTests
{
    private static LottaDB CreateFluentDb(Action<ILottaConfiguration>? extra = null)
    {
        var tableClient = LottaDBFixture.CreateInMemoryTableServiceClient();
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());

        var options = new LottaConfiguration();
        options.Store<BareActor>(s =>
        {
            s.SetKey(a => a.Username);
            s.AddQueryable(a => a.DisplayName).NotAnalyzed();
        });
        options.Store<BareNote>(s =>
        {
            s.SetKey(n => n.NoteId);
            s.AddQueryable(n => n.AuthorId).NotAnalyzed();
            s.AddQueryable(n => n.Content);
        });
        extra?.Invoke(options);

        return new LottaDB($"test{Guid.NewGuid():N}", tableClient, directory, options);
    }

    // === CRUD ===

    [Fact]
    public async Task Fluent_SaveAndGet()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BareActor>("alice", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("alice", loaded.Username);
        Assert.Equal("Alice", loaded.DisplayName);
    }

    [Fact]
    public async Task Fluent_SaveOverwrites()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice Updated" }, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BareActor>("alice", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Alice Updated", loaded.DisplayName);
    }

    [Fact]
    public async Task Fluent_GetNonExistent_ReturnsNull()
    {
        var db = CreateFluentDb();
        var result = await db.GetAsync<BareActor>("nobody", TestContext.Current.CancellationToken);
        Assert.Null(result);
    }

    [Fact]
    public async Task Fluent_DeleteByKey()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        await db.DeleteAsync<BareActor>("alice", TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BareActor>("alice", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Fluent_DeleteByObject()
    {
        var db = CreateFluentDb();
        var actor = new BareActor { Username = "alice", DisplayName = "Alice" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        await db.DeleteAsync(actor, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BareActor>("alice", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Fluent_DeleteNonExistent_NoError()
    {
        var db = CreateFluentDb();
        var result = await db.DeleteAsync<BareActor>("nobody", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Fluent_SaveReturnsObjectResult()
    {
        var db = CreateFluentDb();
        var result = await db.SaveAsync(new BareActor { Username = "alice" }, TestContext.Current.CancellationToken);

        Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Saved, result.Changes[0].Kind);
        Assert.Equal("alice", result.Changes[0].Key);
    }

    [Fact]
    public async Task Fluent_DeleteReturnsObjectResult()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice" }, TestContext.Current.CancellationToken);

        var result = await db.DeleteAsync<BareActor>("alice", TestContext.Current.CancellationToken);

        Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Deleted, result.Changes[0].Kind);
        Assert.Equal("alice", result.Changes[0].Key);
    }

    // === Query (Table Storage) ===

    [Fact]
    public async Task Fluent_Query_ReturnsAll()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        var all = db.Query<BareActor>().ToList();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task Fluent_Query_FilterByTag()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        var results = db.Query<BareActor>()
            .Where(a => a.DisplayName == "Alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Fluent_Query_PredicateOverload()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "Hello" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "bob", Content = "World" }, TestContext.Current.CancellationToken);

        var results = db.Query<BareNote>(n => n.AuthorId == "alice").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Fluent_Query_Take()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "a", DisplayName = "A" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "b", DisplayName = "B" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "c", DisplayName = "C" }, TestContext.Current.CancellationToken);

        var results = db.Query<BareActor>().Take(2).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Fluent_Query_Empty()
    {
        var db = CreateFluentDb();
        var results = db.Query<BareActor>().ToList();
        Assert.Empty(results);
    }

    // === Search (Lucene) ===

    [Fact]
    public async Task Fluent_Search_FindsAfterSave()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareActor>().ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Fluent_Search_RemovedAfterDelete()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.DeleteAsync<BareActor>("alice", TestContext.Current.CancellationToken);

        var results = db.Search<BareActor>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Fluent_Search_UpdateReflected()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice Updated" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareActor>().ToList();
        Assert.Single(results);
        Assert.Equal("Alice Updated", results[0].DisplayName);
    }

    [Fact]
    public async Task Fluent_Search_Take()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "a", DisplayName = "A" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "b", DisplayName = "B" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "c", DisplayName = "C" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareActor>().Take(2).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Fluent_Search_Empty()
    {
        var db = CreateFluentDb();
        var results = db.Search<BareActor>().ToList();
        Assert.Empty(results);
    }

    // === Search with indexed field filters ===

    [Fact]
    public async Task Fluent_Search_FilterByIndexedField()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareActor { Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareActor>()
            .Where(a => a.DisplayName == "Alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Fluent_Search_FilterByIndexedField_NoMatch()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareActor>()
            .Where(a => a.DisplayName == "Nobody")
            .ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Fluent_Search_FilterByNotAnalyzedField()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "Hello world" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "bob", Content = "Goodbye world" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareNote>()
            .Where(n => n.AuthorId == "alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Fluent_Search_FilterByAnalyzedContent()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "Lucene is great for full text search" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "bob", Content = "Azure table storage is fast" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareNote>()
            .Where(n => n.Content.Contains("lucene"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    // === ChangeAsync ===

    [Fact]
    public async Task Fluent_ChangeAsync_MutatesAndSaves()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        await db.ChangeAsync<BareActor>("alice", a =>
        {
            a.DisplayName = "Alice Changed";
            return a;
        }, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BareActor>("alice", TestContext.Current.CancellationToken);
        Assert.Equal("Alice Changed", loaded!.DisplayName);
    }

    [Fact]
    public async Task Fluent_ChangeAsync_NonExistent_Throws()
    {
        var db = CreateFluentDb();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.ChangeAsync<BareActor>("nobody", a => a, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Fluent_ChangeAsync_ReturnsObjectResult()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        var result = await db.ChangeAsync<BareActor>("alice", a =>
        {
            a.DisplayName = "Changed";
            return a;
        }, TestContext.Current.CancellationToken);

        Assert.Single(result.Changes);
        Assert.Equal(ChangeKind.Saved, result.Changes[0].Kind);
    }

    // === DeleteAsync with predicate ===

    [Fact]
    public async Task Fluent_DeleteByPredicate()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice", Content = "A" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareNote { NoteId = "n2", AuthorId = "alice", Content = "B" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareNote { NoteId = "n3", AuthorId = "bob", Content = "C" }, TestContext.Current.CancellationToken);

        await db.DeleteAsync<BareNote>(n => n.AuthorId == "alice", TestContext.Current.CancellationToken);

        var remaining = db.Query<BareNote>().ToList();
        Assert.Single(remaining);
        Assert.Equal("bob", remaining[0].AuthorId);
    }

    // === Multiple types in same DB ===

    [Fact]
    public async Task Fluent_MultipleTypes_Isolated()
    {
        var db = CreateFluentDb();
        await db.SaveAsync(new BareActor { Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareNote { NoteId = "n1", AuthorId = "alice" }, TestContext.Current.CancellationToken);

        var actors = db.Query<BareActor>().ToList();
        var notes = db.Query<BareNote>().ToList();
        Assert.Single(actors);
        Assert.Single(notes);
    }

    // === On<T> handlers with fluent-configured types ===

    [Fact]
    public async Task Fluent_OnHandler_FiresOnSave()
    {
        BareActor? captured = null;
        TriggerKind? capturedKind = null;

        var db = CreateFluentDb(opts =>
        {
            opts.On<BareActor>((actor, kind, _) =>
            {
                captured = actor;
                capturedKind = kind;
                return Task.CompletedTask;
            });
        });

        await db.SaveAsync(new BareActor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        Assert.NotNull(captured);
        Assert.Equal("alice", captured!.Username);
        Assert.Equal(TriggerKind.Saved, capturedKind);
    }

    [Fact]
    public async Task Fluent_OnHandler_FiresOnDelete()
    {
        TriggerKind? capturedKind = null;

        var db = CreateFluentDb(opts =>
        {
            opts.On<BareActor>((_, kind, __) =>
            {
                capturedKind = kind;
                return Task.CompletedTask;
            });
        });

        await db.SaveAsync(new BareActor { Username = "alice" }, TestContext.Current.CancellationToken);
        await db.DeleteAsync<BareActor>("alice", TestContext.Current.CancellationToken);

        Assert.Equal(TriggerKind.Deleted, capturedKind);
    }

    // === Custom composite key ===

    [Fact]
    public async Task Fluent_CompositeKey()
    {
        var tableClient = LottaDBFixture.CreateInMemoryTableServiceClient();
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());

        var options = new LottaConfiguration();
        options.Store<BareActor>(s =>
        {
            s.SetKey(a => $"{a.Domain}/{a.Username}");
        });

        var db = new LottaDB($"test{Guid.NewGuid():N}", tableClient, directory, options);

        await db.SaveAsync(new BareActor { Domain = "example.com", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BareActor>("example.com/alice", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.DisplayName);
    }
}
