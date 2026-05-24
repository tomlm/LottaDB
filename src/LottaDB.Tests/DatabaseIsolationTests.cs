using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class DatabaseIsolationTests : LottaTestBase
{
    [Fact]
    public async Task DatabasesInSameCatalog_AreIsolated()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        // Each database only sees its own data
        var fromDb1 = await db1.GetAsync<Actor>("alice", ct);
        var fromDb2 = await db2.GetAsync<Actor>("bob", ct);
        Assert.NotNull(fromDb1);
        Assert.NotNull(fromDb2);
        Assert.Equal("Alice", fromDb1.DisplayName);
        Assert.Equal("Bob", fromDb2.DisplayName);

        // Cross-database reads return null
        var crossRead1 = await db1.GetAsync<Actor>("bob", ct);
        var crossRead2 = await db2.GetAsync<Actor>("alice", ct);
        Assert.Null(crossRead1);
        Assert.Null(crossRead2);
    }

    [Fact]
    public async Task GetManyAsync_OnlyReturnsFromOwnDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db1.SaveAsync(new Actor { Username = "charlie", DisplayName = "Charlie" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        var db1Results = await db1.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        var db2Results = await db2.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);

        Assert.Equal(2, db1Results.Count);
        Assert.Single(db2Results);
    }

    [Fact]
    public async Task ResetDatabase_OnlyClearsOwnPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        // Reset only db1
        await db1.ResetDatabaseAsync(ct);

        // db1 should be empty
        var fromDb1 = await db1.GetAsync<Actor>("alice", ct);
        Assert.Null(fromDb1);

        // db2 should still have its data
        var fromDb2 = await db2.GetAsync<Actor>("bob", ct);
        Assert.NotNull(fromDb2);
        Assert.Equal("Bob", fromDb2.DisplayName);
    }

    [Fact]
    public async Task Search_OnlyReturnsFromOwnDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        db1.ReloadSearcher();
        db2.ReloadSearcher();

        var search1 = db1.Search<Actor>().ToList();
        var search2 = db2.Search<Actor>().ToList();

        Assert.Single(search1);
        Assert.Equal("alice", search1[0].Username);
        Assert.Single(search2);
        Assert.Equal("bob", search2[0].Username);
    }

    [Fact]
    public async Task DefaultDatabaseId_IsDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync(configure: config =>
        {
            config.Store<Actor>();
        }, cancellationToken: ct);
        await db.ResetDatabaseAsync(ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        var result = await db.GetAsync<Actor>("alice", cancellationToken: ct);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task LottaCatalog_GetDatabase_ReturnsSameInstance()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();

        var db1 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>(), ct);
        var db2 = await catalog.GetDatabaseAsync("mydb", cancellationToken: ct);
        Assert.Same(db1, db2);

        var dbDefault1 = await catalog.GetDatabaseAsync(configure: config => config.Store<Actor>(), cancellationToken: ct);
        var dbDefault2 = await catalog.GetDatabaseAsync("default", cancellationToken: ct);
        Assert.Same(dbDefault1, dbDefault2);
    }

    [Fact]
    public async Task LottaCatalog_MultipleDatabases_AreIsolated()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();

        var db1 = await catalog.GetDatabaseAsync("notes", config => config.Store<Actor>(), ct);
        var db2 = await catalog.GetDatabaseAsync("todos", config => config.Store<Actor>(), ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        Assert.NotNull(await db1.GetAsync<Actor>("alice", ct));
        Assert.Null(await db1.GetAsync<Actor>("bob", ct));
        Assert.NotNull(await db2.GetAsync<Actor>("bob", ct));
        Assert.Null(await db2.GetAsync<Actor>("alice", ct));
    }

    // === Partition boundary isolation tests ===

    [Fact]
    public async Task GetManyAsync_WithPredicate_DoesNotCrossPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        // Predicate that would match db2's data if partition leaked
        var results = await db1.GetManyAsync<Actor>(a => a.DisplayName == "Bob", cancellationToken: ct).ToListAsync(ct);
        Assert.Empty(results);

        // Predicate that matches db1's data
        var results2 = await db1.GetManyAsync<Actor>(a => a.DisplayName == "Alice", cancellationToken: ct).ToListAsync(ct);
        Assert.Single(results2);
    }

    [Fact]
    public async Task GetManyAsync_AllItems_DoesNotCrossPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        // All items from db1 — should not include db2's data
        var all = await db1.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(all);
        Assert.Equal("alice", all[0].Username);
    }

    [Fact]
    public async Task Search_FreeText_DoesNotCrossPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene search engine" }, ct);
        await db2.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Lucene is great" }, ct);

        db1.ReloadSearcher();
        db2.ReloadSearcher();

        // Free-text search
        var results1 = db1.Search<Note>("lucene").ToList();
        Assert.Single(results1);
        Assert.Equal("n1", results1[0].NoteId);

        var results2 = db2.Search<Note>("lucene").ToList();
        Assert.Single(results2);
        Assert.Equal("n2", results2[0].NoteId);
    }

    [Fact]
    public async Task Search_WithPredicate_DoesNotCrossPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        db1.ReloadSearcher();
        db2.ReloadSearcher();

        // LINQ predicate search — should not find db2's data
        var results = db1.Search<Actor>(a => a.DisplayName == "Bob").ToList();
        Assert.Empty(results);

        var results2 = db1.Search<Actor>(a => a.DisplayName == "Alice").ToList();
        Assert.Single(results2);
    }

    [Fact]
    public async Task CrossTypeStorage_UnregisteredType_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();

        // db1 only has Actor registered
        var db1 = await catalog.GetDatabaseAsync("db1", config => config.Store<Actor>(), ct);
        // db2 only has Note registered
        var db2 = await catalog.GetDatabaseAsync("db2", config => config.Store<Note>(), ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        // Saving Actor to db1 works
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        // Saving Note to db1 should throw — Note is not registered on db1
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db1.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" }, ct));

        // Saving Actor to db2 should throw — Actor is not registered on db2
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct));

        // Saving Note to db2 works
        await db2.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" }, ct);
    }

    [Fact]
    public async Task DeleteManyAsync_DoesNotCrossPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db1.SaveAsync(new Actor { Username = "charlie", DisplayName = "Charlie" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        // Delete all actors from db1
        await db1.DeleteManyAsync<Actor>(cancellationToken: ct);

        // db1 should be empty
        var db1Results = await db1.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Empty(db1Results);

        // db2 should be untouched
        var db2Results = await db2.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(db2Results);
        Assert.Equal("Bob", db2Results[0].DisplayName);
    }

    // === Bulk ops and handler scope tests ===

    [Fact]
    public async Task BulkOps_ScopedPerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        // Bulk save to db1
        await db1.SaveManyAsync(new object[]
        {
            new Actor { Username = "alice", DisplayName = "Alice" },
            new Actor { Username = "bob", DisplayName = "Bob" },
        }, ct);

        await db2.SaveAsync(new Actor { Username = "charlie", DisplayName = "Charlie" }, ct);

        // Delete by key from db1
        await db1.DeleteAsync<Actor>("alice", ct);

        // db1 should have only bob left
        var db1Results = await db1.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(db1Results);
        Assert.Equal("Bob", db1Results[0].DisplayName);

        // db2 should be unaffected
        var db2Results = await db2.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(db2Results);
        Assert.Equal("Charlie", db2Results[0].DisplayName);
    }

    [Fact]
    public async Task Handlers_ScopedPerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1Triggered = false;
        var db2Triggered = false;

        var db1 = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.Store<Actor>();
            config.On<Actor>((actor, kind, db, _) =>
            {
                db1Triggered = true;
                return Task.CompletedTask;
            });
        }, ct);
        var db2 = await catalog.GetDatabaseAsync("db2", config =>
        {
            config.Store<Actor>();
            config.On<Actor>((actor, kind, db, _) =>
            {
                db2Triggered = true;
                return Task.CompletedTask;
            });
        }, ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        // Save to db1 only
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        Assert.True(db1Triggered);
        Assert.False(db2Triggered); // db2 handler should NOT fire
    }

    [Fact]
    public async Task ChangeAsync_ScopedPerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "alice", DisplayName = "Other Alice" }, ct);

        // Change in db1 should not affect db2
        await db1.ChangeAsync<Actor>("alice", a => { a.DisplayName = "Alice Updated"; }, ct);

        var fromDb1 = await db1.GetAsync<Actor>("alice", ct);
        var fromDb2 = await db2.GetAsync<Actor>("alice", ct);

        Assert.Equal("Alice Updated", fromDb1!.DisplayName);
        Assert.Equal("Other Alice", fromDb2!.DisplayName);
    }

    [Fact]
    public async Task SameKeyInDifferentDatabases_AreIndependent()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        // Same key, different data in each database
        await db1.SaveAsync(new Actor { Username = "shared_key", DisplayName = "From DB1" }, ct);
        await db2.SaveAsync(new Actor { Username = "shared_key", DisplayName = "From DB2" }, ct);

        var fromDb1 = await db1.GetAsync<Actor>("shared_key", ct);
        var fromDb2 = await db2.GetAsync<Actor>("shared_key", ct);

        Assert.Equal("From DB1", fromDb1!.DisplayName);
        Assert.Equal("From DB2", fromDb2!.DisplayName);
    }
}
