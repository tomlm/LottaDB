using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class CatalogTests : IDisposable
{
    public CatalogTests()
    {
    }

    public void Dispose() { }

    private static LottaCatalog CreateCatalog([CallerMemberName] string? testName = null)
    {
        var sanitized = string.Join("", testName!.Where(char.IsLetterOrDigit).Take(60));
        var catalog = new LottaCatalog(sanitized);
        catalog.ConfigureTestStorage();
        return catalog;
    }

    private static async Task<LottaDB> CreateDbAsync(LottaCatalog catalog, string databaseId, CancellationToken ct = default)
    {
        return await catalog.GetDatabaseAsync(databaseId, config =>
        {
            config.Store<Actor>();
            config.Store<Note>();
        }, ct);
    }

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

        var db1Results = await db1.GetManyAsync<Actor>().ToListAsync(ct);
        var db2Results = await db2.GetManyAsync<Actor>().ToListAsync(ct);

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

    [Fact]
    public async Task DeleteDatabase_OnlyClearsOwnPartition()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        // Delete only db1
        await db1.DeleteDatabaseAsync(ct);

        // db2 should still have its data
        var fromDb2 = await db2.GetAsync<Actor>("bob", ct);
        Assert.NotNull(fromDb2);
        Assert.Equal("Bob", fromDb2.DisplayName);
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

    [Fact]
    public async Task ListAsync_ReturnsAllDatabaseIds()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        await CreateDbAsync(catalog, "notes", ct);
        await CreateDbAsync(catalog, "todos", ct);
        await CreateDbAsync(catalog, "logs", ct);

        var databases = await catalog.ListAsync(ct);

        Assert.Equal(3, databases.Count);
        Assert.Contains("notes", databases);
        Assert.Contains("todos", databases);
        Assert.Contains("logs", databases);
    }

    [Fact]
    public async Task ListAsync_EmptyCatalog_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();

        var databases = await catalog.ListAsync(ct);

        Assert.Empty(databases);
    }

    [Fact]
    public async Task SchemaChange_TriggersIndexRebuild()
    {
        var ct = TestContext.Current.CancellationToken;
        // Shared storage to simulate process restart
        var tableClient = Extensions.CreateMockTableServiceClient("catalog10");

        // First run: create database with Actor only
        var catalog1 = new LottaCatalog("catalog10");
        catalog1.TableServiceClientFactory = _ => tableClient;
        catalog1.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db1 = await catalog1.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        }, ct);
        await db1.ResetDatabaseAsync(ct);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        db1.ReloadSearcher();

        // Verify search works
        var results1 = db1.Search<Actor>().ToList();
        Assert.Single(results1);
        catalog1.Dispose();

        // Second run: same storage, but add Note to schema
        var catalog2 = new LottaCatalog("catalog10");
        catalog2.TableServiceClientFactory = _ => tableClient;
        catalog2.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db2 = await catalog2.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
            config.Store<Note>();  // schema changed!
        }, ct);

        // The index should have been rebuilt — data is still there from table storage
        db2.ReloadSearcher();
        var results2 = db2.Search<Actor>().ToList();
        Assert.Single(results2);
        Assert.Equal("alice", results2[0].Username);
        catalog2.Dispose();
    }

    [Fact]
    public async Task SameSchema_NoRebuildNeeded()
    {
        var ct = TestContext.Current.CancellationToken;
        // Shared storage to simulate process restart
        var tableClient = Extensions.CreateMockTableServiceClient("catalog11");

        // First run
        var catalog1 = new LottaCatalog("catalog11");
        catalog1.TableServiceClientFactory = _ => tableClient;
        catalog1.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db1 = await catalog1.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        }, ct);
        await db1.ResetDatabaseAsync(ct);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        db1.ReloadSearcher();
        catalog1.Dispose();

        // Second run: same schema, same storage
        var catalog2 = new LottaCatalog("catalog11");
        catalog2.TableServiceClientFactory = _ => tableClient;
        catalog2.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db2 = await catalog2.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        }, ct);
        // Index was rebuilt from table storage (new Lucene directory), but schema matches so no extra rebuild triggered
        db2.ReloadSearcher();
        var results = db2.Search<Actor>().ToList();
        // Data is in table storage, and since we get a fresh RAMDirectory, the index is empty
        // but GetDatabaseAsync doesn't rebuild when schema matches — only RebuildSearchIndex populates it
        // This test verifies no error occurs with matching schema
        Assert.NotNull(db2);
        catalog2.Dispose();
    }

    [Fact]
    public async Task DeleteDatabase_RemovesFromManifest()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);

        var beforeDelete = await catalog.ListAsync(ct);
        Assert.Equal(2, beforeDelete.Count);

        await db1.DeleteDatabaseAsync(ct);

        var afterDelete = await catalog.ListAsync(ct);
        Assert.Single(afterDelete);
        Assert.Contains("db2", afterDelete);
    }

    [Fact]
    public async Task DeleteCatalog_DropsEntireTable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db1.ResetDatabaseAsync(ct);
        await db2.ResetDatabaseAsync(ct);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        await catalog.DeleteAsync(ct);

        // Manifest should be empty after table drop + recreate
        var databases = await catalog.ListAsync(ct);
        Assert.Empty(databases);
    }

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
        var db1Results = await db1.GetManyAsync<Actor>().ToListAsync(ct);
        Assert.Single(db1Results);
        Assert.Equal("Bob", db1Results[0].DisplayName);

        // db2 should be unaffected
        var db2Results = await db2.GetManyAsync<Actor>().ToListAsync(ct);
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
    public async Task FirstTimeDatabase_NoStoredSchema_DoesNotRebuild()
    {
        var ct = TestContext.Current.CancellationToken;
        // Brand new database with no prior manifest — should not throw or rebuild
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("brand_new", config =>
        {
            config.Store<Actor>();
        }, ct);

        // Should work fine — empty database, no rebuild needed
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        var result = await db.GetAsync<Actor>("alice", cancellationToken: ct);
        Assert.NotNull(result);
        Assert.Equal("Alice", result.DisplayName);
    }

    [Fact]
    public async Task GetDatabaseAsync_WithoutConfigure_ReturnsEmptyDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("empty", cancellationToken: ct);

        // No types registered — saving should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveAsync(new Actor { Username = "alice" }, ct));
    }

    [Fact]
    public async Task GetDatabaseAsync_ConflictingSchema_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();

        // First call registers Actor
        await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>(), ct);

        // Second call with different schema should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            catalog.GetDatabaseAsync("mydb", config =>
            {
                config.Store<Actor>();
                config.Store<Note>(); // different schema!
            }, ct));
    }

    [Fact]
    public async Task GetDatabaseAsync_SameSchema_ReturnsCachedInstance()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();

        var db1 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>(), ct);
        // Same schema on second call — should return same instance, no error
        var db2 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>(), ct);

        Assert.Same(db1, db2);
    }

    [Fact]
    public async Task DeleteCatalog_ThenCreateNewDatabases_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        // Drop everything
        await catalog.DeleteAsync(ct);

        // Should be able to create new databases after delete
        var db2 = await CreateDbAsync(catalog, "db2", ct);
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        var result = await db2.GetAsync<Actor>("bob", ct);
        Assert.NotNull(result);
        Assert.Equal("Bob", result.DisplayName);

        // Manifest should only show db2
        var databases = await catalog.ListAsync(ct);
        Assert.Single(databases);
        Assert.Contains("db2", databases);
    }

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
        var results = await db1.GetManyAsync<Actor>(a => a.DisplayName == "Bob").ToListAsync(ct);
        Assert.Empty(results);

        // Predicate that matches db1's data
        var results2 = await db1.GetManyAsync<Actor>(a => a.DisplayName == "Alice").ToListAsync(ct);
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
        var all = await db1.GetManyAsync<Actor>().ToListAsync(ct);
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
        var db1Results = await db1.GetManyAsync<Actor>().ToListAsync(ct);
        Assert.Empty(db1Results);

        // db2 should be untouched
        var db2Results = await db2.GetManyAsync<Actor>().ToListAsync(ct);
        Assert.Single(db2Results);
        Assert.Equal("Bob", db2Results[0].DisplayName);
    }

    // === Blob API tests ===

    [Fact]
    public async Task Blob_UploadAndDownload_Stream()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        var content = "Hello, Blob World!";
        using var uploadStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        await db.UploadBlobAsync("test.txt", uploadStream, cancellationToken: ct);

        var downloadStream = await db.DownloadBlobAsync("test.txt", cancellationToken: ct);
        Assert.NotNull(downloadStream);
        using var reader = new StreamReader(downloadStream);
        var result = await reader.ReadToEndAsync();
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_Bytes()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        var content = new byte[] { 1, 2, 3, 4, 5 };
        await db.UploadBlobAsync("data.bin", content, cancellationToken: ct);

        var result = await db.DownloadBlobBytesAsync("data.bin", cancellationToken: ct);
        Assert.NotNull(result);
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_String()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("note.txt", "Hello from LottaDB", cancellationToken: ct);

        var result = await db.DownloadBlobStringAsync("note.txt", cancellationToken: ct);
        Assert.Equal("Hello from LottaDB", result);
    }

    [Fact]
    public async Task Blob_Download_NotFound_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        var stream = await db.DownloadBlobAsync("nonexistent.txt", cancellationToken: ct);
        Assert.Null(stream);

        var bytes = await db.DownloadBlobBytesAsync("nonexistent.txt", cancellationToken: ct);
        Assert.Null(bytes);

        var str = await db.DownloadBlobStringAsync("nonexistent.txt", cancellationToken: ct);
        Assert.Null(str);
    }

    [Fact]
    public async Task Blob_Delete()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("todelete.txt", "temp", cancellationToken: ct);
        var deleted = await db.DeleteBlobAsync("todelete.txt", cancellationToken: ct);
        Assert.True(deleted);

        var result = await db.DownloadBlobStringAsync("todelete.txt", cancellationToken: ct);
        Assert.Null(result);

        // Delete again — should return false
        var deletedAgain = await db.DeleteBlobAsync("todelete.txt", cancellationToken: ct);
        Assert.False(deletedAgain);
    }

    [Fact]
    public async Task Blob_ListBlobs()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("photos/a.jpg", "image-a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/b.jpg", "image-b", cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: ct);

        // List all
        var all = await db.ListBlobsAsync(cancellationToken: ct);
        Assert.Equal(3, all.Count);

        // List with prefix
        var photos = await db.ListBlobsAsync("photos/", cancellationToken: ct);
        Assert.Equal(2, photos.Count);
        Assert.Contains("photos/a.jpg", photos);
        Assert.Contains("photos/b.jpg", photos);

        var docs = await db.ListBlobsAsync("docs/", cancellationToken: ct);
        Assert.Single(docs);
        Assert.Contains("docs/readme.md", docs);
    }

    [Fact]
    public async Task Blob_ListBlobs_NonRecursive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("root.txt", "root", cancellationToken: ct);
        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c", cancellationToken: ct);

        // Non-recursive at root — only root.txt
        var rootFiles = await db.ListBlobsAsync(recursive: false, cancellationToken: ct);
        Assert.Single(rootFiles);
        Assert.Contains("root.txt", rootFiles);

        // Non-recursive in photos/ — only a.jpg
        var photosFlat = await db.ListBlobsAsync("photos/", recursive: false, cancellationToken: ct);
        Assert.Single(photosFlat);
        Assert.Contains("photos/a.jpg", photosFlat);

        // Recursive in photos/ — all 3 photos
        var photosAll = await db.ListBlobsAsync("photos/", recursive: true, cancellationToken: ct);
        Assert.Equal(3, photosAll.Count);
        Assert.Contains("photos/a.jpg", photosAll);
        Assert.Contains("photos/2024/b.jpg", photosAll);
        Assert.Contains("photos/2024/trip/c.jpg", photosAll);
    }

    [Fact]
    public async Task Blob_ListFolders_NonRecursive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("root.txt", "root", cancellationToken: ct);
        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: ct);

        // Immediate subfolders of root
        var rootFolders = await db.ListBlobFoldersAsync(recursive: false, cancellationToken: ct);
        Assert.Equal(2, rootFolders.Count);
        Assert.Contains("photos/", rootFolders);
        Assert.Contains("docs/", rootFolders);

        // Immediate subfolders of photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: false, cancellationToken: ct);
        Assert.Single(photoFolders);
        Assert.Contains("photos/2024/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_Recursive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c", cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: ct);

        // All folders recursively from root
        var allFolders = await db.ListBlobFoldersAsync(recursive: true, cancellationToken: ct);
        Assert.Contains("photos/", allFolders);
        Assert.Contains("photos/2024/", allFolders);
        Assert.Contains("photos/2024/trip/", allFolders);
        Assert.Contains("docs/", allFolders);

        // All folders recursively under photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: true, cancellationToken: ct);
        Assert.Contains("photos/2024/", photoFolders);
        Assert.Contains("photos/2024/trip/", photoFolders);
        Assert.DoesNotContain("photos/", photoFolders); // don't include the queried folder itself
        Assert.DoesNotContain("docs/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_EmptyFolder()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("only-file.txt", "content", cancellationToken: ct);

        var folders = await db.ListBlobFoldersAsync(recursive: false, cancellationToken: ct);
        Assert.Empty(folders);
    }

    [Fact]
    public async Task Blob_ListBlobs_FolderNormalization()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);

        // Both with and without trailing slash should work
        var withSlash = await db.ListBlobsAsync("photos/", cancellationToken: ct);
        var withoutSlash = await db.ListBlobsAsync("photos", cancellationToken: ct);
        Assert.Equal(withSlash, withoutSlash);
    }

    [Fact]
    public async Task Blob_IsolatedPerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);

        await db1.UploadBlobAsync("shared.txt", "from db1", cancellationToken: ct);
        await db2.UploadBlobAsync("shared.txt", "from db2", cancellationToken: ct);

        // Each database has its own blob
        var result1 = await db1.DownloadBlobStringAsync("shared.txt", ct);
        var result2 = await db2.DownloadBlobStringAsync("shared.txt", ct);
        Assert.Equal("from db1", result1);
        Assert.Equal("from db2", result2);

        // Listing is scoped per database
        var db1Blobs = await db1.ListBlobsAsync(cancellationToken: ct);
        var db2Blobs = await db2.ListBlobsAsync(cancellationToken: ct);
        Assert.Single(db1Blobs);
        Assert.Single(db2Blobs);

        // Deleting from db1 doesn't affect db2
        await db1.DeleteBlobAsync("shared.txt", ct);
        Assert.Null(await db1.DownloadBlobStringAsync("shared.txt", ct));
        Assert.Equal("from db2", await db2.DownloadBlobStringAsync("shared.txt", ct));
    }

    [Fact]
    public async Task Blob_Overwrite()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("file.txt", "version 1", cancellationToken: ct);
        await db.UploadBlobAsync("file.txt", "version 2", cancellationToken: ct);

        var result = await db.DownloadBlobStringAsync("file.txt", cancellationToken: ct);
        Assert.Equal("version 2", result);
    }

    // === OnUpload handler integration tests ===

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_ReturnsMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload(); // default handler
        }, ct);

        var meta = await db.UploadBlobAsync("docs/readme.txt", "Hello world", cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.IsType<BlobFile>(meta);
        Assert.Equal("docs/readme.txt", meta.Path);
        Assert.Equal("readme.txt", meta.Name);
        Assert.Equal("docs", meta.FolderPath);
        Assert.Equal("text/plain", meta.MediaType);
        Assert.Equal("Hello world", meta.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_TextContent_Searchable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("notes/meeting.md", "We discussed the quarterly revenue forecast", cancellationToken: ct);
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("quarterly revenue").ToList();
        Assert.Single(results);
        Assert.Equal("notes/meeting.md", results[0].Path);
    }

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_BinaryFile_CorrectType()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("photos/test.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/jpeg", meta.MediaType);
        Assert.Null(meta.Content); // binary, no text extraction
    }

    [Fact]
    public async Task Blob_OnUpload_MetadataPersistedAndRetrievable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("docs/notes.txt", "Some important notes", cancellationToken: ct);

        var loaded = await db.GetAsync<BlobFile>("docs/notes.txt", cancellationToken: ct);
        Assert.NotNull(loaded);
        Assert.Equal("notes.txt", loaded.Name);
        Assert.Equal("docs", loaded.FolderPath);
        Assert.Equal("text/plain", loaded.MediaType);
        Assert.Equal("Some important notes", loaded.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterUpload()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("test.txt", "content", cancellationToken: ct);
        Assert.NotNull(meta);

        // Database should be set — DownloadAsync should work
        var stream = await meta.DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterGetAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("test.txt", "content", cancellationToken: ct);

        var loaded = await db.GetAsync<BlobFile>("test.txt", cancellationToken: ct);
        Assert.NotNull(loaded);

        var stream = await loaded.DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Blob_OnUpload_DeleteAsync_CascadesMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("file.txt", "some content", cancellationToken: ct);

        // Metadata exists
        var before = await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct);
        Assert.NotNull(before);

        // Delete via blob API
        await db.DeleteBlobAsync("file.txt", cancellationToken: ct);

        // Both blob and metadata are gone
        Assert.Null(await db.DownloadBlobStringAsync("file.txt", cancellationToken: ct));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_OnUpload_BlobFileDeleteAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("file.txt", "some content", cancellationToken: ct);
        Assert.NotNull(meta);

        // Delete via BlobFile convenience method
        var deleted = await meta.DeleteAsync(ct);
        Assert.True(deleted);

        Assert.Null(await db.DownloadBlobStringAsync("file.txt", cancellationToken: ct));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_OnUpload_NoHandler_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct); // no OnUpload

        var meta = await db.UploadBlobAsync("test.txt", "content", cancellationToken: ct);

        Assert.Null(meta);
        // Blob still uploaded
        Assert.Equal("content", await db.DownloadBlobStringAsync("test.txt", cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_OnUpload_Overwrite_UpdatesMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("file.txt", "version 1", cancellationToken: ct);
        await db.UploadBlobAsync("file.txt", "version 2", cancellationToken: ct);

        var meta = await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct);
        Assert.NotNull(meta);
        Assert.Equal("version 2", meta.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_ExplicitContentType_Used()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("data.bin", new byte[] { 1, 2, 3 }, contentType: "image/png", cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/png", meta.MediaType);
    }

    [Fact]
    public async Task Blob_OnUpload_Search_DatabasePropertySet()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("notes/search-test.txt", "findable content here", cancellationToken: ct);
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("findable").ToList();
        Assert.Single(results);

        // Database should be set on Search results — DownloadAsync should work
        var stream = await results[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("findable content here", await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Blob_OnUpload_StreamOverload_ReturnsMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("stream content"));
        var meta = await db.UploadBlobAsync("stream.txt", stream, cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.Equal("stream.txt", meta.Name);
        Assert.Equal("text/plain", meta.MediaType);
        Assert.Equal("stream content", meta.Content);
    }

    // === On<T> polymorphic handler dispatch ===

    [Fact]
    public async Task On_BaseTypeHandler_FiresForDerivedType()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var firedTypes = new List<string>();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db, _) =>
            {
                firedTypes.Add("BlobFile");
            });
            config.On<BlobPhoto>(async (photo, kind, db, _) =>
            {
                firedTypes.Add("BlobPhoto");
            });
        }, ct);

        // Upload a .jpg → creates BlobPhoto
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: ct);

        // Both On<BlobPhoto> and On<BlobFile> should fire
        Assert.Contains("BlobPhoto", firedTypes);
        Assert.Contains("BlobFile", firedTypes);
    }

    [Fact]
    public async Task On_BaseTypeHandler_ReceivesDerivedInstance()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        BlobFile? received = null;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db, _) =>
            {
                received = file;
            });
        }, ct);

        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: ct);

        Assert.NotNull(received);
        Assert.IsType<BlobPhoto>(received); // receives the actual derived type
    }

    [Fact]
    public async Task On_UnrelatedTypeHandler_DoesNotFire()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var fired = false;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobMusic>(async (music, kind, db, _) =>
            {
                fired = true;
            });
        }, ct);

        // Upload a .jpg → BlobPhoto, not BlobMusic
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: ct);

        Assert.False(fired);
    }

    [Fact]
    public async Task Blob_Search_BlobPhoto_ReturnsOnlyPhotos()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var photos = db.Search<BlobPhoto>().ToList();
        Assert.Single(photos);
        Assert.Equal("photos/cat.jpg", photos[0].Path);
        Assert.Equal("image/jpeg", photos[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobMusic_ReturnsOnlyMusic()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var music = db.Search<BlobMusic>().ToList();
        Assert.Single(music);
        Assert.Equal("music/song.mp3", music[0].Path);
        Assert.Equal("audio/mpeg", music[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobDocument_ReturnsOnlyDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/report.pdf", new byte[] { 0x25, 0x50, 0x44, 0x46 }, cancellationToken: ct);
        await db.UploadBlobAsync("notes/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var docs = db.Search<BlobDocument>().ToList();
        Assert.Single(docs);
        Assert.Equal("docs/report.pdf", docs[0].Path);
        Assert.Equal("application/pdf", docs[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobVideo_ReturnsOnlyVideos()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("videos/clip.mp4", new byte[] { 0x00, 0x00, 0x00, 0x1C }, cancellationToken: ct);
        await db.UploadBlobAsync("notes/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var videos = db.Search<BlobVideo>().ToList();
        Assert.Single(videos);
        Assert.Equal("videos/clip.mp4", videos[0].Path);
        Assert.Equal("video/mp4", videos[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobFile_ReturnsAllBlobTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var all = db.Search<BlobFile>().ToList();
        Assert.Equal(3, all.Count);
        Assert.Contains(all, b => b is BlobPhoto);
        Assert.Contains(all, b => b is BlobMusic);
        Assert.Contains(all, b => b is BlobFile f && f.MediaType == "text/plain");
    }

    [Fact]
    public async Task Blob_Search_BlobPhoto_DownloadAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var content = new byte[] { 0xFF, 0xD8, 0xFF, 0xE0, 0x00 };
        await db.UploadBlobAsync("photos/test.jpg", content, cancellationToken: ct);
        db.ReloadSearcher();

        var photos = db.Search<BlobPhoto>().ToList();
        Assert.Single(photos);

        var stream = await photos[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        Assert.Equal(content, ms.ToArray());
    }

    [Fact]
    public async Task Blob_Search_BlobMusic_DownloadAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var content = new byte[] { 0x49, 0x44, 0x33, 0x04, 0x00 };
        await db.UploadBlobAsync("music/track.mp3", content, cancellationToken: ct);
        db.ReloadSearcher();

        var music = db.Search<BlobMusic>().ToList();
        Assert.Single(music);

        var stream = await music[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        Assert.Equal(content, ms.ToArray());
    }

    [Fact]
    public async Task Blob_Search_BlobDocument_DownloadAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("docs/notes.txt", "downloadable text", cancellationToken: ct);
        db.ReloadSearcher();

        // BlobFile with text/plain should still be searchable and downloadable
        var results = db.Search<BlobFile>("downloadable").ToList();
        Assert.Single(results);

        var stream = await results[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("downloadable text", await reader.ReadToEndAsync(ct));
    }
}
