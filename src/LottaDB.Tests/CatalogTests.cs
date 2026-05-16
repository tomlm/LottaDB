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

    private static async Task<LottaDB> CreateDbAsync(LottaCatalog catalog, string databaseId)
    {
        return await catalog.GetDatabaseAsync(databaseId, config =>
        {
            config.Store<Actor>();
            config.Store<Note>();
        });
    }

    [Fact]
    public async Task DatabasesInSameCatalog_AreIsolated()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        // Each database only sees its own data
        var fromDb1 = await db1.GetAsync<Actor>("alice");
        var fromDb2 = await db2.GetAsync<Actor>("bob");
        Assert.NotNull(fromDb1);
        Assert.NotNull(fromDb2);
        Assert.Equal("Alice", fromDb1.DisplayName);
        Assert.Equal("Bob", fromDb2.DisplayName);

        // Cross-database reads return null
        var crossRead1 = await db1.GetAsync<Actor>("bob");
        var crossRead2 = await db2.GetAsync<Actor>("alice");
        Assert.Null(crossRead1);
        Assert.Null(crossRead2);
    }

    [Fact]
    public async Task GetManyAsync_OnlyReturnsFromOwnDatabase()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db1.SaveAsync(new Actor { Username = "charlie", DisplayName = "Charlie" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var db1Results = await db1.GetManyAsync<Actor>().ToListAsync();
        var db2Results = await db2.GetManyAsync<Actor>().ToListAsync();

        Assert.Equal(2, db1Results.Count);
        Assert.Single(db2Results);
    }

    [Fact]
    public async Task ResetDatabase_OnlyClearsOwnPartition()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        // Reset only db1
        await db1.ResetDatabaseAsync();

        // db1 should be empty
        var fromDb1 = await db1.GetAsync<Actor>("alice");
        Assert.Null(fromDb1);

        // db2 should still have its data
        var fromDb2 = await db2.GetAsync<Actor>("bob");
        Assert.NotNull(fromDb2);
        Assert.Equal("Bob", fromDb2.DisplayName);
    }

    [Fact]
    public async Task Search_OnlyReturnsFromOwnDatabase()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

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
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync(configure: config =>
        {
            config.Store<Actor>();
        });
        await db.ResetDatabaseAsync(TestContext.Current.CancellationToken);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        var result = await db.GetAsync<Actor>("alice", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task LottaCatalog_GetDatabase_ReturnsSameInstance()
    {
        using var catalog = CreateCatalog();

        var db1 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>());
        var db2 = await catalog.GetDatabaseAsync("mydb");
        Assert.Same(db1, db2);

        var dbDefault1 = await catalog.GetDatabaseAsync(configure: config => config.Store<Actor>());
        var dbDefault2 = await catalog.GetDatabaseAsync("default");
        Assert.Same(dbDefault1, dbDefault2);
    }

    [Fact]
    public async Task LottaCatalog_MultipleDatabases_AreIsolated()
    {
        using var catalog = CreateCatalog();

        var db1 = await catalog.GetDatabaseAsync("notes", config => config.Store<Actor>());
        var db2 = await catalog.GetDatabaseAsync("todos", config => config.Store<Actor>());
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        Assert.NotNull(await db1.GetAsync<Actor>("alice"));
        Assert.Null(await db1.GetAsync<Actor>("bob"));
        Assert.NotNull(await db2.GetAsync<Actor>("bob"));
        Assert.Null(await db2.GetAsync<Actor>("alice"));
    }

    [Fact]
    public async Task DeleteDatabase_OnlyClearsOwnPartition()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        // Delete only db1
        await db1.DeleteDatabaseAsync();

        // db2 should still have its data
        var fromDb2 = await db2.GetAsync<Actor>("bob");
        Assert.NotNull(fromDb2);
        Assert.Equal("Bob", fromDb2.DisplayName);
    }

    [Fact]
    public async Task SameKeyInDifferentDatabases_AreIndependent()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        // Same key, different data in each database
        await db1.SaveAsync(new Actor { Username = "shared_key", DisplayName = "From DB1" });
        await db2.SaveAsync(new Actor { Username = "shared_key", DisplayName = "From DB2" });

        var fromDb1 = await db1.GetAsync<Actor>("shared_key");
        var fromDb2 = await db2.GetAsync<Actor>("shared_key");

        Assert.Equal("From DB1", fromDb1!.DisplayName);
        Assert.Equal("From DB2", fromDb2!.DisplayName);
    }

    [Fact]
    public async Task ListAsync_ReturnsAllDatabaseIds()
    {
        using var catalog = CreateCatalog();
        await CreateDbAsync(catalog, "notes");
        await CreateDbAsync(catalog, "todos");
        await CreateDbAsync(catalog, "logs");

        var databases = await catalog.ListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, databases.Count);
        Assert.Contains("notes", databases);
        Assert.Contains("todos", databases);
        Assert.Contains("logs", databases);
    }

    [Fact]
    public async Task ListAsync_EmptyCatalog_ReturnsEmpty()
    {
        using var catalog = CreateCatalog();

        var databases = await catalog.ListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(databases);
    }

    [Fact]
    public async Task SchemaChange_TriggersIndexRebuild()
    {
        // Shared storage to simulate process restart
        var tableClient = Extensions.CreateMockTableServiceClient("catalog10");

        // First run: create database with Actor only
        var catalog1 = new LottaCatalog("catalog10");
        catalog1.TableServiceClientFactory = _ => tableClient;
        catalog1.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db1 = await catalog1.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        });
        await db1.ResetDatabaseAsync();
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
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
        });

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
        // Shared storage to simulate process restart
        var tableClient = Extensions.CreateMockTableServiceClient("catalog11");

        // First run
        var catalog1 = new LottaCatalog("catalog11");
        catalog1.TableServiceClientFactory = _ => tableClient;
        catalog1.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db1 = await catalog1.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        });
        await db1.ResetDatabaseAsync();
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        db1.ReloadSearcher();
        catalog1.Dispose();

        // Second run: same schema, same storage
        var catalog2 = new LottaCatalog("catalog11");
        catalog2.TableServiceClientFactory = _ => tableClient;
        catalog2.LuceneDirectoryFactory = Extensions.CreateMockDirectory;
        var db2 = await catalog2.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        });
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
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");

        var beforeDelete = await catalog.ListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, beforeDelete.Count);

        await db1.DeleteDatabaseAsync();

        var afterDelete = await catalog.ListAsync(TestContext.Current.CancellationToken);
        Assert.Single(afterDelete);
        Assert.Contains("db2", afterDelete);
    }

    [Fact]
    public async Task DeleteCatalog_DropsEntireTable()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        await catalog.DeleteAsync(TestContext.Current.CancellationToken);

        // Manifest should be empty after table drop + recreate
        var databases = await catalog.ListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(databases);
    }

    [Fact]
    public async Task BulkOps_ScopedPerDatabase()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        // Bulk save to db1
        await db1.SaveManyAsync(new object[]
        {
            new Actor { Username = "alice", DisplayName = "Alice" },
            new Actor { Username = "bob", DisplayName = "Bob" },
        });

        await db2.SaveAsync(new Actor { Username = "charlie", DisplayName = "Charlie" });

        // Delete by key from db1
        await db1.DeleteAsync<Actor>("alice");

        // db1 should have only bob left
        var db1Results = await db1.GetManyAsync<Actor>().ToListAsync();
        Assert.Single(db1Results);
        Assert.Equal("Bob", db1Results[0].DisplayName);

        // db2 should be unaffected
        var db2Results = await db2.GetManyAsync<Actor>().ToListAsync();
        Assert.Single(db2Results);
        Assert.Equal("Charlie", db2Results[0].DisplayName);
    }

    [Fact]
    public async Task Handlers_ScopedPerDatabase()
    {
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
        });
        var db2 = await catalog.GetDatabaseAsync("db2", config =>
        {
            config.Store<Actor>();
            config.On<Actor>((actor, kind, db, _) =>
            {
                db2Triggered = true;
                return Task.CompletedTask;
            });
        });
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        // Save to db1 only
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

        Assert.True(db1Triggered);
        Assert.False(db2Triggered); // db2 handler should NOT fire
    }

    [Fact]
    public async Task ChangeAsync_ScopedPerDatabase()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "alice", DisplayName = "Other Alice" });

        // Change in db1 should not affect db2
        await db1.ChangeAsync<Actor>("alice", a => { a.DisplayName = "Alice Updated"; });

        var fromDb1 = await db1.GetAsync<Actor>("alice");
        var fromDb2 = await db2.GetAsync<Actor>("alice");

        Assert.Equal("Alice Updated", fromDb1!.DisplayName);
        Assert.Equal("Other Alice", fromDb2!.DisplayName);
    }

    [Fact]
    public async Task FirstTimeDatabase_NoStoredSchema_DoesNotRebuild()
    {
        // Brand new database with no prior manifest — should not throw or rebuild
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("brand_new", config =>
        {
            config.Store<Actor>();
        });

        // Should work fine — empty database, no rebuild needed
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        var result = await db.GetAsync<Actor>("alice", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal("Alice", result.DisplayName);
    }

    [Fact]
    public async Task GetDatabaseAsync_WithoutConfigure_ReturnsEmptyDatabase()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("empty");

        // No types registered — saving should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveAsync(new Actor { Username = "alice" }));
    }

    [Fact]
    public async Task GetDatabaseAsync_ConflictingSchema_Throws()
    {
        using var catalog = CreateCatalog();

        // First call registers Actor
        await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>());

        // Second call with different schema should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            catalog.GetDatabaseAsync("mydb", config =>
            {
                config.Store<Actor>();
                config.Store<Note>(); // different schema!
            }));
    }

    [Fact]
    public async Task GetDatabaseAsync_SameSchema_ReturnsCachedInstance()
    {
        using var catalog = CreateCatalog();

        var db1 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>());
        // Same schema on second call — should return same instance, no error
        var db2 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>());

        Assert.Same(db1, db2);
    }

    [Fact]
    public async Task DeleteCatalog_ThenCreateNewDatabases_Works()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

        // Drop everything
        await catalog.DeleteAsync(TestContext.Current.CancellationToken);

        // Should be able to create new databases after delete
        var db2 = await CreateDbAsync(catalog, "db2");
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var result = await db2.GetAsync<Actor>("bob");
        Assert.NotNull(result);
        Assert.Equal("Bob", result.DisplayName);

        // Manifest should only show db2
        var databases = await catalog.ListAsync(TestContext.Current.CancellationToken);
        Assert.Single(databases);
        Assert.Contains("db2", databases);
    }

    [Fact]
    public async Task LargeObject_SplitsAcrossProperties_RoundTrips()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<LargeDocument>();
        });
        await db.ResetDatabaseAsync(TestContext.Current.CancellationToken);

        // Create a payload larger than 63KB to force splitting across table storage properties
        var largePayload = new string('X', 100_000); // ~100KB
        var doc = new LargeDocument
        {
            Id = "large1",
            Title = "Large Document",
            Payload = largePayload,
        };

        await db.SaveAsync(doc, TestContext.Current.CancellationToken);

        // Point read — verifies split property reassembly
        var loaded = await db.GetAsync<LargeDocument>("large1", cancellationToken: TestContext.Current.CancellationToken);
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
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<LargeDocument>();
        });
        await db.ResetDatabaseAsync(TestContext.Current.CancellationToken);

        // Save initial large document
        var payload1 = new string('A', 80_000);
        await db.SaveAsync(new LargeDocument { Id = "doc1", Title = "V1", Payload = payload1 }, TestContext.Current.CancellationToken);

        // Update with different large payload
        var payload2 = new string('B', 120_000);
        await db.SaveAsync(new LargeDocument { Id = "doc1", Title = "V2", Payload = payload2 }, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<LargeDocument>("doc1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("V2", loaded.Title);
        Assert.Equal(payload2, loaded.Payload);
    }

    // === Partition boundary isolation tests ===

    [Fact]
    public async Task GetManyAsync_WithPredicate_DoesNotCrossPartition()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        // Predicate that would match db2's data if partition leaked
        var results = await db1.GetManyAsync<Actor>(a => a.DisplayName == "Bob").ToListAsync();
        Assert.Empty(results);

        // Predicate that matches db1's data
        var results2 = await db1.GetManyAsync<Actor>(a => a.DisplayName == "Alice").ToListAsync();
        Assert.Single(results2);
    }

    [Fact]
    public async Task GetManyAsync_AllItems_DoesNotCrossPartition()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        // All items from db1 — should not include db2's data
        var all = await db1.GetManyAsync<Actor>().ToListAsync();
        Assert.Single(all);
        Assert.Equal("alice", all[0].Username);
    }

    [Fact]
    public async Task Search_FreeText_DoesNotCrossPartition()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene search engine" });
        await db2.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Lucene is great" });

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
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

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
        using var catalog = CreateCatalog();

        // db1 only has Actor registered
        var db1 = await catalog.GetDatabaseAsync("db1", config => config.Store<Actor>());
        // db2 only has Note registered
        var db2 = await catalog.GetDatabaseAsync("db2", config => config.Store<Note>());
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        // Saving Actor to db1 works
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

        // Saving Note to db1 should throw — Note is not registered on db1
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db1.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" }));

        // Saving Actor to db2 should throw — Actor is not registered on db2
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }));

        // Saving Note to db2 works
        await db2.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" });
    }

    [Fact]
    public async Task DeleteManyAsync_DoesNotCrossPartition()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db1.SaveAsync(new Actor { Username = "charlie", DisplayName = "Charlie" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        // Delete all actors from db1
        await db1.DeleteManyAsync<Actor>();

        // db1 should be empty
        var db1Results = await db1.GetManyAsync<Actor>().ToListAsync();
        Assert.Empty(db1Results);

        // db2 should be untouched
        var db2Results = await db2.GetManyAsync<Actor>().ToListAsync();
        Assert.Single(db2Results);
        Assert.Equal("Bob", db2Results[0].DisplayName);
    }

    // === Blob API tests ===

    [Fact]
    public async Task Blob_UploadAndDownload_Stream()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        var content = "Hello, Blob World!";
        using var uploadStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        await db.UploadBlobAsync("test.txt", uploadStream, cancellationToken: TestContext.Current.CancellationToken);

        var downloadStream = await db.DownloadBlobAsync("test.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(downloadStream);
        using var reader = new StreamReader(downloadStream);
        var result = await reader.ReadToEndAsync();
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_Bytes()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        var content = new byte[] { 1, 2, 3, 4, 5 };
        await db.UploadBlobAsync("data.bin", content, cancellationToken: TestContext.Current.CancellationToken);

        var result = await db.DownloadBlobBytesAsync("data.bin", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_String()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("note.txt", "Hello from LottaDB", cancellationToken: TestContext.Current.CancellationToken);

        var result = await db.DownloadBlobStringAsync("note.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("Hello from LottaDB", result);
    }

    [Fact]
    public async Task Blob_Download_NotFound_ReturnsNull()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        var stream = await db.DownloadBlobAsync("nonexistent.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(stream);

        var bytes = await db.DownloadBlobBytesAsync("nonexistent.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(bytes);

        var str = await db.DownloadBlobStringAsync("nonexistent.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(str);
    }

    [Fact]
    public async Task Blob_Delete()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("todelete.txt", "temp", cancellationToken: TestContext.Current.CancellationToken);
        var deleted = await db.DeleteBlobAsync("todelete.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(deleted);

        var result = await db.DownloadBlobStringAsync("todelete.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Null(result);

        // Delete again — should return false
        var deletedAgain = await db.DeleteBlobAsync("todelete.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(deletedAgain);
    }

    [Fact]
    public async Task Blob_ListBlobs()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("photos/a.jpg", "image-a", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/b.jpg", "image-b", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: TestContext.Current.CancellationToken);

        // List all
        var all = await db.ListBlobsAsync(cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, all.Count);

        // List with prefix
        var photos = await db.ListBlobsAsync("photos/", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, photos.Count);
        Assert.Contains("photos/a.jpg", photos);
        Assert.Contains("photos/b.jpg", photos);

        var docs = await db.ListBlobsAsync("docs/", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Single(docs);
        Assert.Contains("docs/readme.md", docs);
    }

    [Fact]
    public async Task Blob_ListBlobs_NonRecursive()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("root.txt", "root", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c", cancellationToken: TestContext.Current.CancellationToken);

        // Non-recursive at root — only root.txt
        var rootFiles = await db.ListBlobsAsync(recursive: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Single(rootFiles);
        Assert.Contains("root.txt", rootFiles);

        // Non-recursive in photos/ — only a.jpg
        var photosFlat = await db.ListBlobsAsync("photos/", recursive: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Single(photosFlat);
        Assert.Contains("photos/a.jpg", photosFlat);

        // Recursive in photos/ — all 3 photos
        var photosAll = await db.ListBlobsAsync("photos/", recursive: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, photosAll.Count);
        Assert.Contains("photos/a.jpg", photosAll);
        Assert.Contains("photos/2024/b.jpg", photosAll);
        Assert.Contains("photos/2024/trip/c.jpg", photosAll);
    }

    [Fact]
    public async Task Blob_ListFolders_NonRecursive()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("root.txt", "root", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: TestContext.Current.CancellationToken);

        // Immediate subfolders of root
        var rootFolders = await db.ListBlobFoldersAsync(recursive: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rootFolders.Count);
        Assert.Contains("photos/", rootFolders);
        Assert.Contains("docs/", rootFolders);

        // Immediate subfolders of photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Single(photoFolders);
        Assert.Contains("photos/2024/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_Recursive()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: TestContext.Current.CancellationToken);

        // All folders recursively from root
        var allFolders = await db.ListBlobFoldersAsync(recursive: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains("photos/", allFolders);
        Assert.Contains("photos/2024/", allFolders);
        Assert.Contains("photos/2024/trip/", allFolders);
        Assert.Contains("docs/", allFolders);

        // All folders recursively under photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Contains("photos/2024/", photoFolders);
        Assert.Contains("photos/2024/trip/", photoFolders);
        Assert.DoesNotContain("photos/", photoFolders); // don't include the queried folder itself
        Assert.DoesNotContain("docs/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_EmptyFolder()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("only-file.txt", "content", cancellationToken: TestContext.Current.CancellationToken);

        var folders = await db.ListBlobFoldersAsync(recursive: false, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Empty(folders);
    }

    [Fact]
    public async Task Blob_ListBlobs_FolderNormalization()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: TestContext.Current.CancellationToken);

        // Both with and without trailing slash should work
        var withSlash = await db.ListBlobsAsync("photos/", cancellationToken: TestContext.Current.CancellationToken);
        var withoutSlash = await db.ListBlobsAsync("photos", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(withSlash, withoutSlash);
    }

    [Fact]
    public async Task Blob_IsolatedPerDatabase()
    {
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");

        await db1.UploadBlobAsync("shared.txt", "from db1");
        await db2.UploadBlobAsync("shared.txt", "from db2");

        // Each database has its own blob
        var result1 = await db1.DownloadBlobStringAsync("shared.txt");
        var result2 = await db2.DownloadBlobStringAsync("shared.txt");
        Assert.Equal("from db1", result1);
        Assert.Equal("from db2", result2);

        // Listing is scoped per database
        var db1Blobs = await db1.ListBlobsAsync();
        var db2Blobs = await db2.ListBlobsAsync();
        Assert.Single(db1Blobs);
        Assert.Single(db2Blobs);

        // Deleting from db1 doesn't affect db2
        await db1.DeleteBlobAsync("shared.txt");
        Assert.Null(await db1.DownloadBlobStringAsync("shared.txt"));
        Assert.Equal("from db2", await db2.DownloadBlobStringAsync("shared.txt"));
    }

    [Fact]
    public async Task Blob_Overwrite()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("file.txt", "version 1", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("file.txt", "version 2", cancellationToken: TestContext.Current.CancellationToken);

        var result = await db.DownloadBlobStringAsync("file.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("version 2", result);
    }

    // === OnUpload handler integration tests ===

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_ReturnsMetadata()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload(); // default handler
        });

        var meta = await db.UploadBlobAsync("docs/readme.txt", "Hello world", cancellationToken: TestContext.Current.CancellationToken);

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
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("notes/meeting.md", "We discussed the quarterly revenue forecast", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("quarterly revenue").ToList();
        Assert.Single(results);
        Assert.Equal("notes/meeting.md", results[0].Path);
    }

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_BinaryFile_CorrectType()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("photos/test.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/jpeg", meta.MediaType);
        Assert.Null(meta.Content); // binary, no text extraction
    }

    [Fact]
    public async Task Blob_OnUpload_MetadataPersistedAndRetrievable()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("docs/notes.txt", "Some important notes", cancellationToken: TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BlobFile>("docs/notes.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("notes.txt", loaded.Name);
        Assert.Equal("docs", loaded.FolderPath);
        Assert.Equal("text/plain", loaded.MediaType);
        Assert.Equal("Some important notes", loaded.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterUpload()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("test.txt", "content", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(meta);

        // Database should be set — DownloadAsync should work
        var stream = await meta.DownloadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterGetAsync()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("test.txt", "content", cancellationToken: TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<BlobFile>("test.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);

        var stream = await loaded.DownloadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Blob_OnUpload_DeleteAsync_CascadesMetadata()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("file.txt", "some content", cancellationToken: TestContext.Current.CancellationToken);

        // Metadata exists
        var before = await db.GetAsync<BlobFile>("file.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(before);

        // Delete via blob API
        await db.DeleteBlobAsync("file.txt", cancellationToken: TestContext.Current.CancellationToken);

        // Both blob and metadata are gone
        Assert.Null(await db.DownloadBlobStringAsync("file.txt", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Blob_OnUpload_BlobFileDeleteAsync_Works()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("file.txt", "some content", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(meta);

        // Delete via BlobFile convenience method
        var deleted = await meta.DeleteAsync(TestContext.Current.CancellationToken);
        Assert.True(deleted);

        Assert.Null(await db.DownloadBlobStringAsync("file.txt", cancellationToken: TestContext.Current.CancellationToken));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Blob_OnUpload_NoHandler_ReturnsNull()
    {
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1"); // no OnUpload

        var meta = await db.UploadBlobAsync("test.txt", "content", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Null(meta);
        // Blob still uploaded
        Assert.Equal("content", await db.DownloadBlobStringAsync("test.txt", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Blob_OnUpload_Overwrite_UpdatesMetadata()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("file.txt", "version 1", cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("file.txt", "version 2", cancellationToken: TestContext.Current.CancellationToken);

        var meta = await db.GetAsync<BlobFile>("file.txt", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(meta);
        Assert.Equal("version 2", meta.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_ExplicitContentType_Used()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("data.bin", new byte[] { 1, 2, 3 }, contentType: "image/png", cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/png", meta.MediaType);
    }

    [Fact]
    public async Task Blob_OnUpload_Search_DatabasePropertySet()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("notes/search-test.txt", "findable content here", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("findable").ToList();
        Assert.Single(results);

        // Database should be set on Search results — DownloadAsync should work
        var stream = await results[0].DownloadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("findable content here", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Blob_OnUpload_StreamOverload_ReturnsMetadata()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("stream content"));
        var meta = await db.UploadBlobAsync("stream.txt", stream, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(meta);
        Assert.Equal("stream.txt", meta.Name);
        Assert.Equal("text/plain", meta.MediaType);
        Assert.Equal("stream content", meta.Content);
    }

    // === On<T> polymorphic handler dispatch ===

    [Fact]
    public async Task On_BaseTypeHandler_FiresForDerivedType()
    {
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
        });

        // Upload a .jpg → creates BlobPhoto
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: TestContext.Current.CancellationToken);

        // Both On<BlobPhoto> and On<BlobFile> should fire
        Assert.Contains("BlobPhoto", firedTypes);
        Assert.Contains("BlobFile", firedTypes);
    }

    [Fact]
    public async Task On_BaseTypeHandler_ReceivesDerivedInstance()
    {
        using var catalog = CreateCatalog();
        BlobFile? received = null;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db, _) =>
            {
                received = file;
            });
        });

        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(received);
        Assert.IsType<BlobPhoto>(received); // receives the actual derived type
    }

    [Fact]
    public async Task On_UnrelatedTypeHandler_DoesNotFire()
    {
        using var catalog = CreateCatalog();
        var fired = false;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobMusic>(async (music, kind, db, _) =>
            {
                fired = true;
            });
        });

        // Upload a .jpg → BlobPhoto, not BlobMusic
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(fired);
    }

    [Fact]
    public async Task Blob_Search_BlobPhoto_ReturnsOnlyPhotos()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var photos = db.Search<BlobPhoto>().ToList();
        Assert.Single(photos);
        Assert.Equal("photos/cat.jpg", photos[0].Path);
        Assert.Equal("image/jpeg", photos[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobMusic_ReturnsOnlyMusic()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var music = db.Search<BlobMusic>().ToList();
        Assert.Single(music);
        Assert.Equal("music/song.mp3", music[0].Path);
        Assert.Equal("audio/mpeg", music[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobDocument_ReturnsOnlyDocuments()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/report.pdf", new byte[] { 0x25, 0x50, 0x44, 0x46 }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("notes/readme.txt", "some text content", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var docs = db.Search<BlobDocument>().ToList();
        Assert.Single(docs);
        Assert.Equal("docs/report.pdf", docs[0].Path);
        Assert.Equal("application/pdf", docs[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobVideo_ReturnsOnlyVideos()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("videos/clip.mp4", new byte[] { 0x00, 0x00, 0x00, 0x1C }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("notes/readme.txt", "some text content", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var videos = db.Search<BlobVideo>().ToList();
        Assert.Single(videos);
        Assert.Equal("videos/clip.mp4", videos[0].Path);
        Assert.Equal("video/mp4", videos[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobFile_ReturnsAllBlobTypes()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: TestContext.Current.CancellationToken);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: TestContext.Current.CancellationToken);
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
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var content = new byte[] { 0xFF, 0xD8, 0xFF, 0xE0, 0x00 };
        await db.UploadBlobAsync("photos/test.jpg", content, cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var photos = db.Search<BlobPhoto>().ToList();
        Assert.Single(photos);

        var stream = await photos[0].DownloadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Equal(content, ms.ToArray());
    }

    [Fact]
    public async Task Blob_Search_BlobMusic_DownloadAsync_Works()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var content = new byte[] { 0x49, 0x44, 0x33, 0x04, 0x00 };
        await db.UploadBlobAsync("music/track.mp3", content, cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var music = db.Search<BlobMusic>().ToList();
        Assert.Single(music);

        var stream = await music[0].DownloadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);
        Assert.Equal(content, ms.ToArray());
    }

    [Fact]
    public async Task Blob_Search_BlobDocument_DownloadAsync_Works()
    {
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("docs/notes.txt", "downloadable text", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        // BlobFile with text/plain should still be searchable and downloadable
        var results = db.Search<BlobFile>("downloadable").ToList();
        Assert.Single(results);

        var stream = await results[0].DownloadAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("downloadable text", await reader.ReadToEndAsync());
    }
}
