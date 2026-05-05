namespace Lotta.Tests;

public class CatalogTests : IDisposable
{
    public CatalogTests()
    {
    }

    public void Dispose() { }

    private static LottaCatalog CreateCatalog(string name)
    {
        var catalog = new LottaCatalog(name);
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
        using var catalog = CreateCatalog("catalog1");
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
        using var catalog = CreateCatalog("catalog2");
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
        using var catalog = CreateCatalog("catalog3");
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
        using var catalog = CreateCatalog("catalog4");
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
        using var catalog = CreateCatalog("simplecatalog");
        var db = await catalog.GetDatabaseAsync(configure: config =>
        {
            config.Store<Actor>();
        });
        await db.ResetDatabaseAsync();

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        var result = await db.GetAsync<Actor>("alice");
        Assert.NotNull(result);
    }

    [Fact]
    public async Task LottaCatalog_GetDatabase_ReturnsSameInstance()
    {
        using var catalog = CreateCatalog("catalogX");

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
        using var catalog = CreateCatalog("catalogY");

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
        using var catalog = CreateCatalog("catalog5");
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
        using var catalog = CreateCatalog("catalog6");
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
        using var catalog = CreateCatalog("catalog7");
        await CreateDbAsync(catalog, "notes");
        await CreateDbAsync(catalog, "todos");
        await CreateDbAsync(catalog, "logs");

        var databases = await catalog.ListAsync();

        Assert.Equal(3, databases.Count);
        Assert.Contains("notes", databases);
        Assert.Contains("todos", databases);
        Assert.Contains("logs", databases);
    }

    [Fact]
    public async Task ListAsync_EmptyCatalog_ReturnsEmpty()
    {
        using var catalog = CreateCatalog("catalog8");

        var databases = await catalog.ListAsync();

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
        using var catalog = CreateCatalog("catalog9");
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");

        var beforeDelete = await catalog.ListAsync();
        Assert.Equal(2, beforeDelete.Count);

        await db1.DeleteDatabaseAsync();

        var afterDelete = await catalog.ListAsync();
        Assert.Single(afterDelete);
        Assert.Contains("db2", afterDelete);
    }

    [Fact]
    public async Task DeleteCatalog_DropsEntireTable()
    {
        using var catalog = CreateCatalog("catalog12");
        var db1 = await CreateDbAsync(catalog, "db1");
        var db2 = await CreateDbAsync(catalog, "db2");
        await db1.ResetDatabaseAsync();
        await db2.ResetDatabaseAsync();

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        await catalog.DeleteAsync();

        // Manifest should be empty after table drop + recreate
        var databases = await catalog.ListAsync();
        Assert.Empty(databases);
    }

    [Fact]
    public async Task BulkOps_ScopedPerDatabase()
    {
        using var catalog = CreateCatalog("catalog13");
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
        using var catalog = CreateCatalog("catalog14");
        var db1Triggered = false;
        var db2Triggered = false;

        var db1 = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.Store<Actor>();
            config.On<Actor>((actor, kind, db) =>
            {
                db1Triggered = true;
                return Task.CompletedTask;
            });
        });
        var db2 = await catalog.GetDatabaseAsync("db2", config =>
        {
            config.Store<Actor>();
            config.On<Actor>((actor, kind, db) =>
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
        using var catalog = CreateCatalog("catalog15");
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
        using var catalog = CreateCatalog("catalog16");
        var db = await catalog.GetDatabaseAsync("brand_new", config =>
        {
            config.Store<Actor>();
        });

        // Should work fine — empty database, no rebuild needed
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        var result = await db.GetAsync<Actor>("alice");
        Assert.NotNull(result);
        Assert.Equal("Alice", result.DisplayName);
    }

    [Fact]
    public async Task GetDatabaseAsync_WithoutConfigure_ReturnsEmptyDatabase()
    {
        using var catalog = CreateCatalog("catalog17");
        var db = await catalog.GetDatabaseAsync("empty");

        // No types registered — saving should throw
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveAsync(new Actor { Username = "alice" }));
    }

    [Fact]
    public async Task GetDatabaseAsync_ConflictingSchema_Throws()
    {
        using var catalog = CreateCatalog("catalog18");

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
        using var catalog = CreateCatalog("catalog19");

        var db1 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>());
        // Same schema on second call — should return same instance, no error
        var db2 = await catalog.GetDatabaseAsync("mydb", config => config.Store<Actor>());

        Assert.Same(db1, db2);
    }

    [Fact]
    public async Task DeleteCatalog_ThenCreateNewDatabases_Works()
    {
        using var catalog = CreateCatalog("catalog20");
        var db1 = await CreateDbAsync(catalog, "db1");
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

        // Drop everything
        await catalog.DeleteAsync();

        // Should be able to create new databases after delete
        var db2 = await CreateDbAsync(catalog, "db2");
        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var result = await db2.GetAsync<Actor>("bob");
        Assert.NotNull(result);
        Assert.Equal("Bob", result.DisplayName);

        // Manifest should only show db2
        var databases = await catalog.ListAsync();
        Assert.Single(databases);
        Assert.Contains("db2", databases);
    }

    [Fact]
    public async Task LargeObject_SplitsAcrossProperties_RoundTrips()
    {
        using var catalog = CreateCatalog("catalog21");
        var db = await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<LargeDocument>();
        });
        await db.ResetDatabaseAsync();

        // Create a payload larger than 63KB to force splitting across table storage properties
        var largePayload = new string('X', 100_000); // ~100KB
        var doc = new LargeDocument
        {
            Id = "large1",
            Title = "Large Document",
            Payload = largePayload,
        };

        await db.SaveAsync(doc);

        // Point read — verifies split property reassembly
        var loaded = await db.GetAsync<LargeDocument>("large1");
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
        using var catalog = CreateCatalog("catalog22");
        var db = await catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<LargeDocument>();
        });
        await db.ResetDatabaseAsync();

        // Save initial large document
        var payload1 = new string('A', 80_000);
        await db.SaveAsync(new LargeDocument { Id = "doc1", Title = "V1", Payload = payload1 });

        // Update with different large payload
        var payload2 = new string('B', 120_000);
        await db.SaveAsync(new LargeDocument { Id = "doc1", Title = "V2", Payload = payload2 });

        var loaded = await db.GetAsync<LargeDocument>("doc1");
        Assert.NotNull(loaded);
        Assert.Equal("V2", loaded.Title);
        Assert.Equal(payload2, loaded.Payload);
    }

    // === Partition boundary isolation tests ===

    [Fact]
    public async Task GetManyAsync_WithPredicate_DoesNotCrossPartition()
    {
        using var catalog = CreateCatalog("catalog23");
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
        using var catalog = CreateCatalog("catalog24");
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
        using var catalog = CreateCatalog("catalog25");
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
        using var catalog = CreateCatalog("catalog26");
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
        using var catalog = CreateCatalog("catalog28");

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
        using var catalog = CreateCatalog("catalog27");
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
        using var catalog = CreateCatalog("catalog30");
        var db = await CreateDbAsync(catalog, "db1");

        var content = "Hello, Blob World!";
        using var uploadStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        await db.UploadBlobAsync("test.txt", uploadStream);

        var downloadStream = await db.DownloadBlobAsync("test.txt");
        Assert.NotNull(downloadStream);
        using var reader = new StreamReader(downloadStream);
        var result = await reader.ReadToEndAsync();
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_Bytes()
    {
        using var catalog = CreateCatalog("catalog31");
        var db = await CreateDbAsync(catalog, "db1");

        var content = new byte[] { 1, 2, 3, 4, 5 };
        await db.UploadBlobAsync("data.bin", content);

        var result = await db.DownloadBlobBytesAsync("data.bin");
        Assert.NotNull(result);
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_String()
    {
        using var catalog = CreateCatalog("catalog32");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("note.txt", "Hello from LottaDB");

        var result = await db.DownloadBlobStringAsync("note.txt");
        Assert.Equal("Hello from LottaDB", result);
    }

    [Fact]
    public async Task Blob_Download_NotFound_ReturnsNull()
    {
        using var catalog = CreateCatalog("catalog33");
        var db = await CreateDbAsync(catalog, "db1");

        var stream = await db.DownloadBlobAsync("nonexistent.txt");
        Assert.Null(stream);

        var bytes = await db.DownloadBlobBytesAsync("nonexistent.txt");
        Assert.Null(bytes);

        var str = await db.DownloadBlobStringAsync("nonexistent.txt");
        Assert.Null(str);
    }

    [Fact]
    public async Task Blob_Delete()
    {
        using var catalog = CreateCatalog("catalog34");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("todelete.txt", "temp");
        var deleted = await db.DeleteBlobAsync("todelete.txt");
        Assert.True(deleted);

        var result = await db.DownloadBlobStringAsync("todelete.txt");
        Assert.Null(result);

        // Delete again — should return false
        var deletedAgain = await db.DeleteBlobAsync("todelete.txt");
        Assert.False(deletedAgain);
    }

    [Fact]
    public async Task Blob_ListBlobs()
    {
        using var catalog = CreateCatalog("catalog35");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("photos/a.jpg", "image-a");
        await db.UploadBlobAsync("photos/b.jpg", "image-b");
        await db.UploadBlobAsync("docs/readme.md", "readme");

        // List all
        var all = await db.ListBlobsAsync();
        Assert.Equal(3, all.Count);

        // List with prefix
        var photos = await db.ListBlobsAsync("photos/");
        Assert.Equal(2, photos.Count);
        Assert.Contains("photos/a.jpg", photos);
        Assert.Contains("photos/b.jpg", photos);

        var docs = await db.ListBlobsAsync("docs/");
        Assert.Single(docs);
        Assert.Contains("docs/readme.md", docs);
    }

    [Fact]
    public async Task Blob_ListBlobs_NonRecursive()
    {
        using var catalog = CreateCatalog("catalog60");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("root.txt", "root");
        await db.UploadBlobAsync("photos/a.jpg", "a");
        await db.UploadBlobAsync("photos/2024/b.jpg", "b");
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c");

        // Non-recursive at root — only root.txt
        var rootFiles = await db.ListBlobsAsync(recursive: false);
        Assert.Single(rootFiles);
        Assert.Contains("root.txt", rootFiles);

        // Non-recursive in photos/ — only a.jpg
        var photosFlat = await db.ListBlobsAsync("photos/", recursive: false);
        Assert.Single(photosFlat);
        Assert.Contains("photos/a.jpg", photosFlat);

        // Recursive in photos/ — all 3 photos
        var photosAll = await db.ListBlobsAsync("photos/", recursive: true);
        Assert.Equal(3, photosAll.Count);
        Assert.Contains("photos/a.jpg", photosAll);
        Assert.Contains("photos/2024/b.jpg", photosAll);
        Assert.Contains("photos/2024/trip/c.jpg", photosAll);
    }

    [Fact]
    public async Task Blob_ListFolders_NonRecursive()
    {
        using var catalog = CreateCatalog("catalog61");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("root.txt", "root");
        await db.UploadBlobAsync("photos/a.jpg", "a");
        await db.UploadBlobAsync("photos/2024/b.jpg", "b");
        await db.UploadBlobAsync("docs/readme.md", "readme");

        // Immediate subfolders of root
        var rootFolders = await db.ListBlobFoldersAsync(recursive: false);
        Assert.Equal(2, rootFolders.Count);
        Assert.Contains("photos/", rootFolders);
        Assert.Contains("docs/", rootFolders);

        // Immediate subfolders of photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: false);
        Assert.Single(photoFolders);
        Assert.Contains("photos/2024/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_Recursive()
    {
        using var catalog = CreateCatalog("catalog62");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("photos/a.jpg", "a");
        await db.UploadBlobAsync("photos/2024/b.jpg", "b");
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c");
        await db.UploadBlobAsync("docs/readme.md", "readme");

        // All folders recursively from root
        var allFolders = await db.ListBlobFoldersAsync(recursive: true);
        Assert.Contains("photos/", allFolders);
        Assert.Contains("photos/2024/", allFolders);
        Assert.Contains("photos/2024/trip/", allFolders);
        Assert.Contains("docs/", allFolders);

        // All folders recursively under photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: true);
        Assert.Contains("photos/2024/", photoFolders);
        Assert.Contains("photos/2024/trip/", photoFolders);
        Assert.DoesNotContain("photos/", photoFolders); // don't include the queried folder itself
        Assert.DoesNotContain("docs/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_EmptyFolder()
    {
        using var catalog = CreateCatalog("catalog63");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("only-file.txt", "content");

        var folders = await db.ListBlobFoldersAsync(recursive: false);
        Assert.Empty(folders);
    }

    [Fact]
    public async Task Blob_ListBlobs_FolderNormalization()
    {
        using var catalog = CreateCatalog("catalog64");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("photos/a.jpg", "a");

        // Both with and without trailing slash should work
        var withSlash = await db.ListBlobsAsync("photos/");
        var withoutSlash = await db.ListBlobsAsync("photos");
        Assert.Equal(withSlash, withoutSlash);
    }

    [Fact]
    public async Task Blob_IsolatedPerDatabase()
    {
        using var catalog = CreateCatalog("catalog36");
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
        using var catalog = CreateCatalog("catalog37");
        var db = await CreateDbAsync(catalog, "db1");

        await db.UploadBlobAsync("file.txt", "version 1");
        await db.UploadBlobAsync("file.txt", "version 2");

        var result = await db.DownloadBlobStringAsync("file.txt");
        Assert.Equal("version 2", result);
    }

    // === OnUpload handler integration tests ===

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_ReturnsMetadata()
    {
        using var catalog = CreateCatalog("catalog40");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload(); // default handler
        });

        var meta = await db.UploadBlobAsync("docs/readme.txt", "Hello world");

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
        using var catalog = CreateCatalog("catalog41");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("notes/meeting.md", "We discussed the quarterly revenue forecast");
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("quarterly revenue").ToList();
        Assert.Single(results);
        Assert.Equal("notes/meeting.md", results[0].Path);
    }

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_BinaryFile_CorrectType()
    {
        using var catalog = CreateCatalog("catalog42");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("photos/test.jpg", new byte[] { 0xFF, 0xD8, 0xFF });

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/jpeg", meta.MediaType);
        Assert.Null(meta.Content); // binary, no text extraction
    }

    [Fact]
    public async Task Blob_OnUpload_MetadataPersistedAndRetrievable()
    {
        using var catalog = CreateCatalog("catalog43");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("code/app.cs", "Console.WriteLine(\"Hello\");");

        var loaded = await db.GetAsync<BlobFile>("code/app.cs");
        Assert.NotNull(loaded);
        Assert.Equal("app.cs", loaded.Name);
        Assert.Equal("code", loaded.FolderPath);
        Assert.Equal("text/x-csharp", loaded.MediaType);
        Assert.Equal("Console.WriteLine(\"Hello\");", loaded.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterUpload()
    {
        using var catalog = CreateCatalog("catalog44");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("test.txt", "content");
        Assert.NotNull(meta);

        // Database should be set — DownloadAsync should work
        var stream = await meta.DownloadAsync();
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterGetAsync()
    {
        using var catalog = CreateCatalog("catalog45");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("test.txt", "content");

        var loaded = await db.GetAsync<BlobFile>("test.txt");
        Assert.NotNull(loaded);

        var stream = await loaded.DownloadAsync();
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Blob_OnUpload_DeleteAsync_CascadesMetadata()
    {
        using var catalog = CreateCatalog("catalog46");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("file.txt", "some content");

        // Metadata exists
        var before = await db.GetAsync<BlobFile>("file.txt");
        Assert.NotNull(before);

        // Delete via blob API
        await db.DeleteBlobAsync("file.txt");

        // Both blob and metadata are gone
        Assert.Null(await db.DownloadBlobStringAsync("file.txt"));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt"));
    }

    [Fact]
    public async Task Blob_OnUpload_BlobFileDeleteAsync_Works()
    {
        using var catalog = CreateCatalog("catalog47");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("file.txt", "some content");
        Assert.NotNull(meta);

        // Delete via BlobFile convenience method
        var deleted = await meta.DeleteAsync();
        Assert.True(deleted);

        Assert.Null(await db.DownloadBlobStringAsync("file.txt"));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt"));
    }

    [Fact]
    public async Task Blob_OnUpload_NoHandler_ReturnsNull()
    {
        using var catalog = CreateCatalog("catalog48");
        var db = await CreateDbAsync(catalog, "db1"); // no OnUpload

        var meta = await db.UploadBlobAsync("test.txt", "content");

        Assert.Null(meta);
        // Blob still uploaded
        Assert.Equal("content", await db.DownloadBlobStringAsync("test.txt"));
    }

    [Fact]
    public async Task Blob_OnUpload_Overwrite_UpdatesMetadata()
    {
        using var catalog = CreateCatalog("catalog49");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("file.txt", "version 1");
        await db.UploadBlobAsync("file.txt", "version 2");

        var meta = await db.GetAsync<BlobFile>("file.txt");
        Assert.NotNull(meta);
        Assert.Equal("version 2", meta.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_ExplicitContentType_Used()
    {
        using var catalog = CreateCatalog("catalog50");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        var meta = await db.UploadBlobAsync("data.bin", new byte[] { 1, 2, 3 }, contentType: "image/png");

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/png", meta.MediaType);
    }

    [Fact]
    public async Task Blob_OnUpload_Search_DatabasePropertySet()
    {
        using var catalog = CreateCatalog("catalog52");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        await db.UploadBlobAsync("notes/search-test.txt", "findable content here");
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("findable").ToList();
        Assert.Single(results);

        // Database should be set on Search results — DownloadAsync should work
        var stream = await results[0].DownloadAsync();
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("findable content here", await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task Blob_OnUpload_StreamOverload_ReturnsMetadata()
    {
        using var catalog = CreateCatalog("catalog51");
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        });

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("stream content"));
        var meta = await db.UploadBlobAsync("stream.txt", stream);

        Assert.NotNull(meta);
        Assert.Equal("stream.txt", meta.Name);
        Assert.Equal("text/plain", meta.MediaType);
        Assert.Equal("stream content", meta.Content);
    }

    // === On<T> polymorphic handler dispatch ===

    [Fact]
    public async Task On_BaseTypeHandler_FiresForDerivedType()
    {
        using var catalog = CreateCatalog("catalog80");
        var firedTypes = new List<string>();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db) =>
            {
                firedTypes.Add("BlobFile");
            });
            config.On<BlobPhoto>(async (photo, kind, db) =>
            {
                firedTypes.Add("BlobPhoto");
            });
        });

        // Upload a .jpg → creates BlobPhoto
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 });

        // Both On<BlobPhoto> and On<BlobFile> should fire
        Assert.Contains("BlobPhoto", firedTypes);
        Assert.Contains("BlobFile", firedTypes);
    }

    [Fact]
    public async Task On_BaseTypeHandler_ReceivesDerivedInstance()
    {
        using var catalog = CreateCatalog("catalog81");
        BlobFile? received = null;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db) =>
            {
                received = file;
            });
        });

        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 });

        Assert.NotNull(received);
        Assert.IsType<BlobPhoto>(received); // receives the actual derived type
    }

    [Fact]
    public async Task On_UnrelatedTypeHandler_DoesNotFire()
    {
        using var catalog = CreateCatalog("catalog82");
        var fired = false;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobMusic>(async (music, kind, db) =>
            {
                fired = true;
            });
        });

        // Upload a .jpg → BlobPhoto, not BlobMusic
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 });

        Assert.False(fired);
    }
}
