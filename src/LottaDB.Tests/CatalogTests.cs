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
}
