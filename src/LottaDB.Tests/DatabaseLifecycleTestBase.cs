using System.Runtime.CompilerServices;
using Azure.Data.Tables;
using Azure.Storage.Blobs;

namespace Lotta.Tests;

public abstract class DatabaseLifecycleTestBase : LottaTestBase
{
    protected DatabaseLifecycleTestBase(string provider) : base(provider) { }
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
    public async Task DeleteDatabase_CleansUpLuceneDirectory()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "cleanup", ct);
        await db.ResetDatabaseAsync(ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        // Get the Lucene directory path before delete
        var directory = db.GetLuceneDirectory();
        string? dirPath = null;
        if (directory is Lucene.Net.Store.FSDirectory fsDir)
            dirPath = fsDir.Directory.FullName;

        await db.DeleteDatabaseAsync(ct);

        // FSDirectory folder should be deleted
        if (dirPath != null)
            Assert.False(System.IO.Directory.Exists(dirPath), $"Lucene directory should be deleted: {dirPath}");
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

        // Shared storage factories so catalog2 sees catalog1's data (simulates process restart)
        Func<TableServiceClient>? sharedTableFactory = null;
        Func<BlobServiceClient>? sharedBlobFactory = null;

        // First run: create database with Actor only
        var catalog1 = CreateCatalog(catalog =>
        {
            sharedTableFactory = catalog.TableServiceClientFactory;
            sharedBlobFactory = catalog.BlobServiceClientFactory;
        });
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

        // Second run: same backing storage, but add Note to schema
        var catalog2 = CreateCatalog(catalog =>
        {
            catalog.TableServiceClientFactory = sharedTableFactory!;
            catalog.BlobServiceClientFactory = sharedBlobFactory!;
        });
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

        // Shared storage factories so catalog2 sees catalog1's data (simulates process restart)
        Func<TableServiceClient>? sharedTableFactory = null;
        Func<BlobServiceClient>? sharedBlobFactory = null;

        // First run
        var catalog1 = CreateCatalog(catalog =>
        {
            sharedTableFactory = catalog.TableServiceClientFactory;
            sharedBlobFactory = catalog.BlobServiceClientFactory;
        });
        var db1 = await catalog1.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        }, ct);
        await db1.ResetDatabaseAsync(ct);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        db1.ReloadSearcher();
        catalog1.Dispose();

        // Second run: same backing storage, simulates process restart
        var catalog2 = CreateCatalog(catalog =>
        {
            catalog.TableServiceClientFactory = sharedTableFactory!;
            catalog.BlobServiceClientFactory = sharedBlobFactory!;
        });
        var db2 = await catalog2.GetDatabaseAsync("mydb", config =>
        {
            config.Store<Actor>();
        }, ct);
        // Schema matches so no rebuild triggered — verifies no error occurs
        db2.ReloadSearcher();
        Assert.NotNull(db2);
        catalog2.Dispose();
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
}

