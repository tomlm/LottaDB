using System.Runtime.CompilerServices;
using Azure.Data.Tables;
using Azure.Storage.Blobs;

namespace Lotta.Tests;

/// <summary>
/// Genuine cross-process write-lock contention.
///
/// <para>
/// Two independent <see cref="LottaCatalog"/> instances stand in for two server processes:
/// they share in-memory table/blob storage but point at the <b>same FSDirectory on disk</b>.
/// <c>NativeFSLockFactory</c> refuses a second acquire even within one process — it tracks
/// held paths in a static set and opens <c>write.lock</c> with <c>FileShare.None</c> — so
/// this is real contention, not a simulation.
/// </para>
///
/// <para>
/// This cannot live in a provider <c>TestBase</c>: the Memory provider's RAMDirectory uses
/// SingleInstanceLockFactory with a distinct instance per catalog, so it has no cross-catalog
/// lock at all. These tests therefore run once, in the shared test project.
/// </para>
///
/// <para>
/// Before the lazy write lock, every test here failed at <c>GetDatabaseAsync</c> with
/// <c>LockObtainFailedException</c>, because the IndexWriter was created eagerly in the
/// LottaDB constructor.
/// </para>
/// </summary>
public class MultiProcessLockTests
{
    /// <summary>
    /// When set, each catalog stores its objects in the Lucene index itself (<c>UseLucene()</c>)
    /// instead of the shared in-memory tables — so the shared on-disk directory is the only storage
    /// the two "processes" have in common, as it would be for two real servers.
    /// </summary>
    protected virtual bool IndexIsStorage => false;

    /// <summary>
    /// Build two catalogs over one shared backing store: shared in-memory tables and blobs,
    /// and a shared on-disk Lucene directory that carries a real file lock.
    /// </summary>
    protected (LottaCatalog first, LottaCatalog second) CreateSharedCatalogs(
        [CallerMemberName] string? testName = null)
    {
        var root = Path.Combine(TestRun.GetTempFolder(), "MultiProcessLock" + (IndexIsStorage ? "Lucene" : ""),
            string.Join("", testName!.Where(char.IsLetterOrDigit)));
        System.IO.Directory.CreateDirectory(root);

        var sharedLuceneDirectory = LuceneDirectories.FileSystem(root);

        if (IndexIsStorage)
        {
            // Each process has its own storage client; with UseLucene() they share only the files.
            return (new LottaCatalog("shared", catalog => catalog.UseLucene(sharedLuceneDirectory)),
                    new LottaCatalog("shared", catalog => catalog.UseLucene(sharedLuceneDirectory)));
        }

        Func<TableServiceClient>? tableFactory = null;
        Func<BlobServiceClient>? blobFactory = null;

        var first = new LottaCatalog("shared", catalog =>
        {
            catalog.UseMemory();
            tableFactory = catalog.TableServiceClientFactory;
            blobFactory = catalog.BlobServiceClientFactory;
            catalog.LuceneDirectoryFactory = sharedLuceneDirectory;
        });

        var second = new LottaCatalog("shared", catalog =>
        {
            catalog.TableServiceClientFactory = tableFactory!;
            catalog.BlobServiceClientFactory = blobFactory!;
            catalog.LuceneDirectoryFactory = sharedLuceneDirectory;
        });

        return (first, second);
    }

    private static Task<LottaDB> OpenAsync(LottaCatalog catalog, CancellationToken ct,
        Action<ILottaConfiguration>? extra = null)
        => catalog.GetDatabaseAsync("shared", config =>
        {
            config.Store<Actor>();
            extra?.Invoke(config);
        }, ct);

    private static async Task WaitUntil(Func<bool> condition, string because, int timeoutMs = 10000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(25);
        }
        Assert.Fail($"Timed out after {timeoutMs}ms waiting for: {because}");
    }

    /// <summary>The headline scenario: a second server opens a database the first is writing to.</summary>
    [Fact]
    public async Task SecondCatalog_OpensDatabase_WhileFirstHoldsWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = -1);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        Assert.True(db1.HoldsWriteLock);

        // Opening must not need the write lock.
        var db2 = await OpenAsync(c2, ct);

        Assert.False(db2.HoldsWriteLock);
    }

    [Fact]
    public async Task SecondCatalog_Search_SeesFirstCatalogsCommits()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = -1);
        var db2 = await OpenAsync(c2, ct, config => config.MaxSearchStaleness = 0);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        db1.ReloadSearcher();   // commit so the other process can see it

        var seen = db2.Search<Actor>().ToList();
        Assert.Single(seen);
        Assert.Equal("alice", seen[0].Username);
    }

    [Fact]
    public async Task SecondCatalog_Write_WhileFirstHoldsLock_ThrowsWriteLockUnavailable()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = -1);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        Assert.True(db1.HoldsWriteLock);

        var db2 = await OpenAsync(c2, ct, config =>
        {
            config.WriteLockReleaseDelay = -1;
            config.WriteLockTimeout = 0;   // fail fast rather than waiting a second
        });

        await Assert.ThrowsAsync<WriteLockUnavailableException>(
            () => db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct));
    }

    /// <summary>
    /// Proves the "acquire before mutating storage" ordering: a write that cannot get the lock
    /// must not have written the Table Storage row either.
    /// </summary>
    [Fact]
    public async Task BlockedWrite_LeavesNoPartialState()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = -1);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        var db2 = await OpenAsync(c2, ct, config =>
        {
            config.WriteLockReleaseDelay = -1;
            config.WriteLockTimeout = 0;
        });

        await Assert.ThrowsAsync<WriteLockUnavailableException>(
            () => db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct));

        // The table row must not exist — the write failed before touching storage.
        Assert.Null(await db1.GetAsync<Actor>("bob", ct));
    }

    /// <summary>The writer role migrates: once the first server goes idle, the second can write.</summary>
    [Fact]
    public async Task SecondCatalog_Write_AfterFirstReleases_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = 100);
        var db2 = await OpenAsync(c2, ct, config =>
        {
            config.WriteLockReleaseDelay = 100;
            config.MaxSearchStaleness = 0;
        });

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await WaitUntil(() => !db1.HoldsWriteLock, "the first catalog to release the write lock");

        await db2.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);
        db2.ReloadSearcher();
        await WaitUntil(() => !db2.HoldsWriteLock, "the second catalog to release the write lock");

        // And the role can migrate back.
        await db1.SaveAsync(new Actor { Username = "carol", DisplayName = "Carol" }, ct);
        db1.ReloadSearcher();

        Assert.Equal(3, db1.Search<Actor>().ToList().Count);
    }

    [Fact]
    public async Task ReadOnlyCatalog_Searches_WhileFirstHoldsWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = -1);
        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        db1.ReloadSearcher();
        Assert.True(db1.HoldsWriteLock);

        var reader = await OpenAsync(c2, ct, config =>
        {
            config.ReadOnly = true;
            config.MaxSearchStaleness = 0;
        });

        Assert.Single(reader.Search<Actor>().ToList());
        Assert.False(reader.HoldsWriteLock);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => reader.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct));
    }

    /// <summary>
    /// The writer role changes hands while both servers read and read-modify-write the same
    /// object. Each must see the other's committed changes once it takes the role — nothing it
    /// wrote or read while it last held the role may be served stale.
    /// </summary>
    [Fact]
    public async Task WriterHandover_ReadsAndChanges_SeeOtherProcessesWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var db1 = await OpenAsync(c1, ct, config => config.MaxSearchStaleness = 0);
        var db2 = await OpenAsync(c2, ct, config => config.MaxSearchStaleness = 0);

        await db1.SaveAsync(new Actor { Username = "alice", DisplayName = "v1", Counter = 1 }, ct);
        await db1.ReleaseWriteLockAsync(ct);

        Assert.Equal(1, (await db2.GetAsync<Actor>("alice", ct))!.Counter);
        await db2.ChangeAsync<Actor>("alice", a => a.Counter++, ct);
        await db2.ReleaseWriteLockAsync(ct);

        Assert.Equal(2, (await db1.GetAsync<Actor>("alice", ct))!.Counter);
        await db1.ChangeAsync<Actor>("alice", a => a.Counter++, ct);
        await db1.ReleaseWriteLockAsync(ct);

        var final = (await db2.GetAsync<Actor>("alice", ct))!;
        Assert.Equal(3, final.Counter);
        Assert.Equal(final.GetETag(), (await db1.GetAsync<Actor>("alice", ct))!.GetETag());
    }

    /// <summary>
    /// A server writing continuously never goes idle, so the idle-release path never fires and
    /// it would keep the writer role forever. <c>WriteLockMaxHoldTime</c> exists to force it to
    /// yield anyway — and the contract that matters is that another process actually gets a
    /// turn, not that a flag momentarily flips.
    /// </summary>
    [Fact]
    public async Task WriteLockMaxHoldTime_UnderContinuousWrites_LetsAnotherProcessWrite()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var busy = await OpenAsync(c1, ct, config =>
        {
            config.WriteLockReleaseDelay = -1;   // never yield on idle ...
            config.WriteLockMaxHoldTime = 200;   // ... but do yield after holding this long
        });

        var other = await OpenAsync(c2, ct, config =>
        {
            config.WriteLockReleaseDelay = -1;
            config.WriteLockTimeout = 20000;     // fails the test if the turn never comes
        });

        using var stop = new CancellationTokenSource();
        var keepWriting = Task.Run(async () =>
        {
            for (var i = 0; !stop.IsCancellationRequested; i++)
            {
                // Once the other process takes over, our writes fail — that is the point.
                try { await busy.SaveAsync(new Actor { Username = $"busy{i}", DisplayName = "busy" }, stop.Token); }
                catch (WriteLockUnavailableException) { }
                catch (OperationCanceledException) { break; }

                try { await Task.Delay(10, stop.Token); }
                catch (OperationCanceledException) { break; }
            }
        }, CancellationToken.None);

        try
        {
            // Throws WriteLockUnavailableException after WriteLockTimeout if max-hold never fires.
            await other.SaveAsync(new Actor { Username = "takeover", DisplayName = "Took over" }, ct);
        }
        finally
        {
            stop.Cancel();
            await keepWriting;
        }

        Assert.NotNull(await other.GetAsync<Actor>("takeover", ct));
    }

    /// <summary>
    /// A read replica that boots before the writer has ever created the index must not crash —
    /// losing the startup race is normal when N readers and a writer start together. It serves
    /// empty results and picks the index up as soon as it appears.
    /// </summary>
    [Fact]
    public async Task ReadOnlyCatalog_OpenedBeforeIndexExists_ServesEmptyThenCatchesUp()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        // The reader opens first — nothing has ever created this index.
        var reader = await OpenAsync(c2, ct, config =>
        {
            config.ReadOnly = true;
            config.MaxSearchStaleness = 0;
        });

        Assert.Empty(reader.Search<Actor>().ToList());
        Assert.False(reader.HoldsWriteLock);

        // Now the writer shows up.
        var writer = await OpenAsync(c1, ct, config => config.WriteLockReleaseDelay = -1);
        await writer.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        writer.ReloadSearcher();

        Assert.Single(reader.Search<Actor>().ToList());
    }

    /// <summary>
    /// Two servers booting simultaneously against a directory that has never been written to.
    /// Exactly one lays down the bootstrap commit; neither may fail.
    /// </summary>
    [Fact]
    public async Task TwoCatalogs_OpenBrandNewIndexConcurrently_BothSucceed()
    {
        var ct = TestContext.Current.CancellationToken;
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;

        var open1 = Task.Run(() => OpenAsync(c1, ct), ct);
        var open2 = Task.Run(() => OpenAsync(c2, ct), ct);

        var dbs = await Task.WhenAll(open1, open2);

        Assert.All(dbs, db => Assert.Empty(db.Search<Actor>().ToList()));
        Assert.All(dbs, db => Assert.False(db.HoldsWriteLock));
    }
}
