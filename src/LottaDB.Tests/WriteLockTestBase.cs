namespace Lotta.Tests;

/// <summary>
/// Verifies the lazy-acquire / idle-release lifecycle of the cross-process Lucene write lock.
///
/// <para>
/// The write lock is what prevents two processes from opening the same database. It is now
/// taken on the first write rather than at open, and released after
/// <see cref="ILottaConfiguration.WriteLockReleaseDelay"/> of write inactivity, so read-only
/// servers never contend for it and the writer role can migrate between servers.
/// </para>
///
/// <para>
/// These tests cover the state machine on every provider. Genuine cross-process contention
/// needs a real <c>FSDirectory</c> lock and lives in <see cref="MultiProcessLockTests"/> —
/// the Memory provider's <c>RAMDirectory</c> is in-process only and cannot exercise it.
/// </para>
/// </summary>
public abstract class WriteLockTestBase : LottaTestBase
{
    protected WriteLockTestBase(Action<LottaCatalog> config) : base(config) { }

    /// <summary>Poll until <paramref name="condition"/> holds, or fail after <paramref name="timeoutMs"/>.</summary>
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

    [Fact]
    public async Task GetDatabaseAsync_WithoutWriting_DoesNotHoldWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct, reset: false);

        Assert.False(db.HoldsWriteLock);
    }

    /// <summary>
    /// Regression guard for the Lucene.Net.Linq fallback: <c>Context.CreateSearcher</c> catches
    /// IndexNotFoundException by creating a temporary IndexWriter — taking the write lock from
    /// inside a read path. The constructor bootstraps an empty commit so that never fires.
    /// </summary>
    [Fact]
    public async Task Search_OnBrandNewDatabase_DoesNotAcquireWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct, reset: false);

        Assert.Empty(db.Search<Actor>().ToList());
        Assert.Empty(db.Search<object>().ToList());

        Assert.False(db.HoldsWriteLock);
    }

    [Fact]
    public async Task SaveAsync_AcquiresWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = -1, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        Assert.True(db.HoldsWriteLock);
    }

    [Fact]
    public async Task Idle_AfterWriteLockReleaseDelay_ReleasesWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = 100, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        await WaitUntil(() => !db.HoldsWriteLock, "the idle timer to release the write lock");
    }

    [Fact]
    public async Task SaveAsync_AfterAutoRelease_ReacquiresAndIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = 100, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await WaitUntil(() => !db.HoldsWriteLock, "the write lock to be released");

        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);
        Assert.True(db.HoldsWriteLock);

        db.ReloadSearcher();
        var found = db.Search<Actor>().ToList();
        Assert.Equal(2, found.Count);
    }

    [Fact]
    public async Task ReleaseWriteLockAsync_CommitsPendingWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        // Huge auto-commit delay so the debounce never fires — the release must do the commit.
        var db = await CreateDbAsync(config =>
        {
            config.AutoCommitDelay = 60000;
            config.WriteLockReleaseDelay = -1;
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.ReleaseWriteLockAsync(ct);

        Assert.False(db.HoldsWriteLock);
        Assert.Single(db.Search<Actor>().ToList());
    }

    /// <summary>
    /// ReleaseWriteLockAsync drains with Timeout.InfiniteTimeSpan, which is -1ms. A naive
    /// "elapsed > timeout" check treats that as already expired, abandons the release on the
    /// first poll and reports success while the lock is still held.
    /// </summary>
    [Fact]
    public async Task ReleaseWriteLockAsync_WithWriteInFlight_StillReleases()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = -1, cancellationToken: ct);

        // Keep a write in flight so the drain loop actually runs at least one iteration.
        var gate = new TaskCompletionSource();
        using var handler = db.On<Note>(async (note, kind, database, token) =>
        {
            await gate.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
        });

        Task slowWrite = Task.CompletedTask;
        Task release = Task.CompletedTask;
        try
        {
            slowWrite = db.SaveAsync(new Note { NoteId = "n1", Content = "hi", AuthorId = "alice" }, ct);
            await WaitUntil(() => db.HoldsWriteLock, "the slow write to take the write lock");

            release = db.ReleaseWriteLockAsync(ct);
            await Task.Delay(150, ct);

            Assert.False(release.IsCompleted, "release must wait for the in-flight write to drain");
        }
        finally
        {
            // Always unblock the handler. Leaving it parked on an assertion failure would strand
            // the unbounded drain inside ReleaseWriteLockAsync, which holds the writer gate —
            // hanging the whole test run rather than failing this one test.
            gate.TrySetResult();
        }

        // Bounded so a regression surfaces as a fast, legible failure instead of a hung run.
        await slowWrite.WaitAsync(TimeSpan.FromSeconds(30), ct);
        await release.WaitAsync(TimeSpan.FromSeconds(30), ct);

        Assert.False(db.HoldsWriteLock);
    }

    [Fact]
    public async Task ReleaseWriteLockAsync_WhenNotHeld_IsNoOp()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct, reset: false);

        await db.ReleaseWriteLockAsync(ct);
        await db.ReleaseWriteLockAsync(ct);   // idempotent

        Assert.False(db.HoldsWriteLock);
    }

    [Fact]
    public async Task ReleaseWriteLockAsync_ThenDispose_DoesNotThrow()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "releasedispose", ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.ReleaseWriteLockAsync(ct);

        db.Dispose();   // must tolerate an already-released writer
    }

    [Fact]
    public async Task ReadOnly_SaveAsync_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        // Create and populate the database first, so the read-only open finds a real index.
        var db = await CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.ReleaseWriteLockAsync(ct);

        var readOnly = await Catalog.GetDatabaseAsync("readonlysave", config =>
        {
            config.Store<Actor>();
            config.ReadOnly = true;
        }, ct);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => readOnly.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct));

        Assert.False(readOnly.HoldsWriteLock);
    }

    [Fact]
    public async Task ReadOnly_Search_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        var readOnly = await Catalog.GetDatabaseAsync("readonlysearch", config =>
        {
            config.Store<Actor>();
            config.ReadOnly = true;
        }, ct);

        Assert.Empty(readOnly.Search<Actor>().ToList());
        Assert.False(readOnly.HoldsWriteLock);
    }

    /// <summary>
    /// Maintenance operations take the write lock like any other write, so they must also give
    /// it back. A server that rebuilds its index and then only serves reads would otherwise hold
    /// the cross-process lock forever, locking every other server out of writing.
    /// </summary>
    [Fact]
    public async Task RebuildSearchIndex_ThenIdle_ReleasesWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = 100, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await WaitUntil(() => !db.HoldsWriteLock, "the initial save's lock to be released");

        await db.RebuildSearchIndex(ct);

        await WaitUntil(() => !db.HoldsWriteLock, "RebuildSearchIndex to release the write lock");
    }

    [Fact]
    public async Task ResetDatabaseAsync_ThenIdle_ReleasesWriteLock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = 100, cancellationToken: ct);

        await db.ResetDatabaseAsync(ct);

        await WaitUntil(() => !db.HoldsWriteLock, "ResetDatabaseAsync to release the write lock");
    }

    /// <summary>
    /// Many concurrent writes starting from a released state all funnel through the writer gate.
    /// Exercises the acquisition thundering herd and the in-flight lease drain.
    /// </summary>
    [Fact]
    public async Task ConcurrentSaves_AfterAutoRelease_AllIndexed()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config => config.WriteLockReleaseDelay = 50, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "seed", DisplayName = "Seed" }, ct);
        await WaitUntil(() => !db.HoldsWriteLock, "the write lock to be released");

        await Task.WhenAll(Enumerable.Range(0, 50).Select(i =>
            db.SaveAsync(new Actor { Username = $"user{i}", DisplayName = $"User {i}" }, ct)));

        db.ReloadSearcher();
        Assert.Equal(51, db.Search<Actor>().ToList().Count);
    }

    /// <summary>
    /// An On&lt;T&gt; handler that writes re-enters the write path while the outer lease is held.
    /// With a 1ms release delay the idle timer is racing the handler, so this also covers the
    /// "release must not run while a write is in flight" drain.
    /// </summary>
    [Fact]
    public async Task NestedHandlerSave_WithAggressiveRelease_DoesNotDeadlock()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(config =>
        {
            config.WriteLockReleaseDelay = 1;
            config.On<Note>(async (note, kind, database, token) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await database.SaveAsync(new NoteView
                {
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    Content = note.Content,
                }, token);
            });
        }, cancellationToken: ct);

        for (int i = 0; i < 10; i++)
        {
            await db.SaveAsync(new Note { NoteId = $"n{i}", Content = $"note {i}", AuthorId = "alice" }, ct);
        }

        db.ReloadSearcher();
        Assert.Equal(10, db.Search<NoteView>().ToList().Count);
    }
}
