namespace Lotta.Tests;

public class ChangeAsyncTests : IClassFixture<LottaDBFixture>
{

    [Fact]
    public async Task ChangeAsync_MutatesAndSaves()
    {
        var db = LottaDBFixture.CreateDb();

        var actor = new Actor { Domain = "change.test", Username = "mutate", DisplayName = "Before" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        await db.ChangeAsync<Actor>("mutate", a =>
        {
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<Actor>("mutate", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("After", loaded.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ByObject_ExtractsKeys()
    {
        var db = LottaDBFixture.CreateDb();

        var actor = new Actor { Domain = "change.test", Username = "by-obj", DisplayName = "Before" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        // Get the object so it has tracked ETag
        var loaded = await db.GetAsync<Actor>("by-obj", TestContext.Current.CancellationToken);

        await db.ChangeAsync<Actor>(loaded!.Username, a =>
        {
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        var updated = await db.GetAsync<Actor>("by-obj", TestContext.Current.CancellationToken);
        Assert.Equal("After", updated!.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ReturnsObjectResult()
    {
        var db = LottaDBFixture.CreateDb();

        var actor = new Actor { Domain = "change.test", Username = "result", DisplayName = "Before" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        var result = await db.ChangeAsync<Actor>("result", a =>
        {
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        Assert.NotEmpty(result.Changes);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Saved);
    }

    [Fact]
    public async Task ChangeAsync_NonExistent_Throws()
    {
        var db = LottaDBFixture.CreateDb();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.ChangeAsync<Actor>("ghost", a =>
            {
                a.DisplayName = "impossible";
                return a;
            }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangeAsync_MutationIsPure_CalledAtLeastOnce()
    {
        var db = LottaDBFixture.CreateDb();
        var actor = new Actor { Domain = "change.test", Username = "pure", DisplayName = "Before" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        int callCount = 0;
        await db.ChangeAsync<Actor>("pure", a =>
        {
            Interlocked.Increment(ref callCount);
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        Assert.True(callCount >= 1);
    }

    /// <summary>
    /// ChangeAsync must be ETag-sensitive. Scenario:
    ///   1. Write V1.
    ///   2. Start a ChangeAsync that reads V1 but blocks inside the mutator before committing.
    ///   3. While blocked, an outer SaveAsync writes V2 (different field than the mutation).
    ///   4. Unblock the mutator. The first commit attempt must fail (ETag mismatch with V1),
    ///      ChangeAsync must re-read (now seeing V2), re-invoke the mutator, and commit V3 —
    ///      i.e. the mutation merged on top of V2, not V1.
    /// </summary>
    [Fact]
    public async Task ChangeAsync_RetriesOnEtagConflict_MergesWithConcurrentWrite()
    {
        var db = LottaDBFixture.CreateDb();
        var ct = TestContext.Current.CancellationToken;

        // V1
        var v1 = new Actor
        {
            Domain = "change.test",
            Username = "concurrent-mutate",
            DisplayName = "V1",
            AvatarUrl = "v1.png"
        };
        await db.SaveAsync(v1, ct);

        var firstReadDone = new TaskCompletionSource();
        var allowApply = new TaskCompletionSource();
        int mutateCalls = 0;

        // Task A: inside the mutator, on the first invocation, signal we've read V1 then
        // block until the outer test has written V2. The second invocation (after the
        // ETag conflict forces a re-read) should apply immediately without blocking.
        var changeTask = Task.Run(async () =>
        {
            return await db.ChangeAsync<Actor>("concurrent-mutate", actor =>
            {
                var call = Interlocked.Increment(ref mutateCalls);
                if (call == 1)
                {
                    firstReadDone.TrySetResult();
                    allowApply.Task.Wait();
                }
                actor.DisplayName = "V3";
                return actor;
            }, ct);
        }, ct);

        // Wait until ChangeAsync has read V1 and entered the mutator.
        await firstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        // Outer write: V2 — changes AvatarUrl (an unrelated field) and bumps DisplayName.
        // This bumps the ETag so the in-flight ChangeAsync's V1-based write will 412.
        var loaded = await db.GetAsync<Actor>("concurrent-mutate", ct);
        Assert.NotNull(loaded);
        loaded!.DisplayName = "V2";
        loaded.AvatarUrl = "v2.png";
        await db.SaveAsync(loaded, ct);

        // Let the blocked mutator proceed. Its commit must fail and retry.
        allowApply.TrySetResult();

        var result = await changeTask;
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Saved);

        // Mutator must have been called at least twice: once on the stale V1 read,
        // and again on the retry after the ETag conflict re-read V2.
        Assert.True(mutateCalls >= 2,
            $"expected mutator to be called at least twice due to ETag conflict retry; got {mutateCalls}");

        // Final state: V3 DisplayName merged over V2's AvatarUrl (not V1's).
        var final = await db.GetAsync<Actor>("concurrent-mutate", ct);
        Assert.NotNull(final);
        Assert.Equal("V3", final!.DisplayName);
        Assert.Equal("v2.png", final.AvatarUrl);
    }

    /// <summary>
    /// When ChangeAsync retries after an ETag conflict, the On&lt;T&gt; handler must fire
    /// for the final committed value only — never for the intermediate mutation that lost
    /// the conflict. If a discarded attempt triggers handlers, downstream side effects
    /// (index writes, notifications) get duplicated or applied to phantom state.
    /// </summary>
    [Fact]
    public async Task ChangeAsync_Retry_FiresOnceForCommittedValue()
    {
        var db = LottaDBFixture.CreateDb();
        var ct = TestContext.Current.CancellationToken;
        var username = "handler-once";

        await db.SaveAsync(new Actor
        {
            Domain = "change.test",
            Username = username,
            DisplayName = "V1",
            AvatarUrl = "v1.png"
        }, ct);

        var handlerCalls = new List<string>();
        var handlerLock = new object();
        using var handle = db.On<Actor>((a, _, _) =>
        {
            if (a.Username == username)
                lock (handlerLock) handlerCalls.Add(a.DisplayName);
            return Task.CompletedTask;
        });

        var firstReadDone = new TaskCompletionSource();
        var allowApply = new TaskCompletionSource();

        var changeTask = Task.Run(async () =>
        {
            return await db.ChangeAsync<Actor>(username, actor =>
            {
                if (firstReadDone.TrySetResult())
                    allowApply.Task.Wait();
                actor.DisplayName = "V3";
                return actor;
            }, ct);
        }, ct);

        await firstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        var loaded = await db.GetAsync<Actor>(username, ct);
        loaded!.DisplayName = "V2";
        loaded.AvatarUrl = "v2.png";
        await db.SaveAsync(loaded, ct);

        allowApply.TrySetResult();
        await changeTask;

        // Two commits landed: the outer SaveAsync (V2) and the ChangeAsync's final commit (V3).
        // The V1-based mutation that lost the ETag race must NOT have fired the handler.
        List<string> snapshot;
        lock (handlerLock) snapshot = handlerCalls.ToList();

        Assert.Equal(2, snapshot.Count);
        Assert.Contains("V2", snapshot);
        Assert.Contains("V3", snapshot);
        // Sanity: no phantom value from a discarded attempt. Mutator only ever writes "V3",
        // so a duplicate "V3" would mean the discarded attempt's handler fired.
        Assert.Single(snapshot, s => s == "V3");
    }

    /// <summary>
    /// N parallel ChangeAsync calls on the same row must all land — none may be lost to
    /// an ETag conflict. Each increments a counter field; the final value must equal N.
    /// </summary>
    [Fact]
    public async Task ChangeAsync_ParallelWriters_NoLostUpdates()
    {
        var db = LottaDBFixture.CreateDb();
        var ct = TestContext.Current.CancellationToken;
        var username = "parallel-counter";
        const int N = 10;

        await db.SaveAsync(new Actor
        {
            Domain = "change.test",
            Username = username,
            Counter = 0
        }, ct);

        var tasks = Enumerable.Range(0, N).Select(_ => Task.Run(() =>
            db.ChangeAsync<Actor>(username, a =>
            {
                a.Counter++;
                return a;
            }, ct), ct)).ToArray();

        await Task.WhenAll(tasks);

        var final = await db.GetAsync<Actor>(username, ct);
        Assert.Equal(N, final!.Counter);
        var final2 = db.Search<Actor>(a => a.Username == final.Username).Single();
        Assert.Equal(N, final2!.Counter);
    }

    /// <summary>
    /// After a retry, Lucene must reflect the final committed value — not the discarded
    /// intermediate mutation. If the Lucene session were written before the conditional
    /// commit landed, the index would diverge from table storage.
    /// </summary>
    [Fact]
    public async Task ChangeAsync_RetryAfterConflict_LuceneReflectsFinalCommit()
    {
        var db = LottaDBFixture.CreateDb();
        var ct = TestContext.Current.CancellationToken;
        var username = "lucene-final";

        await db.SaveAsync(new Actor
        {
            Domain = "change.test",
            Username = username,
            DisplayName = "V1",
            AvatarUrl = "v1.png"
        }, ct);

        var firstReadDone = new TaskCompletionSource();
        var allowApply = new TaskCompletionSource();

        var changeTask = Task.Run(async () =>
        {
            return await db.ChangeAsync<Actor>(username, actor =>
            {
                if (firstReadDone.TrySetResult())
                    allowApply.Task.Wait();
                actor.DisplayName = "V3-lucene";
                return actor;
            }, ct);
        }, ct);

        await firstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        var loaded = await db.GetAsync<Actor>(username, ct);
        loaded!.DisplayName = "V2-lucene";
        loaded.AvatarUrl = "v2.png";
        await db.SaveAsync(loaded, ct);

        allowApply.TrySetResult();
        await changeTask;

        // Lucene Search returns deserialized POCOs — check both the indexed field (DisplayName)
        // and a non-indexed field (AvatarUrl, round-trips via _json) reflect the merged state.
        var indexed = db.Search<Actor>().ToList().Single(a => a.Username == username);
        Assert.Equal("V3-lucene", indexed.DisplayName);
        Assert.Equal("v2.png", indexed.AvatarUrl);

        // And no stale "V1"-based variant lingers in the index.
        var stale = db.Search<Actor>().ToList().Where(a => a.Username == username && a.AvatarUrl == "v1.png").ToList();
        Assert.Empty(stale);
    }

    /// <summary>
    /// The mutator's input on each invocation must be a fresh read from storage. On retry
    /// after an ETag conflict, the mutator must see the newly-committed state (V2), not
    /// the stale snapshot it was handed the first time (V1).
    /// </summary>
    [Fact]
    public async Task ChangeAsync_RetryAfterConflict_MutatorSeesFreshInputOnRetry()
    {
        var db = LottaDBFixture.CreateDb();
        var ct = TestContext.Current.CancellationToken;
        var username = "fresh-input";

        await db.SaveAsync(new Actor
        {
            Domain = "change.test",
            Username = username,
            DisplayName = "V1",
            AvatarUrl = "v1.png"
        }, ct);

        var firstReadDone = new TaskCompletionSource();
        var allowApply = new TaskCompletionSource();
        var seenInputs = new List<(string DisplayName, string AvatarUrl)>();

        var changeTask = Task.Run(async () =>
        {
            return await db.ChangeAsync<Actor>(username, actor =>
            {
                seenInputs.Add((actor.DisplayName, actor.AvatarUrl));
                if (firstReadDone.TrySetResult())
                    allowApply.Task.Wait();
                actor.DisplayName = "V3-fresh";
                return actor;
            }, ct);
        }, ct);

        await firstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

        var loaded = await db.GetAsync<Actor>(username, ct);
        loaded!.DisplayName = "V2-fresh";
        loaded.AvatarUrl = "v2.png";
        await db.SaveAsync(loaded, ct);

        allowApply.TrySetResult();
        await changeTask;

        Assert.Equal(2, seenInputs.Count);
        // First invocation reads the pre-conflict V1 snapshot.
        Assert.Equal(("V1", "v1.png"), seenInputs[0]);
        // Second invocation must see the V2 state that beat us to the commit.
        Assert.Equal(("V2-fresh", "v2.png"), seenInputs[1]);
    }
}
