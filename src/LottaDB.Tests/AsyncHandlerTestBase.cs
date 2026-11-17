using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Queues;

namespace Lotta.Tests;

/// <summary>
/// Tests for OnAsync&lt;T&gt;: handlers that are queued by a save and run later, outside it.
/// These run the handlers by hand with <see cref="LottaDB.DrainAsync"/>, so every step is in order.
/// </summary>
public abstract class AsyncHandlerTestBase : LottaTestBase
{
    protected AsyncHandlerTestBase(Action<LottaCatalog> config) : base(config) { }

    /// <summary>Failing runs are tried again at once, so a drain sees every attempt.</summary>
    private static AsyncHandlerOptions Quick(int maxAttempts = 5) => new() { MaxAttempts = maxAttempts, RetryDelay = TimeSpan.Zero };

    private static async Task<AsyncHandlerStatus> StatusOf(LottaDB db, string name, CancellationToken ct) =>
        (await db.GetHandlerStatusAsync(ct)).Single(s => s.Name == name);

    // === Order ===

    [Fact]
    public async Task HigherPriority_DrainsFirst_AndEachHandlerIsFirstInFirstOut()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Note>("low", (note, _, _, _) => { ran.Add($"low:{note.NoteId}"); return Task.CompletedTask; }, priority: 1, latest: false);
            opts.OnAsync<Note>("high", (note, _, _, _) => { ran.Add($"high:{note.NoteId}"); return Task.CompletedTask; }, priority: 10, latest: false);
        }, cancellationToken: ct);

        foreach (var id in new[] { "n1", "n2", "n3" })
            await db.SaveAsync(new Note { NoteId = id, Content = id }, ct);
        Assert.Empty(ran);   // nothing runs inside the save

        Assert.Equal(6, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["high:n1", "high:n2", "high:n3", "low:n1", "low:n2", "low:n3"], ran);
    }

    [Fact]
    public async Task EqualPriority_TakesTheOldestWorkAcrossHandlers()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Note>("notes", (note, _, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; }, priority: 5, latest: false);
            opts.OnAsync<Actor>("actors", (actor, _, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; }, priority: 5, latest: false);
        }, cancellationToken: ct);

        // More than a second apart: Azure records when a message was queued to the second.
        await db.SaveAsync(new Note { NoteId = "n1" }, ct);
        await Task.Delay(1100, ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await Task.Delay(1100, ct);
        await db.SaveAsync(new Note { NoteId = "n2" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["n1", "alice", "n2"], ran);
    }

    // === Latest ===

    [Fact]
    public async Task Latest_SeveralSavesOfOneObject_GiveOneRun_OnTheObjectAsItIsThen()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<int>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (actor, _, _, _) => { seen.Add(actor.Counter); return Task.CompletedTask; }, latest: true);
        }, cancellationToken: ct);

        for (var n = 1; n <= 3; n++)
            await db.SaveAsync(new Actor { Username = "alice", Counter = n }, ct);
        await db.SaveAsync(new Actor { Username = "bob", Counter = 7 }, ct);

        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal([3, 7], seen);
        Assert.Equal(0, (await StatusOf(db, "count", ct)).Waiting);

        // A save after the run is new work.
        await db.SaveAsync(new Actor { Username = "alice", Counter = 4 }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal([3, 7, 4], seen);
    }

    [Fact]
    public async Task NotLatest_EverySaveGivesARun_EachOnTheCurrentObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<int>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (actor, _, _, _) => { seen.Add(actor.Counter); return Task.CompletedTask; }, latest: false);
        }, cancellationToken: ct);

        for (var n = 1; n <= 3; n++)
            await db.SaveAsync(new Actor { Username = "alice", Counter = n }, ct);

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal([3, 3, 3], seen);
    }

    [Fact]
    public async Task Latest_ASupersededMessageIsCoveredByTheNewerOnesRetries()
    {
        var ct = TestContext.Current.CancellationToken;
        var attempts = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("flaky", (_, _, _, _) => ++attempts < 3 ? throw new InvalidOperationException("not yet") : Task.CompletedTask,
                latest: true, options: Quick());
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        await db.SaveAsync(new Actor { Username = "alice", Counter = 2 }, ct);

        // The first message is dropped; the second fails twice and then succeeds.
        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        var status = await StatusOf(db, "flaky", ct);
        Assert.Equal((0, 0, null), (status.Waiting, status.Poisoned, status.LastError));
    }

    // === What the handler is given ===

    [Fact]
    public async Task Handler_GetsTheCurrentObject_AsItsOwnType_FromEverySavePath()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            // Registered for the base type: derived types are its business too.
            opts.OnAsync<BaseEntity>("people", (entity, kind, _, _) =>
            {
                seen.Add($"{entity.GetType().Name}:{entity.Id}:{entity.Name}:{kind}");
                return Task.CompletedTask;
            }, latest: false);
        }, cancellationToken: ct);

        await db.SaveAsync(new Employee { Id = "e1", Name = "Eve" }, ct);
        await db.SaveManyAsync(new[] { new Person { Id = "p1", Name = "Pat" }, new Person { Id = "p2", Name = "Pip" } }, ct);
        await db.DrainAsync(cancellationToken: ct);
        await db.ChangeAsync<Person>("p1", p => { p.Name = "Patricia"; return p; }, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(["Employee:e1:Eve:Saved", "Person:p1:Pat:Saved", "Person:p2:Pip:Saved", "Person:p1:Patricia:Saved"], seen);
    }

    [Fact]
    public async Task SaveMany_OfAnObjectGivenANewKey_QueuesTheHandlerUnderTheNewKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("names", (actor, _, _, _) => { seen.Add(actor.Username); return Task.CompletedTask; }, latest: false);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        seen.Clear();

        // Loaded as alice, written as carol. The object still carries the key it was loaded
        // under, and the handler was queued for alice, the object that was not saved.
        var actor = await db.GetAsync<Actor>("alice", ct);
        actor!.Username = "carol";
        await db.SaveManyAsync(new[] { actor }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["carol"], seen);
    }

    [Fact]
    public async Task Handler_GetsAnObjectOfAnySize()
    {
        var ct = TestContext.Current.CancellationToken;
        var length = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.Store<LargeDocument>();
            opts.OnAsync<LargeDocument>("big", (document, _, _, _) => { length = document.Payload.Length; return Task.CompletedTask; });
        }, cancellationToken: ct);

        // Far more than a queue message can hold: the message carries only the key.
        await db.SaveAsync(new LargeDocument { Id = "big", Title = "Big", Payload = new string('x', 300_000) }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(300_000, length);
    }

    [Fact]
    public async Task Handler_WritingBackAnObjectThatChangedDuringItsRun_GetsAConcurrencyException()
    {
        var ct = TestContext.Current.CancellationToken;
        Exception? caught = null;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("writer", async (actor, _, db, token) =>
            {
                // Someone else changes the object while the handler is working on it.
                await db.ChangeAsync<Actor>(actor.Username, a => { a.DisplayName = "changed elsewhere"; return a; }, token);
                actor.Counter = 99;
                try
                {
                    await db.SaveAsync(actor, token);
                }
                catch (Exception e)
                {
                    caught = e;
                }
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.IsType<ConcurrencyException>(caught);
        Assert.Equal(0, (await db.GetAsync<Actor>("alice", ct))!.Counter);
    }

    [Fact]
    public async Task AnObjectDeletedBeforeItsRun_IsSkipped_AndDeletesAndRebuildsQueueNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (_, _, _, _) => { runs++; return Task.CompletedTask; }, latest: false);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob" }, ct);
        await db.DeleteAsync<Actor>("alice", ct);
        await db.RebuildSearchIndex(ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // bob only
        Assert.Equal(1, runs);
        Assert.Equal(0, (await StatusOf(db, "count", ct)).Waiting);
    }

    [Fact]
    public async Task AnUploadedBlob_QueuesHandlersForItsRecord()
    {
        var ct = TestContext.Current.CancellationToken;
        var paths = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnUpload();
            opts.OnAsync<BlobFile>("files", (file, _, _, _) => { paths.Add(file.Path); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.UploadBlobAsync("docs/hello.txt", "hello", cancellationToken: ct);

        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["docs/hello.txt"], paths);
    }

    // === Failure ===

    [Fact]
    public async Task AFailingRun_IsTriedAgain_AndSucceeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var attempts = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("flaky", (_, _, _, _) => ++attempts < 3 ? throw new InvalidOperationException("not yet") : Task.CompletedTask, options: Quick());
        }, cancellationToken: ct);

        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);   // a handler's failure is not the save's

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        var status = await StatusOf(db, "flaky", ct);
        Assert.Equal((0, 0, null), (status.Waiting, status.Poisoned, status.LastError));
    }

    [Fact]
    public async Task ARunThatKeepsFailing_IsSetAside_AndCanBeTriedAgainByHand()
    {
        var ct = TestContext.Current.CancellationToken;
        var broken = true;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("broken", (_, _, _, _) =>
            {
                runs++;
                return broken ? throw new InvalidOperationException("the model is down") : Task.CompletedTask;
            }, options: Quick(maxAttempts: 3));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        var status = await StatusOf(db, "broken", ct);
        Assert.Equal((0, 1), (status.Waiting, status.Poisoned));
        Assert.Contains("the model is down", status.LastError);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));   // set aside means left alone

        broken = false;
        Assert.Equal(1, await db.RetryPoisonedAsync("broken", ct));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        status = await StatusOf(db, "broken", ct);
        Assert.Equal((0, 0, null, 4), (status.Waiting, status.Poisoned, status.LastError, runs));
    }

    [Fact]
    public async Task AFailedRun_WaitsBeforeItIsTriedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("slow-retry", (_, _, _, _) => throw new InvalidOperationException("no"),
                options: new AsyncHandlerOptions { RetryDelay = TimeSpan.FromMinutes(1) });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // one attempt; the next is a minute away
        var status = await StatusOf(db, "slow-retry", ct);
        Assert.Equal((1, 0), (status.Waiting, status.Poisoned));
    }

    [Fact]
    public async Task ARunThatTakesTooLong_IsCancelled_AndCountsAsAFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("stuck", (_, _, _, token) => Task.Delay(Timeout.Infinite, token),
                options: new AsyncHandlerOptions { MaxAttempts = 1, Timeout = TimeSpan.FromMilliseconds(200) });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, (await StatusOf(db, "stuck", ct)).Poisoned);
    }

    [Fact]
    public async Task ARunThatFindsTheWriteLockHeld_WaitsBeforeItIsTriedAgain_AndIsNotCountedAsAFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            // What a handler's own save throws while another process holds the database's write lock.
            opts.OnAsync<Actor>("blocked", (_, _, _, _) =>
                ++runs <= 2 ? throw new WriteLockUnavailableException("blocked") : Task.CompletedTask,
                options: new AsyncHandlerOptions { MaxAttempts = 1, RetryDelay = TimeSpan.FromSeconds(1) });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        // Once given back with no wait, the message was run again at once, so a drain with no
        // limit never returned; the limit here only turns that into a failure instead of a hang.
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(TimeSpan.FromSeconds(20));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: limit.Token));
        var status = await StatusOf(db, "blocked", ct);
        Assert.Equal((1, 0, null), (status.Waiting, status.Poisoned, status.LastError));   // MaxAttempts is 1: a counted failure would be set aside

        // Run again after the retry delay; the second time the wait is twice as long.
        await Task.Delay(1500, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: limit.Token));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: limit.Token));
        await Task.Delay(2500, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: limit.Token));

        status = await StatusOf(db, "blocked", ct);
        Assert.Equal((0, 0, 3), (status.Waiting, status.Poisoned, runs));
    }

    // === Loops ===

    [Fact]
    public async Task AHandlerWritingBackToItsObject_DoesNotQueueItselfAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("increment", (actor, _, db, token) =>
                db.ChangeAsync<Actor>(actor.Username, a => { a.Counter++; return a; }, token), latest: false);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, (await db.GetAsync<Actor>("alice", ct))!.Counter);
    }

    [Fact]
    public async Task AHandlersSave_QueuesOnlyLowerPriorities_SoHandlersCannotCircle()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("describe", async (actor, _, db, token) =>
            {
                ran.Add("describe");
                await db.ChangeAsync<Actor>(actor.Username, a => { a.DisplayName = "described"; return a; }, token);
            }, priority: 10);
            opts.OnAsync<Actor>("tag", async (actor, _, db, token) =>
            {
                ran.Add($"tag sees {actor.DisplayName}");
                await db.ChangeAsync<Actor>(actor.Username, a => { a.AvatarUrl = "tagged"; return a; }, token);
            }, priority: 1);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        // "tag" was queued by the first save and again by describe's change; being "latest" it ran once, after describe.
        // Its own change did not send the work back up to "describe".
        Assert.Equal(["describe", "tag sees described"], ran);
        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("described", "tagged"), (alice!.DisplayName, alice.AvatarUrl));
    }

    [Fact]
    public async Task AWriteThatChangesNothing_WritesNothing_AndQueuesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var inline = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.On<Actor>((_, _, _, _) => { inline++; return Task.CompletedTask; });
            opts.OnAsync<Actor>("count", (_, _, _, _) => { runs++; return Task.CompletedTask; }, latest: false);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var before = await db.GetAsync<Actor>("alice", ct);

        // A change that returns the object as it is: no write at all.
        var result = await db.ChangeAsync<Actor>("alice", a => a, ct);
        Assert.Empty(result.Changes);
        Assert.Equal(1, inline);
        Assert.Equal(before!.GetETag(), (await db.GetAsync<Actor>("alice", ct))!.GetETag());

        // Saving an object exactly as it was loaded: the save happens, but there is nothing new to handle.
        await db.SaveAsync(before, ct);

        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task AChainOfHandlersLongerThanAllowed_IsSetAsideWhereItCanBeSeen()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.AsyncHandlerMaxHops = 1;
            opts.OnAsync<Note>("first", async (note, _, db, token) =>
            {
                ran.Add("first");
                await db.SaveAsync(new Actor { Username = note.NoteId }, token);
            }, priority: 30);
            opts.OnAsync<Actor>("second", async (actor, _, db, token) =>
            {
                ran.Add("second");
                await db.SaveAsync(new Person { Id = actor.Username }, token);
            }, priority: 20);
            opts.OnAsync<Person>("third", (_, _, _, _) => { ran.Add("third"); return Task.CompletedTask; }, priority: 10);
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(["first", "second"], ran);
        var third = await StatusOf(db, "third", ct);
        Assert.Equal((0, 1), (third.Waiting, third.Poisoned));
    }

    // === Control ===

    [Fact]
    public async Task Drain_StopsAtTheLimit_AndCannotBeCalledFromAHandler()
    {
        var ct = TestContext.Current.CancellationToken;
        Exception? fromHandler = null;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("nested", async (_, _, db, token) =>
            {
                try
                {
                    await db.DrainAsync(cancellationToken: token);
                }
                catch (Exception e)
                {
                    fromHandler = e;
                }
            }, latest: false);
        }, cancellationToken: ct);

        foreach (var name in new[] { "a", "b", "c" })
            await db.SaveAsync(new Actor { Username = name }, ct);

        Assert.Equal(2, await db.DrainAsync(max: 2, cancellationToken: ct));
        Assert.Equal(1, (await StatusOf(db, "nested", ct)).Waiting);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.IsType<InvalidOperationException>(fromHandler);
    }

    [Fact]
    public async Task APausedHandler_IsLeftAlone_UntilItIsResumed()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("held", (_, _, _, _) => { ran.Add("held"); return Task.CompletedTask; }, priority: 10);
            opts.OnAsync<Actor>("free", (_, _, _, _) => { ran.Add("free"); return Task.CompletedTask; }, priority: 1);
        }, cancellationToken: ct);

        db.PauseHandlers("held");
        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.True((await StatusOf(db, "held", ct)).Paused);
        db.ResumeHandlers("held");
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["free", "held"], ran);
        Assert.Throws<ArgumentException>(() => db.PauseHandlers("no-such-handler"));
    }

    [Fact]
    public async Task ClosingTheDatabase_CancelsADrainsRun_AndItsWorkStaysQueued()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("long", async (_, _, _, token) =>
            {
                started.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, token);
                }
                catch (OperationCanceledException)
                {
                    cancelled.TrySetResult();
                    throw;
                }
            }, options: new AsyncHandlerOptions { MaxAttempts = 1 });   // a counted failure would be set aside
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        var drain = db.DrainAsync(cancellationToken: ct);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);

        // Before, a drain's run had only the drain's token, which closing the database did not cancel.
        db.Dispose();

        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain);
        // The queues belong to the catalog, which is still open.
        var status = await StatusOf(db, "long", ct);
        Assert.Equal((1, 0, null), (status.Waiting, status.Poisoned, status.LastError));
    }

    [Fact]
    public async Task Requeue_QueuesTheHandlerForEveryStoredObjectOfItsType()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("redo", (actor, _, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; });
        }, cancellationToken: ct);

        foreach (var name in new[] { "a", "b", "c" })
            await db.SaveAsync(new Actor { Username = name }, ct);
        await db.SaveAsync(new Note { NoteId = "not an actor" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        Assert.Equal(3, await db.RequeueAsync("redo", ct));
        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["a", "b", "c"], ran.Order());
    }

    [Fact]
    public async Task Reset_EmptiesTheQueues()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (_, _, _, _) => { runs++; return Task.CompletedTask; }, latest: false);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob" }, ct);
        Assert.Equal(2, (await StatusOf(db, "count", ct)).Waiting);

        await db.ResetDatabaseAsync(ct);

        Assert.Equal(0, (await StatusOf(db, "count", ct)).Waiting);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, runs);
    }

    // === Opening ===

    [Fact]
    public async Task AnOpenThatFails_KeepsNothing_SoTheNextOpenStartsTheHandlers()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var inner = Catalog.QueueServiceClientFactory!;
        Catalog.QueueServiceClientFactory = () => new FailsOnceQueueServiceClient(inner());
        void Configure(ILottaConfiguration opts)
        {
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (_, _, _, _) => { runs++; return Task.CompletedTask; });
        }

        // Fails while starting the handlers, the last step of opening.
        await Assert.ThrowsAsync<RequestFailedException>(() => CreateDbAsync(Configure, cancellationToken: ct));

        // Before, the half-open database was kept and handed out again: its saves queued nothing and said nothing.
        var db = await CreateDbAsync(Configure, cancellationToken: ct);
        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);
        Assert.Single(await db.GetHandlerStatusAsync(ct));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task OpeningOneDatabaseTwiceAtOnce_GivesOneInstance()
    {
        var ct = TestContext.Current.CancellationToken;
        Task<LottaDB> Open() => Catalog.GetDatabaseAsync("twice", opts =>
        {
            opts.Store<Actor>();
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (_, _, _, _) => Task.CompletedTask);
        }, ct);

        var both = await Task.WhenAll(Open(), Open());
        Assert.Same(both[0], both[1]);
        Assert.Same(both[0], await Open());
        Assert.Single(await both[0].GetHandlerStatusAsync(ct));
    }

    [Fact]
    public async Task CallersSharingAnOpen_EachWaitWithTheirOwnToken()
    {
        var ct = TestContext.Current.CancellationToken;
        using var hold = new OpenHold(Catalog);
        static void Configure(ILottaConfiguration opts)
        {
            opts.Store<Actor>();
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (_, _, _, _) => Task.CompletedTask);
        }
        using var first = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var gaveUp = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var a = Task.Run(() => Catalog.GetDatabaseAsync("shared", Configure, first.Token), ct);
        await hold.Reached.WaitAsync(TimeSpan.FromSeconds(20), ct);
        // Each on its own thread: before, joining an open could block until it finished.
        var b = Task.Run(() => Catalog.GetDatabaseAsync("shared", Configure, ct), ct);
        var c = Task.Run(() => Catalog.GetDatabaseAsync("shared", Configure, gaveUp.Token), ct);

        // Before, everyone waited on the open under the token of the caller who started it: one
        // who gave up waited on regardless, and the starter giving up failed the open for all.
        gaveUp.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => c.WaitAsync(TimeSpan.FromSeconds(20), ct));
        first.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => a.WaitAsync(TimeSpan.FromSeconds(20), ct));
        Assert.False(b.IsCompleted);   // the open is still held

        hold.Release();
        var db = await b.WaitAsync(TimeSpan.FromSeconds(20), ct);
        Assert.Same(db, await Catalog.GetDatabaseAsync("shared", Configure, ct));
        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);
        Assert.Single(await db.GetHandlerStatusAsync(ct));
    }

    [Fact]
    public async Task DisposingTheCatalogDuringAnOpen_KeepsAndHandsOutNoDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var hold = new OpenHold(Catalog);
        // Auto, so a database kept would have its pump running handlers.
        static void Configure(ILottaConfiguration opts)
        {
            opts.Store<Actor>();
            opts.OnAsync<Actor>("count", (_, _, _, _) => Task.CompletedTask);
        }

        var opening = Task.Run(() => Catalog.GetDatabaseAsync("closing", Configure, ct), ct);
        await hold.Reached.WaitAsync(TimeSpan.FromSeconds(20), ct);
        Catalog.Dispose();
        hold.Release();

        // Before, the open went on to keep the database in the disposed catalog and hand it out
        // live, its handlers running and its index open, with nothing left to dispose it.
        await Assert.ThrowsAsync<ObjectDisposedException>(() => opening.WaitAsync(TimeSpan.FromSeconds(20), ct));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => Catalog.GetDatabaseAsync("closing", Configure, ct));
    }

    [Fact]
    public async Task DeletingTheCatalogDuringAnOpen_KeepsNothing_SoTheNextOpenStartsAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        using var hold = new OpenHold(Catalog);
        var runs = 0;
        void Configure(ILottaConfiguration opts)
        {
            opts.Store<Actor>();
            opts.AsyncHandlers = AsyncHandlerMode.Manual;
            opts.OnAsync<Actor>("count", (_, _, _, _) => { runs++; return Task.CompletedTask; });
        }

        var opening = Task.Run(() => Catalog.GetDatabaseAsync("deleted", Configure, ct), ct);
        await hold.Reached.WaitAsync(TimeSpan.FromSeconds(20), ct);
        // Deletes the manifest the open has already written.
        var deleting = Catalog.DeleteAsync(ct);
        hold.Release();
        await deleting.WaitAsync(TimeSpan.FromSeconds(20), ct);

        // Before, the open kept a database whose manifest the deletion had removed.
        await Assert.ThrowsAsync<InvalidOperationException>(() => opening.WaitAsync(TimeSpan.FromSeconds(20), ct));
        var db = await Catalog.GetDatabaseAsync("deleted", Configure, ct);
        Assert.Contains("deleted", await Catalog.ListAsync(ct));
        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, runs);
    }

    /// <summary>
    /// Holds the catalog's next open at its last step, making the handlers' queues, until
    /// <see cref="Release"/>: the queue storage's first request for a queue waits for it.
    /// </summary>
    private sealed class OpenHold : IDisposable
    {
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _released = new();
        private readonly ConcurrentBag<QueueServiceClient> _clients = [];
        private int _taken;

        public OpenHold(LottaCatalog catalog)
        {
            var inner = catalog.QueueServiceClientFactory!;
            catalog.QueueServiceClientFactory = () =>
            {
                var client = inner();
                _clients.Add(client);
                return new Held(client, this);
            };
        }

        /// <summary>Completes once an open is being held.</summary>
        public Task Reached => _reached.Task;

        public void Release() => _released.Set();

        public void Dispose()
        {
            _released.Set();
            foreach (var client in _clients)
                (client as IDisposable)?.Dispose();
        }

        /// <summary>
        /// Not IDisposable: the catalog letting go of it while an open is held must not close the
        /// client under that open. <see cref="OpenHold"/> closes them at the end of the test.
        /// </summary>
        private sealed class Held(QueueServiceClient inner, OpenHold hold) : QueueServiceClient
        {
            public override QueueClient GetQueueClient(string queueName)
            {
                if (Interlocked.Exchange(ref hold._taken, 1) == 0)
                {
                    hold._reached.TrySetResult();
                    hold._released.Wait();
                }
                return inner.GetQueueClient(queueName);
            }

            public override AsyncPageable<Azure.Storage.Queues.Models.QueueItem> GetQueuesAsync(
                Azure.Storage.Queues.Models.QueueTraits traits = default, string? prefix = null, CancellationToken cancellationToken = default) =>
                inner.GetQueuesAsync(traits, prefix, cancellationToken);
        }
    }

    /// <summary>Queue storage whose first request for a queue fails, as a transient error would.</summary>
    private sealed class FailsOnceQueueServiceClient(QueueServiceClient inner) : QueueServiceClient, IDisposable
    {
        private int _calls;

        public override QueueClient GetQueueClient(string queueName) =>
            Interlocked.Increment(ref _calls) == 1 ? throw new RequestFailedException(503, "Queue storage is unavailable.") : inner.GetQueueClient(queueName);

        public void Dispose() => (inner as IDisposable)?.Dispose();
    }

    // === Registration ===

    [Theory]
    [InlineData("ab")]
    [InlineData("Describe")]
    [InlineData("has space")]
    [InlineData("-lead")]
    [InlineData("trail-")]
    [InlineData("double--hyphen")]
    [InlineData("describe-poison")]
    [InlineData("a-name-that-is-longer-than-forty-characters-x")]
    public void AHandlerName_MustBeUsableAsAQueueName(string name)
    {
        var config = new LottaConfiguration();
        Assert.Throws<ArgumentException>(() => config.OnAsync<Actor>(name, (_, _, _, _) => Task.CompletedTask));
    }

    [Fact]
    public void TwoHandlers_CannotShareAName_AndOptionsAreChecked()
    {
        var config = new LottaConfiguration();
        config.OnAsync<Actor>("describe-mail", (_, _, _, _) => Task.CompletedTask);

        Assert.Throws<ArgumentException>(() => config.OnAsync<Note>("describe-mail", (_, _, _, _) => Task.CompletedTask));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            config.OnAsync<Note>("other", (_, _, _, _) => Task.CompletedTask, options: new AsyncHandlerOptions { MaxAttempts = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            config.OnAsync<Note>("other", (_, _, _, _) => Task.CompletedTask, options: new AsyncHandlerOptions { RetryDelay = TimeSpan.FromSeconds(-1) }));
    }
}

/// <summary>
/// OnAsync&lt;T&gt; with handlers running by themselves in the background, as they do by default.
/// </summary>
public abstract class AsyncHandlerPumpTestBase : LottaTestBase
{
    protected AsyncHandlerPumpTestBase(Action<LottaCatalog> config) : base(config) { }

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task Handlers_RunByThemselves_SoonAfterTheSave()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.OnAsync<Actor>("greet", async (actor, _, db, token) =>
            {
                // A save from inside the handler works: the handler is not inside anyone's write.
                await db.SaveAsync(new Note { NoteId = $"hello-{actor.Username}" }, token);
                seen.TrySetResult(actor.Username);
            }), cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal("alice", await seen.Task.WaitAsync(Patience, ct));
        Assert.NotNull(await db.GetAsync<Note>("hello-alice", ct));
    }

    [Fact]
    public async Task APausedHandler_Waits_AndRunsOnceResumed()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.OnAsync<Actor>("held", (_, _, _, _) => { seen.TrySetResult(); return Task.CompletedTask; }), cancellationToken: ct);

        db.PauseHandlers();
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await Task.Delay(500, ct);
        Assert.False(seen.Task.IsCompleted);

        db.ResumeHandlers();
        await seen.Task.WaitAsync(Patience, ct);
    }

    [Fact]
    public async Task ClosingTheDatabase_CancelsARunInProgress()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.OnAsync<Actor>("long", async (_, _, _, token) =>
            {
                started.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.Infinite, token);
                }
                catch (OperationCanceledException)
                {
                    cancelled.TrySetResult();
                    throw;
                }
            }), cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await started.Task.WaitAsync(Patience, ct);

        db.Dispose();

        await cancelled.Task.WaitAsync(Patience, ct);
    }
}
