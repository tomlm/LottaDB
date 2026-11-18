using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Queues;

namespace Lotta.Tests;

/// <summary>
/// Tests for queued rules (<c>When&lt;T&gt;().Queue(...)</c>): work that is queued by a save and run later, outside it.
/// These run the handlers by hand with <see cref="LottaDB.DrainAsync"/>, so every step is in order.
/// </summary>
public abstract class QueuedRuleTestBase : LottaTestBase
{
    protected QueuedRuleTestBase(Action<LottaCatalog> config) : base(config) { }

    private static async Task<QueueStatus> StatusOf(LottaDB db, string name, CancellationToken ct) =>
        (await db.GetQueueStatusAsync(ct)).Single(s => s.Name == name);

    // === Order ===

    [Fact]
    public async Task HigherPriority_DrainsFirst_AndEachHandlerIsFirstInFirstOut()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Queue("low", (note, _, _) => { ran.Add($"low:{note.NoteId}"); return Task.CompletedTask; }).Priority(1).EverySave();
            opts.Rules.When<Note>().Queue("high", (note, _, _) => { ran.Add($"high:{note.NoteId}"); return Task.CompletedTask; }).Priority(10).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Queue("notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; }).Priority(5).EverySave();
            opts.Rules.When<Actor>().Queue("actors", (actor, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; }).Priority(5).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (actor, _, _) => { seen.Add(actor.Counter); return Task.CompletedTask; });
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (actor, _, _) => { seen.Add(actor.Counter); return Task.CompletedTask; }).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("flaky", (_, _, _) => ++attempts < 3 ? throw new InvalidOperationException("not yet") : Task.CompletedTask).Retry(5, TimeSpan.Zero);
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
            opts.QueueMode = QueueMode.Manual;
            // Registered for the base type: derived types are its business too.
            opts.Rules.When<BaseEntity>().Queue("people", (entity, _, _) =>
            {
                seen.Add($"{entity.GetType().Name}:{entity.Id}:{entity.Name}");
                return Task.CompletedTask;
            }).EverySave();
        }, cancellationToken: ct);

        await db.SaveAsync(new Employee { Id = "e1", Name = "Eve" }, ct);
        await db.SaveManyAsync(new[] { new Person { Id = "p1", Name = "Pat" }, new Person { Id = "p2", Name = "Pip" } }, ct);
        await db.DrainAsync(cancellationToken: ct);
        await db.ChangeAsync<Person>("p1", p => { p.Name = "Patricia"; return p; }, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(["Employee:e1:Eve", "Person:p1:Pat", "Person:p2:Pip", "Person:p1:Patricia"], seen);
    }

    [Fact]
    public async Task SaveMany_OfAnObjectGivenANewKey_QueuesTheHandlerUnderTheNewKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("names", (actor, _, _) => { seen.Add(actor.Username); return Task.CompletedTask; }).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Store<LargeDocument>();
            opts.Rules.When<LargeDocument>().Queue("big", (document, _, _) => { length = document.Payload.Length; return Task.CompletedTask; });
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("writer", async (actor, db, token) =>
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => { runs++; return Task.CompletedTask; }).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.OnUpload();
            opts.Rules.When<BlobFile>().Queue("files", (file, _, _) => { paths.Add(file.Path); return Task.CompletedTask; });
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("flaky", (_, _, _) => ++attempts < 3 ? throw new InvalidOperationException("not yet") : Task.CompletedTask).Retry(5, TimeSpan.Zero);
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("broken", (_, _, _) =>
            {
                runs++;
                return broken ? throw new InvalidOperationException("the model is down") : Task.CompletedTask;
            }).Retry(3, TimeSpan.Zero);
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("slow-retry", (_, _, _) => throw new InvalidOperationException("no")).Retry(5, TimeSpan.FromMinutes(1));
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("stuck", (_, _, token) => Task.Delay(Timeout.Infinite, token)).Retry(1, TimeSpan.FromSeconds(2)).Timeout(TimeSpan.FromMilliseconds(200));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        var timer = System.Diagnostics.Stopwatch.StartNew();
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(60), $"the run was stopped after {timer.Elapsed}, not its 200 ms timeout");
        Assert.Equal(1, (await StatusOf(db, "stuck", ct)).Poisoned);
    }

    [Fact]
    public async Task ARunThatFindsTheWriteLockHeld_WaitsBeforeItIsTriedAgain_AndIsNotCountedAsAFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            // What a handler's own save throws while another process holds the database's write lock.
            opts.Rules.When<Actor>().Queue("blocked", (_, _, _) =>
                ++runs <= 2 ? throw new WriteLockUnavailableException("blocked") : Task.CompletedTask).Retry(1, TimeSpan.FromSeconds(1));
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("increment", (actor, db, token) =>
                db.ChangeAsync<Actor>(actor.Username, a => { a.Counter++; return a; }, token)).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("describe", async (actor, db, token) =>
            {
                ran.Add("describe");
                await db.ChangeAsync<Actor>(actor.Username, a => { a.DisplayName = "described"; return a; }, token);
            }).Priority(10);
            opts.Rules.When<Actor>().Queue("tag", async (actor, db, token) =>
            {
                ran.Add($"tag sees {actor.DisplayName}");
                await db.ChangeAsync<Actor>(actor.Username, a => { a.AvatarUrl = "tagged"; return a; }, token);
            }).Priority(1);
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Do((_, _, _) => { inline++; return Task.CompletedTask; });
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => { runs++; return Task.CompletedTask; }).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.MaxQueueHops = 1;
            opts.Rules.When<Note>().Queue("first", async (note, db, token) =>
            {
                ran.Add("first");
                await db.SaveAsync(new Actor { Username = note.NoteId }, token);
            }).Priority(30);
            opts.Rules.When<Actor>().Queue("second", async (actor, db, token) =>
            {
                ran.Add("second");
                await db.SaveAsync(new Person { Id = actor.Username }, token);
            }).Priority(20);
            opts.Rules.When<Person>().Queue("third", (_, _, _) => { ran.Add("third"); return Task.CompletedTask; }).Priority(10);
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("nested", async (_, db, token) =>
            {
                try
                {
                    await db.DrainAsync(cancellationToken: token);
                }
                catch (Exception e)
                {
                    fromHandler = e;
                }
            }).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("held", (_, _, _) => { ran.Add("held"); return Task.CompletedTask; }).Priority(10);
            opts.Rules.When<Actor>().Queue("free", (_, _, _) => { ran.Add("free"); return Task.CompletedTask; }).Priority(1);
        }, cancellationToken: ct);

        db.PauseQueue("held");
        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.True((await StatusOf(db, "held", ct)).Paused);
        db.ResumeQueue("held");
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["free", "held"], ran);
        Assert.Throws<ArgumentException>(() => db.PauseQueue("no-such-handler"));
    }

    [Fact]
    public async Task ClosingTheDatabase_CancelsADrainsRun_AndItsWorkStaysQueued()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("long", async (_, _, token) =>
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
            }).Retry(1, TimeSpan.FromSeconds(2));   // a counted failure would be set aside
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
    public async Task ClosingTheDatabase_DuringTheLastRunADrainAllows_StillThrows()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("long", async (_, _, token) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, token);
            }).Retry(1, TimeSpan.FromSeconds(2));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        var drain = db.DrainAsync(1, ct);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
        db.Dispose();

        // Before, a drain whose interrupted run was its last returned 1, as if that run had been done.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain.WaitAsync(TimeSpan.FromSeconds(20), ct));
        var status = await StatusOf(db, "long", ct);
        Assert.Equal((1, 0, null), (status.Waiting, status.Poisoned, status.LastError));
    }

    // === Filter ===

    /// <summary>A filter that fails on "boom"; a method, since an expression cannot hold a throw.</summary>
    private static bool Picky(Note note) => note.Content == "boom" ? throw new InvalidOperationException("bad filter") : true;

    [Fact]
    public async Task When_ASaveTheFilterIsFalseFor_QueuesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => note.Content.Length > 5)
                .Queue("long-notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; }).EverySave();
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "short", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "long", Content = "a longer note" }, ct);

        Assert.Equal(1, (await StatusOf(db, "long-notes", ct)).Waiting);   // only the long one was queued
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["long"], ran);
    }

    [Fact]
    public async Task When_AnObjectThatNoLongerMatchesByItsRun_IsSkipped_AndNotCountedAsARun()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => note.Content.Length > 5)
                .Queue("long-notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a longer note" }, ct);   // queued
        await db.SaveAsync(new Note { NoteId = "n1", Content = "hi" }, ct);              // not queued, and the queued run no longer applies

        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Empty(ran);
        var status = await StatusOf(db, "long-notes", ct);
        Assert.Equal((0, 0, null), (status.Waiting, status.Poisoned, status.LastError));
    }

    [Fact]
    public async Task When_AFilterThatThrowsOnSave_QueuesNothing_AndTheSaveStillHappens()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => Picky(note))
                .Queue("picky", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "n1", Content = "boom" }, ct);

        Assert.Contains("bad filter", Assert.Single(result.Errors).Message);
        Assert.NotNull(await db.GetAsync<Note>("n1", ct));
        Assert.Equal(0, (await StatusOf(db, "picky", ct)).Waiting);
    }

    [Fact]
    public async Task When_Requeue_QueuesOnlyTheObjectsTheFilterIsTrueFor()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => note.Content.Length > 5)
                .Queue("long-notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "a", Content = "a longer note" }, ct);
        await db.SaveAsync(new Note { NoteId = "b", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "c", Content = "another long one" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        Assert.Equal(2, await db.RequeueAsync("long-notes", ct));
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["a", "c"], ran.Order());
    }

    /// <summary>A queued rule for notes with <paramref name="filter"/>, the notes saved and drained, then a requeue: which notes ran, and how they were found.</summary>
    private async Task<(int Queued, List<string> Ran, string? Path)> RequeueWithAsync(
        System.Linq.Expressions.Expression<Func<Note, bool>> filter, Note[] notes, CancellationToken ct, string testName)
    {
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(filter).Queue("redo", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct, testName: testName);
        await db.SaveManyAsync(notes, ct);
        await db.DrainAsync(cancellationToken: ct);
        db.ReloadSearcher();
        ran.Clear();

        var queued = await db.RequeueAsync("redo", ct);
        await db.DrainAsync(cancellationToken: ct);
        return (queued, [.. ran.Order()], db.LastRequeuePath);
    }

    [Fact]
    public async Task Requeue_WithAFilterTheIndexCanAnswer_UsesTheIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var (queued, ran, path) = await RequeueWithAsync(n => n.AuthorId == "alice",
            [new Note { NoteId = "a1", AuthorId = "alice" }, new Note { NoteId = "b1", AuthorId = "bob" }, new Note { NoteId = "a2", AuthorId = "alice" }],
            ct, nameof(Requeue_WithAFilterTheIndexCanAnswer_UsesTheIndex));

        Assert.Equal((2, "index"), (queued, path));
        Assert.Equal(["a1", "a2"], ran);
    }

    [Fact]
    public async Task Requeue_WithAFilterComparingTwoFields_ReadsEveryObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var (queued, ran, path) = await RequeueWithAsync(n => n.AuthorId == n.Domain,
            [new Note { NoteId = "same", AuthorId = "x", Domain = "x" }, new Note { NoteId = "differ", AuthorId = "x", Domain = "y" }],
            ct, nameof(Requeue_WithAFilterComparingTwoFields_ReadsEveryObject));

        Assert.Equal((1, "scan"), (queued, path));
        Assert.Equal(["same"], ran);
    }

    [Fact]
    public async Task Requeue_WhenTheIndexFindsMoreThanTheFilterIsTrueFor_QueuesOnlyTrueMatches()
    {
        var ct = TestContext.Current.CancellationToken;
        // Content is analyzed: the index matches the word "hello" in any note that has it.
        var (queued, ran, path) = await RequeueWithAsync(n => n.Content == "hello",
            [new Note { NoteId = "exact", Content = "hello" }, new Note { NoteId = "longer", Content = "hello world" }, new Note { NoteId = "other", Content = "bye" }],
            ct, nameof(Requeue_WhenTheIndexFindsMoreThanTheFilterIsTrueFor_QueuesOnlyTrueMatches));

        Assert.Equal((1, "index"), (queued, path));
        Assert.Equal(["exact"], ran);
    }

    [Fact]
    public async Task Requeue_QueuesTheHandlerForEveryStoredObjectOfItsType()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("redo", (actor, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; });
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => { runs++; return Task.CompletedTask; }).EverySave();
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }

        // Fails while starting the handlers, the last step of opening.
        await Assert.ThrowsAsync<RequestFailedException>(() => CreateDbAsync(Configure, cancellationToken: ct));

        // Before, the half-open database was kept and handed out again: its saves queued nothing and said nothing.
        var db = await CreateDbAsync(Configure, cancellationToken: ct);
        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);
        Assert.Single(await db.GetQueueStatusAsync(ct));
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => Task.CompletedTask);
        }, ct);

        var both = await Task.WhenAll(Open(), Open());
        Assert.Same(both[0], both[1]);
        Assert.Same(both[0], await Open());
        Assert.Single(await both[0].GetQueueStatusAsync(ct));
    }

    [Fact]
    public async Task CallersSharingAnOpen_EachWaitWithTheirOwnToken()
    {
        var ct = TestContext.Current.CancellationToken;
        using var hold = new OpenHold(Catalog);
        static void Configure(ILottaConfiguration opts)
        {
            opts.Store<Actor>();
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => Task.CompletedTask);
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
        Assert.Single(await db.GetQueueStatusAsync(ct));
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
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => Task.CompletedTask);
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
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("count", (_, _, _) => { runs++; return Task.CompletedTask; });
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

        public override AsyncPageable<Azure.Storage.Queues.Models.QueueItem> GetQueuesAsync(
            Azure.Storage.Queues.Models.QueueTraits traits = default, string? prefix = null, CancellationToken cancellationToken = default) =>
            inner.GetQueuesAsync(traits, prefix, cancellationToken);

        public void Dispose() => (inner as IDisposable)?.Dispose();
    }

    // === Rules ===

    private readonly ConcurrentQueue<string> _namedRuns = new();

    private Task Summarize(Note note, LottaDB db, CancellationToken cancellationToken)
    {
        _namedRuns.Enqueue($"summarize:{note.NoteId}");
        return Task.CompletedTask;
    }

    private sealed class NoteHandlers(ConcurrentQueue<string> runs)
    {
        public Task NotifyFollowersAsync(Note note, LottaDB db, CancellationToken cancellationToken)
        {
            runs.Enqueue($"notify:{note.NoteId}");
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Queue_NamedAfterItsHandlerMethod_RunsFromThatQueue()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlers = new NoteHandlers(_namedRuns);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Queue(Summarize).Queue(handlers.NotifyFollowersAsync);
        }, cancellationToken: ct);
        _namedRuns.Clear();

        await db.SaveAsync(new Note { NoteId = "n1" }, ct);

        var names = (await db.GetQueueStatusAsync(ct)).Select(s => (s.Name, s.Waiting)).ToList();
        Assert.Equal([("summarize", 1), ("notify-followers", 1)], names);
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["notify:n1", "summarize:n1"], _namedRuns.Order());
    }

    [Fact]
    public async Task OneRule_CanBothDoAndQueue_AndTheFilterAppliesToEach()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(n => n.Content.Length > 5)
                .Do((note, _, _) => { ran.Add($"inline:{note.NoteId}"); return Task.CompletedTask; })
                .Queue("later", (note, _, _) => { ran.Add($"queued:{note.NoteId}"); return Task.CompletedTask; })
                .Do((note, _, _) => { ran.Add($"inline2:{note.NoteId}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "short", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "long", Content = "a longer note" }, ct);
        Assert.Equal(["inline:long", "inline2:long"], ran);   // inline, in the save, in order

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["inline:long", "inline2:long", "queued:long"], ran);
    }

    [Fact]
    public async Task Modifiers_ApplyOnlyToTheQueueTheyFollow()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>()
                .Queue("first", (a, _, _) => { ran.Add($"first:{a.Counter}"); return Task.CompletedTask; })
                .Queue("second", (a, _, _) => { ran.Add($"second:{a.Counter}"); return Task.CompletedTask; }).Priority(10).EverySave();
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        await db.SaveAsync(new Actor { Username = "alice", Counter = 2 }, ct);

        var statuses = (await db.GetQueueStatusAsync(ct)).ToDictionary(s => s.Name);
        Assert.Equal((0, true), (statuses["first"].Priority, statuses["first"].Latest));
        Assert.Equal((10, false), (statuses["second"].Priority, statuses["second"].Latest));
        // "second" first, as it has the higher priority, once for each save; "first" once, for the latest.
        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["second:2", "second:2", "first:2"], ran);
    }

    [Fact]
    public async Task Retry_SetsTheAttemptsAndTheDelay()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("fails", (_, _, _) => { runs++; throw new InvalidOperationException("no"); }).Retry(2, TimeSpan.FromSeconds(1));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // the second attempt is a second away
        Assert.Equal((1, 0), ((await StatusOf(db, "fails", ct)).Waiting, (await StatusOf(db, "fails", ct)).Poisoned));
        await Task.Delay(1500, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // two attempts allowed: set aside
        var status = await StatusOf(db, "fails", ct);
        Assert.Equal((0, 1, 2), (status.Waiting, status.Poisoned, runs));
    }

    [Fact]
    public async Task ARuleChangedAfterTheDatabaseOpened_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        RuleBuilder<Actor>? rule = null;
        QueuedRuleBuilder<Actor>? queued = null;
        await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            rule = opts.Rules.When<Actor>();
            queued = rule.Queue("count", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);

        // Read when the database opened: a change now would silently do nothing.
        Assert.Throws<InvalidOperationException>(() => queued!.Priority(5));
        Assert.Throws<InvalidOperationException>(() => rule!.Do((_, _, _) => Task.CompletedTask));
        Assert.Throws<InvalidOperationException>(() => rule!.Queue("more", (_, _, _) => Task.CompletedTask));
    }

    // === Orphaned queues ===

    /// <summary>Makes a queue of this database that no rule uses, as a renamed rule leaves behind, holding work for the actors.</summary>
    private async Task MakeOrphanAsync(LottaDB db, string name, string[] waiting, string[] poisoned, CancellationToken ct)
    {
        var queues = Catalog.GetQueueServiceClient()!;
        var queue = queues.GetQueueClient(Internal.AsyncQueueNames.Queue(Catalog.Name, db.DatabaseId, name));
        var poison = queues.GetQueueClient(Internal.AsyncQueueNames.Poison(Catalog.Name, db.DatabaseId, name));
        await queue.CreateIfNotExistsAsync(cancellationToken: ct);
        await poison.CreateIfNotExistsAsync(cancellationToken: ct);
        var seq = 0;
        foreach (var key in waiting)
            await queue.SendMessageAsync(new Internal.AsyncMessage { Type = typeof(Actor).FullName!, Key = key, Sender = "old", Seq = ++seq }.ToText(), ct);
        foreach (var key in poisoned)
            await poison.SendMessageAsync(new Internal.AsyncMessage { Type = typeof(Actor).FullName!, Key = key, Sender = "old", Seq = ++seq, Attempts = 5, LastError = "failed" }.ToText(), ct);
    }

    [Fact]
    public async Task AnOrphanedQueue_IsReported_AndItsWorkCanBeMovedToTheRenamedRule()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("new-name", (a, _, _) => { ran.Add(a.Username); return Task.CompletedTask; }).EverySave();
        }, cancellationToken: ct);
        await db.SaveManyAsync(new[] { new Actor { Username = "alice" }, new Actor { Username = "bob" }, new Actor { Username = "carol" } }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();
        await MakeOrphanAsync(db, "old-name", ["alice", "bob"], ["carol"], ct);

        var orphan = Assert.Single(await db.GetQueueStatusAsync(ct), s => s.Orphaned);
        Assert.Equal(("old-name", 2, 1), (orphan.Name, orphan.Waiting, orphan.Poisoned));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));   // never run by itself

        await Assert.ThrowsAsync<ArgumentException>(() => db.MoveQueuedAsync("new-name", "old-name", ct));   // from must be orphaned
        await Assert.ThrowsAsync<ArgumentException>(() => db.MoveQueuedAsync("old-name", "no-such-rule", ct));
        Assert.Equal(3, await db.MoveQueuedAsync("old-name", "new-name", ct));

        var statuses = (await db.GetQueueStatusAsync(ct)).ToDictionary(s => s.Name);
        Assert.Equal((2, 1), (statuses["new-name"].Waiting, statuses["new-name"].Poisoned));
        Assert.Equal((true, 0, 0), (statuses["old-name"].Orphaned, statuses["old-name"].Waiting, statuses["old-name"].Poisoned));   // left in place, empty
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["alice", "bob"], ran.Order());
        Assert.Equal(1, await db.RetryPoisonedAsync("new-name", ct));   // set-aside work stayed set aside
    }

    [Fact]
    public async Task AnOrphanedQueue_CanBeDeleted_ButARulesQueueCannot()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().Queue("kept", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);
        await MakeOrphanAsync(db, "removed", ["alice"], [], ct);
        Assert.Contains(await db.GetQueueStatusAsync(ct), s => s.Name == "removed" && s.Orphaned);

        await Assert.ThrowsAsync<ArgumentException>(() => db.DeleteQueueAsync("kept", ct));
        await db.DeleteQueueAsync("removed", ct);

        var status = Assert.Single(await db.GetQueueStatusAsync(ct));
        Assert.Equal(("kept", false), (status.Name, status.Orphaned));
    }
}

/// <summary>
/// Queued rules with work running by itself in the background, as it does by default.
/// </summary>
public abstract class QueuedRulePumpTestBase : LottaTestBase
{
    protected QueuedRulePumpTestBase(Action<LottaCatalog> config) : base(config) { }

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task Handlers_RunByThemselves_SoonAfterTheSave()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Actor>().Queue("greet", async (actor, db, token) =>
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
            opts.Rules.When<Actor>().Queue("held", (_, _, _) => { seen.TrySetResult(); return Task.CompletedTask; }), cancellationToken: ct);

        db.PauseQueue();
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await Task.Delay(500, ct);
        Assert.False(seen.Task.IsCompleted);

        db.ResumeQueue();
        await seen.Task.WaitAsync(Patience, ct);
    }

    [Fact]
    public async Task ClosingTheDatabase_CancelsARunInProgress()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Actor>().Queue("long", async (_, _, token) =>
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
