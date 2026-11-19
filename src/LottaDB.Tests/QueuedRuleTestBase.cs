using System.Collections.Concurrent;
using Azure;
using Azure.Storage.Queues;
using Lotta.Internal;
using static Lotta.Rule;

namespace Lotta.Tests;

/// <summary>
/// Tests for queued rules (<c>When&lt;T&gt;().ModifyQueued(...)</c>, <c>.DoQueued(...)</c>): work that is queued by a
/// save and run later, outside it. These run the rules by hand with <see cref="LottaDB.DrainAsync"/>,
/// so every step is in order.
/// </summary>
public abstract class QueuedRuleTestBase : LottaTestBase
{
    protected QueuedRuleTestBase(Action<LottaCatalog> config) : base(config) { }

    /// <summary>Where a rule stands: the messages waiting for its type, and its own set-aside work and last error.</summary>
    /// <param name="rule">The rule's identity: <c>Actor/count</c>.</param>
    private static async Task<(int Waiting, int Poisoned, string? LastError)> StatusOf<T>(LottaDB db, string rule, CancellationToken ct)
    {
        var status = await db.Rules.StatusAsync(ct);
        var own = status.Rules.Single(r => r.Rule == rule);
        return (status.Types.SingleOrDefault(t => t.Type == typeof(T).FullName)?.Waiting ?? 0, own.Poisoned, own.LastError);
    }

    /// <summary>How many messages wait in a type's queue; 0 when nothing was ever queued for the type.</summary>
    private static async Task<int> WaitingOf<T>(LottaDB db, CancellationToken ct) =>
        (await db.Rules.StatusAsync(ct)).Types.SingleOrDefault(t => t.Type == typeof(T).FullName)?.Waiting ?? 0;

    /// <summary>The queue of a type in a database, and the queue its failed work is set aside in.</summary>
    private (QueueClient Queue, QueueClient Poison) QueuesOf<T>(string databaseId)
    {
        var queues = Catalog.GetQueueServiceClient()!;
        var name = RuleQueueNames.Queue(Catalog.Name, databaseId, typeof(T));
        return (queues.GetQueueClient(name), queues.GetQueueClient(RuleQueueNames.Poison(name)));
    }

    /// <summary>Runs work as someone outside the rule would: not as part of the run it is called from.</summary>
    private static Task Elsewhere(Func<Task> work)
    {
        using (ExecutionContext.SuppressFlow())
            return Task.Run(work);
    }

    // === Order ===

    [Fact]
    public async Task ASave_RunsNothingItself_AndItsObjectsAreTakenFirstInFirstOut()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("first", (note, _, _) => { ran.Add($"first:{note.NoteId}"); return Task.CompletedTask; });
            opts.Rules.When<Note>().DoQueued("second", (note, _, _) => { ran.Add($"second:{note.NoteId}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        foreach (var id in new[] { "n1", "n2", "n3" })
            await db.SaveAsync(new Note { NoteId = id, Content = id }, ct);
        Assert.Empty(ran);   // nothing runs inside the save
        Assert.Equal(3, await WaitingOf<Note>(db, ct));   // one message a save, whatever number of rules it matched

        Assert.Equal(6, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["first:n1", "second:n1", "first:n2", "second:n2", "first:n3", "second:n3"], ran);
    }

    [Fact]
    public async Task RulesOfOneObject_RunInPhaseOrder_ModifyBeforeThen()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            // Registered the other way round: the phase decides, not the order of registration.
            opts.Rules.When<Actor>()
                .DoQueued("notify", (actor, _, _) => { ran.Add($"notify sees {actor.DisplayName}"); return Task.CompletedTask; })
                .ModifyQueued("describe", (actor, _, _) => { ran.Add("describe"); actor.DisplayName = $"#{actor.Counter}"; return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);

        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["describe", "notify sees #1"], ran);
        // The update's write queued neither itself nor the rule that was still to come.
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task TypeQueues_AreTakenInTurn_SoNoTypeWaitsForAnotherToEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
            opts.Rules.When<Actor>().DoQueued("actors", (actor, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; });
        }, cancellationToken: ct);

        foreach (var id in new[] { "n1", "n2", "n3" })
            await db.SaveAsync(new Note { NoteId = id }, ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(4, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["n1", "n2", "n3"], ran.Where(id => id != "alice"));
        Assert.True(ran.IndexOf("alice") <= 1, $"the actor ran at {ran.IndexOf("alice")}, after the notes: {string.Join(", ", ran)}");
    }

    [Fact]
    public async Task EverySaveGivesARun_EachOnTheObjectAsItIsThen()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<int>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Actor>().DoQueued("count", (actor, _, _) => { seen.Add(actor.Counter); return Task.CompletedTask; });
        }, cancellationToken: ct);

        for (var n = 1; n <= 3; n++)
            await db.SaveAsync(new Actor { Username = "alice", Counter = n }, ct);

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal([3, 3, 3], seen);
        Assert.Equal(0, await WaitingOf<Actor>(db, ct));

        // A save after the run is new work.
        await db.SaveAsync(new Actor { Username = "alice", Counter = 4 }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal([3, 3, 3, 4], seen);
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
            opts.MaxConcurrency = 1;
            // Registered for the base type: derived types are its business too.
            opts.Rules.When<BaseEntity>().DoQueued("people", (entity, _, _) =>
            {
                seen.Add($"{entity.GetType().Name}:{entity.Id}:{entity.Name}");
                return Task.CompletedTask;
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Employee { Id = "e1", Name = "Eve" }, ct);
        await db.SaveManyAsync(new[] { new Person { Id = "p1", Name = "Pat" }, new Person { Id = "p2", Name = "Pip" } }, ct);

        // One queue for each concrete type, though the rule is for their base.
        var types = (await db.Rules.StatusAsync(ct)).Types.Select(t => (t.Type, t.Waiting)).ToList();
        Assert.Equal([(typeof(Employee).FullName!, 1), (typeof(Person).FullName!, 2)], types);

        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["Employee:e1:Eve", "Person:p1:Pat", "Person:p2:Pip"], seen.Order());

        await db.ChangeAsync<Person>("p1", p => { p.Name = "Patricia"; return p; }, ct);
        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal("Person:p1:Patricia", seen[^1]);
        Assert.Equal(4, seen.Count);
    }

    [Fact]
    public async Task SaveMany_OfAnObjectGivenANewKey_QueuesTheRuleUnderTheNewKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("names", (actor, _, _) => { seen.Add(actor.Username); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        seen.Clear();

        // Loaded as alice, written as carol. The object still carries the key it was loaded
        // under, and the rule must be queued for carol, the object that was saved.
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
            opts.Rules.When<LargeDocument>().DoQueued("big", (document, _, _) => { length = document.Payload.Length; return Task.CompletedTask; });
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
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("writer", async (actor, context, token) =>
            {
                if (++runs > 1)
                    return;   // the run queued by the change from elsewhere
                // Someone else changes the object while the handler is working on it.
                await Elsewhere(() => context.Db.ChangeAsync<Actor>(actor.Username, a => { a.DisplayName = "changed elsewhere"; return a; }, token));
                actor.Counter = 99;
                try
                {
                    await context.Db.SaveAsync(actor, token);
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
            opts.MaxConcurrency = 1;
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob" }, ct);
        await db.DeleteAsync<Actor>("alice", ct);
        await db.RebuildSearchIndex(ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // bob only
        Assert.Equal(1, runs);
        Assert.Equal(0, await WaitingOf<Actor>(db, ct));
    }

    [Fact]
    public async Task AnUploadedBlob_QueuesRulesForItsRecord()
    {
        var ct = TestContext.Current.CancellationToken;
        var paths = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.OnUpload();
            opts.Rules.When<BlobFile>().DoQueued("files", (file, _, _) => { paths.Add(file.Path); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.UploadBlobAsync("docs/hello.txt", "hello", cancellationToken: ct);

        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["docs/hello.txt"], paths);
    }

    // === DoQueued ===

    [Fact]
    public async Task DoQueued_IsToldItsRule_TheAttempt_AndAKeyThatIsTheSameOnEveryRetry()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<(string Rule, int Attempt, string Key, LottaDB Db)>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.Rules.When<Actor>().DoQueued("notify", (_, context, _) =>
            {
                seen.Add((context.Rule, context.Attempt, context.IdempotencyKey, context.Db));
                return seen.Count < 3 ? throw new InvalidOperationException("the mail server is down") : Task.CompletedTask;
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));

        Assert.Equal([("notify", 1), ("notify", 2), ("notify", 3)], seen.Select(s => (s.Rule, s.Attempt)));
        Assert.All(seen, s => Assert.Same(db, s.Db));
        // The same for every try of one save's work, so the mail server can drop the duplicates.
        var key = Assert.Single(seen.Select(s => s.Key).Distinct());
        Assert.Equal(32, key.Length);

        // Another save is other work.
        await db.SaveAsync(new Actor { Username = "alice", Counter = 2 }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, seen[^1].Attempt);
        Assert.NotEqual(key, seen[^1].Key);
    }

    // === ModifyQueued ===

    [Fact]
    public async Task ModifyQueued_GivesTheHandlerACopyToEdit_AndWritesItBackOnlyIfItChanged()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().ModifyQueued("describe", (actor, _, _) => { actor.DisplayName = $"#{actor.Counter}"; return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));

        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal("#1", alice!.DisplayName);
        // The rule's own write does not queue the rule again.
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));

        // A later save does; the handler then leaves the object as it finds it, and nothing is written.
        alice.AvatarUrl = "alice.png";
        await db.SaveAsync(alice, ct);
        var etag = alice.GetETag();
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        var after = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(etag, after!.GetETag());
        Assert.Equal(("#1", "alice.png"), (after!.DisplayName, after.AvatarUrl));
    }

    [Fact]
    public async Task ModifyQueued_ThatFails_LeavesTheObjectAsItWas_ForTheRulesAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 1;
            opts.Rules.When<Actor>()
                .ModifyQueued("describe", (actor, _, _) =>
                {
                    actor.DisplayName = "half done";   // on its copy
                    throw new InvalidOperationException("the model is down");
                })
                .DoQueued("notify", (actor, _, _) => { seen.Add(actor.DisplayName); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["Alice"], seen);
        Assert.Equal("Alice", (await db.GetAsync<Actor>("alice", ct))!.DisplayName);
        Assert.Equal(1, (await StatusOf<Actor>(db, "Actor/describe", ct)).Poisoned);
    }

    [Fact]
    public async Task ModifyRules_FollowOneAnother_EachRunningWhenItsInputsChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>(a => Changed(a.Counter))
                .ModifyQueued("describe", (actor, _, _) => { ran.Add("describe"); actor.DisplayName = $"#{actor.Counter}"; return Task.CompletedTask; });
            opts.Rules.When<Actor>(a => Changed(a.DisplayName))
                .ModifyQueued("avatar", (actor, _, _) => { ran.Add("avatar"); actor.AvatarUrl = $"{actor.DisplayName}.png"; return Task.CompletedTask; });
        }, cancellationToken: ct);

        // A new object: everything about it is a change, so one message lists both rules.
        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("#1", "#1.png"), (alice!.DisplayName, alice.AvatarUrl));

        // Only the counter changes: "describe" is queued, and its write, which changes the name, queues "avatar".
        ran.Clear();
        alice.Counter = 2;
        await db.SaveAsync(alice, ct);
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["describe", "avatar"], ran);
        alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("#2", "#2.png"), (alice!.DisplayName, alice.AvatarUrl));

        // A change to neither input queues nothing.
        alice.Domain = "example.org";
        await db.SaveAsync(alice, ct);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task ModifyQueued_WhenTheObjectWasSavedMeanwhile_AppliesWhatItSetToTheNewerVersion_IfItsInputsAreTheSame()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>(a => Changed(a.Counter)).ModifyQueued("describe", async (actor, context, token) =>
            {
                runs++;
                await Elsewhere(() => context.Db.ChangeAsync<Actor>(actor.Username, a => { a.AvatarUrl = "set elsewhere"; return a; }, token));
                actor.DisplayName = $"#{actor.Counter}";
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));

        // Both changes are there, and the handler was not called a second time to get them there.
        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("#1", "set elsewhere", 1), (alice!.DisplayName, alice.AvatarUrl, runs));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task ModifyQueued_WhenItsInputsChangedMeanwhile_DropsItsResult_AndTheSaveThatChangedThemQueuedAFreshRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>(a => Changed(a.Counter)).ModifyQueued("describe", async (actor, context, token) =>
            {
                if (++runs > 1)
                    return;   // the fresh run: leaves the object alone, so what is stored shows what the first run did
                await Elsewhere(() => context.Db.ChangeAsync<Actor>(actor.Username, a => { a.Counter = 2; return a; }, token));
                actor.DisplayName = "made for counter 1";
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));

        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("", 2, 2), (alice!.DisplayName, alice.Counter, runs));
    }

    [Fact]
    public async Task ModifyQueued_WithoutNamedInputs_DropsItsResult_WhenTheObjectWasSavedMeanwhile()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().ModifyQueued("describe", async (actor, context, token) =>
            {
                if (++runs > 1)
                    return;
                await Elsewhere(() => context.Db.ChangeAsync<Actor>(actor.Username, a => { a.AvatarUrl = "set elsewhere"; return a; }, token));
                actor.DisplayName = "made for the older version";
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        // The save from elsewhere queued the rule again, as any save does.
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));

        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("", "set elsewhere"), (alice!.DisplayName, alice.AvatarUrl));
    }

    [Fact]
    public async Task ModifyQueued_WhenSomeoneElseSetTheSameProperty_LetsTheirsStand()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>(a => Changed(a.Counter)).ModifyQueued("describe", async (actor, context, token) =>
            {
                await Elsewhere(() => context.Db.ChangeAsync<Actor>(actor.Username, a => { a.DisplayName = "theirs"; return a; }, token));
                actor.DisplayName = "the rule's";
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));

        Assert.Equal("theirs", (await db.GetAsync<Actor>("alice", ct))!.DisplayName);
        Assert.Null((await StatusOf<Actor>(db, "Actor/describe", ct)).LastError);   // dropping a result is no failure
    }

    // === Changed ===

    [Fact]
    public async Task Changed_QueuesTheRule_OnlyForASaveThatChangesTheValue_OnEverySavePath()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Actor>(a => Changed(a.DisplayName)).DoQueued("renamed", (actor, _, _) => { seen.Add(actor.DisplayName); return Task.CompletedTask; });
        }, cancellationToken: ct);

        // A new object counts as changed.
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" }, ct);
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));

        // Loaded, and saved with something else changed: not for this rule.
        var loaded = await db.GetAsync<Actor>("alice", ct);
        loaded!.Counter = 5;
        await db.SaveAsync(loaded, ct);
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));
        loaded.DisplayName = "B";
        await db.SaveAsync(loaded, ct);
        Assert.Equal(2, await WaitingOf<Actor>(db, ct));

        // Saved without having been loaded: compared with what is stored.
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "B", Counter = 9 }, ct);
        Assert.Equal(2, await WaitingOf<Actor>(db, ct));
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "C" }, ct);
        Assert.Equal(3, await WaitingOf<Actor>(db, ct));

        await db.ChangeAsync<Actor>("alice", a => { a.Counter++; return a; }, ct);
        Assert.Equal(3, await WaitingOf<Actor>(db, ct));
        await db.ChangeAsync<Actor>("alice", a => { a.DisplayName = "D"; return a; }, ct);
        Assert.Equal(4, await WaitingOf<Actor>(db, ct));

        await db.SaveManyAsync(new[] { new Actor { Username = "alice", DisplayName = "D", Counter = 7 } }, ct);
        Assert.Equal(4, await WaitingOf<Actor>(db, ct));
        await db.SaveManyAsync(new[] { new Actor { Username = "alice", DisplayName = "E" } }, ct);
        Assert.Equal(5, await WaitingOf<Actor>(db, ct));

        Assert.Equal(5, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["E", "E", "E", "E", "E"], seen);
    }

    [Fact]
    public async Task Changed_WorksInAnInlineRule_AlongsideOtherConditions()
    {
        var ct = TestContext.Current.CancellationToken;
        var fired = new List<int>();
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Actor>(a => Changed(a.Counter) && a.Counter > 1).Do((actor, _, _) => { fired.Add(actor.Counter); return Task.CompletedTask; }),
            cancellationToken: ct);

        var alice = new Actor { Username = "alice", Counter = 1 };
        await db.SaveAsync(alice, ct);          // changed, but not more than 1
        alice.Counter = 2;
        await db.SaveAsync(alice, ct);          // fires
        alice.DisplayName = "Alice";
        await db.SaveAsync(alice, ct);          // more than 1, but not changed
        await db.ChangeAsync<Actor>("alice", a => { a.Counter = 3; return a; }, ct);   // fires

        Assert.Equal([2, 3], fired);
    }

    [Fact]
    public async Task Changed_WorksInARuleAddedToTheOpenDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        var fired = new List<int>();
        var db = await CreateDbAsync(cancellationToken: ct);
        using var rule = db.Rules.When<Actor>(a => Changed(a.Counter)).Do((actor, _, _) => { fired.Add(actor.Counter); return Task.CompletedTask; });

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);                          // new
        await db.SaveAsync(new Actor { Username = "alice", Counter = 1, DisplayName = "Alice" }, ct);   // not loaded: compared with what is stored
        await db.SaveAsync(new Actor { Username = "alice", Counter = 2, DisplayName = "Alice" }, ct);

        Assert.Equal([1, 2], fired);
    }

    [Fact]
    public async Task Changed_IsTakenToBeTrue_WhenTheRunComesUp_AndTheRestOfTheFilterIsCheckedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Actor>(a => Changed(a.DisplayName) && a.Counter > 0).DoQueued("named", (actor, _, _) => { seen.Add(actor.Username); return Task.CompletedTask; });
        }, cancellationToken: ct);

        // Queued, then saved so that the rest of the filter is false: skipped when its run comes up.
        var alice = new Actor { Username = "alice", DisplayName = "A", Counter = 1 };
        await db.SaveAsync(alice, ct);
        alice.Counter = 0;
        await db.SaveAsync(alice, ct);
        // Queued, then saved with the name as it was: at its run nothing has "changed", and it runs all the same.
        var bob = new Actor { Username = "bob", DisplayName = "B", Counter = 1 };
        await db.SaveAsync(bob, ct);
        bob.Counter = 2;
        await db.SaveAsync(bob, ct);
        Assert.Equal(2, await WaitingOf<Actor>(db, ct));

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["bob"], seen);
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
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.Rules.When<Actor>().DoQueued("flaky", (_, _, _) => ++attempts < 3 ? throw new InvalidOperationException("not yet") : Task.CompletedTask);
        }, cancellationToken: ct);

        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);   // a rule's failure is not the save's

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal((0, 0, null), await StatusOf<Actor>(db, "Actor/flaky", ct));
    }

    [Fact]
    public async Task ARuleThatFails_IsRetriedByItself_WhileTheOthersOfItsMessageAreDone()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.Rules.When<Actor>()
                .DoQueued("before", (_, _, _) => { ran.Add("before"); return Task.CompletedTask; })
                .DoQueued("flaky", (_, context, _) =>
                {
                    ran.Add($"flaky {context.Attempt}");
                    return context.Attempt < 3 ? throw new InvalidOperationException("not yet") : Task.CompletedTask;
                })
                .DoQueued("after", (_, _, _) => { ran.Add("after"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(5, await db.DrainAsync(cancellationToken: ct));
        // The failure held nothing back, and the rules that were done were not done again.
        Assert.Equal(["before", "flaky 1", "after", "flaky 2", "flaky 3"], ran);
        Assert.Equal(0, await WaitingOf<Actor>(db, ct));
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
            opts.RuleMaxAttempts = 3;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.Rules.When<Actor>().DoQueued("broken", (_, _, _) =>
            {
                runs++;
                return broken ? throw new InvalidOperationException("the model is down") : Task.CompletedTask;
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        var status = await StatusOf<Actor>(db, "Actor/broken", ct);
        Assert.Equal((0, 1), (status.Waiting, status.Poisoned));
        Assert.Contains("the model is down", status.LastError);
        Assert.Equal(1, Assert.Single((await db.Rules.StatusAsync(ct)).Types).Poisoned);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));   // set aside means left alone

        broken = false;
        Assert.Equal(1, await db.Rules.RetryAsync("broken", ct));   // by its name alone, as only one rule has it
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal((0, 0, null), await StatusOf<Actor>(db, "Actor/broken", ct));
        Assert.Equal(4, runs);
    }

    [Fact]
    public async Task ARuleSetAside_DoesNotTakeTheOtherRulesOfItsMessageWithIt_AndItsWorkCanBeDiscarded()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 2;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.Rules.When<Actor>()
                .DoQueued("broken", (_, _, _) => { ran.Add("broken"); throw new InvalidOperationException("never works"); })
                .DoQueued("flaky", (_, context, _) =>
                {
                    ran.Add("flaky");
                    return context.Attempt < 2 ? throw new InvalidOperationException("not yet") : Task.CompletedTask;
                })
                .DoQueued("fine", (_, _, _) => { ran.Add("fine"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(5, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["broken", "flaky", "fine", "broken", "flaky"], ran);
        var status = await db.Rules.StatusAsync(ct);
        Assert.Equal([("Actor/broken", 1), ("Actor/flaky", 0), ("Actor/fine", 0)], status.Rules.Select(r => (r.Rule, r.Poisoned)));
        Assert.Equal((0, 1), (status.Types[0].Waiting, status.Types[0].Poisoned));

        Assert.Equal(0, await db.Rules.DiscardAsync("Actor/fine", ct));   // nothing of that rule's is set aside
        Assert.Equal(1, await db.Rules.DiscardAsync("Actor/broken", ct));
        status = await db.Rules.StatusAsync(ct);
        Assert.Equal((0, 0), (status.Rules[0].Poisoned, status.Types[0].Poisoned));
        Assert.Equal(0, await db.Rules.RetryAsync("Actor/broken", ct));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task AFailedRun_WaitsBeforeItIsTriedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleRetryDelay = TimeSpan.FromMinutes(1);
            opts.Rules.When<Actor>().DoQueued("slow-retry", (_, _, _) => throw new InvalidOperationException("no"));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // one attempt; the next is a minute away
        var status = await StatusOf<Actor>(db, "Actor/slow-retry", ct);
        Assert.Equal((1, 0), (status.Waiting, status.Poisoned));
    }

    [Fact]
    public async Task TheAttemptsAndTheDelay_AreTheDatabases()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 2;
            opts.RuleRetryDelay = TimeSpan.FromSeconds(1);
            opts.Rules.When<Actor>().DoQueued("fails", (_, _, _) => { runs++; throw new InvalidOperationException("no"); });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // the second attempt is a second away
        var status = await StatusOf<Actor>(db, "Actor/fails", ct);
        Assert.Equal((1, 0), (status.Waiting, status.Poisoned));
        await Task.Delay(1500, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));   // two attempts allowed: set aside
        status = await StatusOf<Actor>(db, "Actor/fails", ct);
        Assert.Equal((0, 1, 2), (status.Waiting, status.Poisoned, runs));
    }

    [Fact]
    public async Task ARunThatTakesTooLong_IsCancelled_AndCountsAsAFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 1;
            opts.RuleTimeout = TimeSpan.FromMilliseconds(200);
            opts.Rules.When<Actor>().DoQueued("stuck", (_, _, token) => Task.Delay(Timeout.Infinite, token));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        var timer = System.Diagnostics.Stopwatch.StartNew();
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(60), $"the run was stopped after {timer.Elapsed}, not its 200 ms timeout");
        var status = await StatusOf<Actor>(db, "Actor/stuck", ct);
        Assert.Equal(1, status.Poisoned);
        Assert.Contains("stopped after", status.LastError);
    }

    [Fact]
    public async Task ARunThatFindsTheWriteLockHeld_WaitsBeforeItIsTriedAgain_AndIsNotCountedAsAFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 1;
            opts.RuleRetryDelay = TimeSpan.FromSeconds(1);
            // What a handler's own save throws while another process holds the database's write lock.
            opts.Rules.When<Actor>().DoQueued("blocked", (_, _, _) =>
                ++runs <= 2 ? throw new WriteLockUnavailableException("blocked") : Task.CompletedTask);
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        // Given back with no wait, the message would be run again at once, and a drain with no
        // limit would never return; the limit here only turns that into a failure instead of a hang.
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(TimeSpan.FromSeconds(20));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: limit.Token));
        Assert.Equal((1, 0, null), await StatusOf<Actor>(db, "Actor/blocked", ct));   // one attempt allowed: a counted failure would be set aside

        // Run again after the retry delay; the second time the wait is twice as long.
        await Task.Delay(1500, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: limit.Token));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: limit.Token));
        await Task.Delay(2500, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: limit.Token));

        var status = await StatusOf<Actor>(db, "Actor/blocked", ct);
        Assert.Equal((0, 0, 3), (status.Waiting, status.Poisoned, runs));
    }

    [Fact]
    public async Task AMessageThatCannotBeRead_IsSetAside_NotRunOrLost()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }, cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        await QueuesOf<Actor>(db.DatabaseId).Queue.SendMessageAsync("not one of ours", ct);

        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        var type = Assert.Single((await db.Rules.StatusAsync(ct)).Types);
        Assert.Equal((0, 1, 1), (type.Waiting, type.Poisoned, runs));
    }

    // === Loops ===

    [Fact]
    public async Task AHandlerWritingBackToItsObject_DoesNotQueueItselfAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("increment", (actor, context, token) =>
                context.Db.ChangeAsync<Actor>(actor.Username, a => { a.Counter++; return a; }, token));
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, (await db.GetAsync<Actor>("alice", ct))!.Counter);
    }

    [Fact]
    public async Task AHandlersSaveOfItsObject_GivesTheRulesStillToComeTheNewObject_AndQueuesTheOnesAlreadyDone()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("describe", async (actor, context, token) =>
            {
                ran.Add("describe");
                await context.Db.ChangeAsync<Actor>(actor.Username, a => { a.DisplayName = "described"; return a; }, token);
            });
            opts.Rules.When<Actor>().DoQueued("tag", async (actor, context, token) =>
            {
                ran.Add($"tag sees {actor.DisplayName}");
                await context.Db.ChangeAsync<Actor>(actor.Username, a => { a.AvatarUrl = "tagged"; return a; }, token);
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        // describe's change queued nothing: "tag" was still to come in the same run, and was given the changed object.
        // tag's change queued "describe" again, which had already run; that run changed nothing, and there it ends.
        Assert.Equal(["describe", "tag sees described", "describe"], ran);
        var alice = await db.GetAsync<Actor>("alice", ct);
        Assert.Equal(("described", "tagged"), (alice!.DisplayName, alice.AvatarUrl));
        Assert.Equal(0, await WaitingOf<Actor>(db, ct));
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
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 1 }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var before = await db.GetAsync<Actor>("alice", ct);

        // A change that returns the object as it is: no write at all.
        var result = await db.ChangeAsync<Actor>("alice", a => a, ct);
        Assert.Empty(result.Changes);
        Assert.Equal(1, inline);
        Assert.Equal(before!.GetETag(), (await db.GetAsync<Actor>("alice", ct))!.GetETag());

        // Saving an object exactly as it was loaded: the save happens, but there is nothing new to do.
        await db.SaveAsync(before!, ct);

        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task AChainOfRulesLongerThanAllowed_IsSetAsideWhereItCanBeSeen()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxQueueHops = 1;
            opts.Rules.When<Note>().DoQueued("first", async (note, context, token) =>
            {
                ran.Add("first");
                await context.Db.SaveAsync(new Actor { Username = note.NoteId }, token);
            });
            opts.Rules.When<Actor>().DoQueued("second", async (actor, context, token) =>
            {
                ran.Add("second");
                await context.Db.SaveAsync(new Person { Id = actor.Username }, token);
            });
            opts.Rules.When<Person>().DoQueued("third", (_, _, _) => { ran.Add("third"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(["first", "second"], ran);
        var third = await StatusOf<Person>(db, "Person/third", ct);
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
            opts.Rules.When<Actor>().DoQueued("nested", async (_, context, token) =>
            {
                try
                {
                    await context.Db.DrainAsync(cancellationToken: token);
                }
                catch (Exception e)
                {
                    fromHandler = e;
                }
            });
        }, cancellationToken: ct);

        foreach (var name in new[] { "a", "b", "c" })
            await db.SaveAsync(new Actor { Username = name }, ct);

        Assert.Equal(2, await db.DrainAsync(max: 2, cancellationToken: ct));
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.IsType<InvalidOperationException>(fromHandler);
    }

    [Fact]
    public async Task Drain_WithALimit_CanStopPartWayThroughAnObjectsRules_AndTheRestStayReady()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            foreach (var name in new[] { "one", "two", "three" })
                opts.Rules.When<Actor>().DoQueued(name, (_, _, _) => { ran.Add(name); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal(2, await db.DrainAsync(max: 2, cancellationToken: ct));
        Assert.Equal(["one", "two"], ran);
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["one", "two", "three"], ran);
    }

    [Fact]
    public async Task TwoRunsForOneObject_NeverOverlap()
    {
        var ct = TestContext.Current.CancellationToken;
        var inside = 0;
        var overlaps = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 4;
            opts.Rules.When<Actor>().DoQueued("slow", async (_, _, token) =>
            {
                if (Interlocked.Increment(ref inside) > 1)
                    Interlocked.Increment(ref overlaps);
                await Task.Delay(50, token);
                Interlocked.Decrement(ref inside);
            });
        }, cancellationToken: ct);

        // Four messages for one object, and room to run four at once.
        for (var n = 1; n <= 4; n++)
            await db.SaveAsync(new Actor { Username = "alice", Counter = n }, ct);

        Assert.Equal(4, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, overlaps);
    }

    [Fact]
    public async Task MaxConcurrency_RunsThatManyObjectsAtOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var inside = 0;
        var together = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 2;
            opts.RuleMaxAttempts = 1;
            // Each run waits for the other to have started: one at a time, neither would finish.
            opts.Rules.When<Actor>().DoQueued("meet", async (_, _, token) =>
            {
                if (Interlocked.Increment(ref inside) == 2)
                    together.TrySetResult();
                await together.Task.WaitAsync(TimeSpan.FromSeconds(20), token);
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob" }, ct);

        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal((0, 0, null), await StatusOf<Actor>(db, "Actor/meet", ct));
    }

    [Fact]
    public async Task Status_ShowsARunInProgress()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("slow", async (_, _, token) =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(token);
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        var drain = db.DrainAsync(cancellationToken: ct);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);

        var during = Assert.Single((await db.Rules.StatusAsync(ct)).Rules);
        Assert.Equal(("Actor/slow", 1, false), (during.Rule, during.Running, during.Orphaned));

        release.SetResult();
        Assert.Equal(1, await drain.WaitAsync(TimeSpan.FromSeconds(20), ct));
        Assert.Equal(0, Assert.Single((await db.Rules.StatusAsync(ct)).Rules).Running);
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
            opts.RuleMaxAttempts = 1;   // a counted failure would be set aside
            opts.Rules.When<Actor>().DoQueued("long", async (_, _, token) =>
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
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        var drain = db.DrainAsync(cancellationToken: ct);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);

        // A drain's run must stop when the database closes, not only when the drain's own token is cancelled.
        db.Dispose();

        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain);
        // The queues belong to the catalog, which is still open.
        Assert.Equal((1, 0, null), await StatusOf<Actor>(db, "Actor/long", ct));
    }

    [Fact]
    public async Task ClosingTheDatabase_DuringTheLastRunADrainAllows_StillThrows()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 1;
            opts.Rules.When<Actor>().DoQueued("long", async (_, _, token) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, token);
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        var drain = db.DrainAsync(1, ct);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
        db.Dispose();

        // An interrupted run is no run: the drain must not return 1 as if it had been done.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain.WaitAsync(TimeSpan.FromSeconds(20), ct));
        Assert.Equal((1, 0, null), await StatusOf<Actor>(db, "Actor/long", ct));
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
                .DoQueued("long-notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "short", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "long", Content = "a longer note" }, ct);

        Assert.Equal(1, await WaitingOf<Note>(db, ct));   // only the long one was queued
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["long"], ran);
    }

    [Fact]
    public async Task When_SeveralRulesWithFilters_OneMessageListsOnlyTheRulesTheSaveMatched()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>(note => note.Content.Length > 5)
                .DoQueued("long-notes", (note, _, _) => { ran.Add($"long:{note.NoteId}"); return Task.CompletedTask; });
            opts.Rules.When<Note>(note => note.AuthorId == "alice")
                .DoQueued("by-alice", (note, _, _) => { ran.Add($"alice:{note.NoteId}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a longer note", AuthorId = "bob" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Content = "hi", AuthorId = "alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n3", Content = "a longer note", AuthorId = "alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n4", Content = "hi", AuthorId = "bob" }, ct);

        Assert.Equal(3, await WaitingOf<Note>(db, ct));
        Assert.Equal(4, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["long:n1", "alice:n2", "long:n3", "alice:n3"], ran);
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
                .DoQueued("long-notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a longer note" }, ct);   // queued
        await db.SaveAsync(new Note { NoteId = "n1", Content = "hi" }, ct);              // not queued, and the queued run no longer applies

        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Empty(ran);
        Assert.Equal((0, 0, null), await StatusOf<Note>(db, "Note/long-notes", ct));
    }

    [Fact]
    public async Task When_AFilterThatThrowsOnSave_QueuesNothing_AndTheSaveStillHappens()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => Picky(note))
                .DoQueued("picky", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "n1", Content = "boom" }, ct);

        var error = Assert.Single(result.Errors);
        Assert.Contains("bad filter", error.Message);
        Assert.Contains("Note/picky", error.Message);
        Assert.NotNull(await db.GetAsync<Note>("n1", ct));
        Assert.Equal(0, await WaitingOf<Note>(db, ct));
    }

    // === Backfill ===

    [Fact]
    public async Task Backfill_QueuesOnlyTheObjectsTheFilterIsTrueFor()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => note.Content.Length > 5)
                .DoQueued("long-notes", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "a", Content = "a longer note" }, ct);
        await db.SaveAsync(new Note { NoteId = "b", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "c", Content = "another long one" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        Assert.Equal(2, await db.Rules.BackfillAsync("long-notes", ct));
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["a", "c"], ran.Order());
    }

    /// <summary>A queued rule for notes with <paramref name="filter"/>, the notes saved and drained, then a backfill: which notes ran, and how they were found.</summary>
    private async Task<(int Queued, List<string> Ran, string? Path)> BackfillWithAsync(
        System.Linq.Expressions.Expression<Func<Note, bool>> filter, Note[] notes, CancellationToken ct, string testName)
    {
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(filter).DoQueued("redo", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct, testName: testName);
        await db.SaveManyAsync(notes, ct);
        await db.DrainAsync(cancellationToken: ct);
        db.ReloadSearcher();
        ran.Clear();

        var queued = await db.Rules.BackfillAsync("Note/redo", ct);
        await db.DrainAsync(cancellationToken: ct);
        return (queued, [.. ran.Order()], db.LastBackfillPath);
    }

    [Fact]
    public async Task Backfill_WithAFilterTheIndexCanAnswer_UsesTheIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        var (queued, ran, path) = await BackfillWithAsync(n => n.AuthorId == "alice",
            [new Note { NoteId = "a1", AuthorId = "alice" }, new Note { NoteId = "b1", AuthorId = "bob" }, new Note { NoteId = "a2", AuthorId = "alice" }],
            ct, nameof(Backfill_WithAFilterTheIndexCanAnswer_UsesTheIndex));

        Assert.Equal((2, "index"), (queued, path));
        Assert.Equal(["a1", "a2"], ran);
    }

    [Fact]
    public async Task Backfill_WithAFilterComparingTwoFields_ReadsEveryObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var (queued, ran, path) = await BackfillWithAsync(n => n.AuthorId == n.Domain,
            [new Note { NoteId = "same", AuthorId = "x", Domain = "x" }, new Note { NoteId = "differ", AuthorId = "x", Domain = "y" }],
            ct, nameof(Backfill_WithAFilterComparingTwoFields_ReadsEveryObject));

        Assert.Equal((1, "scan"), (queued, path));
        Assert.Equal(["same"], ran);
    }

    [Fact]
    public async Task Backfill_WhenTheIndexFindsMoreThanTheFilterIsTrueFor_QueuesOnlyTrueMatches()
    {
        var ct = TestContext.Current.CancellationToken;
        // Content is analyzed: the index matches the word "hello" in any note that has it.
        var (queued, ran, path) = await BackfillWithAsync(n => n.Content == "hello",
            [new Note { NoteId = "exact", Content = "hello" }, new Note { NoteId = "longer", Content = "hello world" }, new Note { NoteId = "other", Content = "bye" }],
            ct, nameof(Backfill_WhenTheIndexFindsMoreThanTheFilterIsTrueFor_QueuesOnlyTrueMatches));

        Assert.Equal((1, "index"), (queued, path));
        Assert.Equal(["exact"], ran);
    }

    [Fact]
    public async Task Backfill_TakesChangedToBeTrue_AndAsksTheIndexForTheRest()
    {
        var ct = TestContext.Current.CancellationToken;
        var (queued, ran, path) = await BackfillWithAsync(n => Changed(n.Content) && n.AuthorId == "alice",
            [new Note { NoteId = "a1", AuthorId = "alice" }, new Note { NoteId = "b1", AuthorId = "bob" }],
            ct, nameof(Backfill_TakesChangedToBeTrue_AndAsksTheIndexForTheRest));

        Assert.Equal((1, "index"), (queued, path));
        Assert.Equal(["a1"], ran);
    }

    [Fact]
    public async Task Backfill_OfARuleForChangesOnly_QueuesEveryObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var (queued, ran, _) = await BackfillWithAsync(n => Changed(n.Content),
            [new Note { NoteId = "a1" }, new Note { NoteId = "b1" }],
            ct, nameof(Backfill_OfARuleForChangesOnly_QueuesEveryObject));

        Assert.Equal(2, queued);
        Assert.Equal(["a1", "b1"], ran);
    }

    [Fact]
    public async Task Backfill_QueuesTheRuleForEveryStoredObjectOfItsType_AndOnlyThatRule()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>()
                .DoQueued("redo", (actor, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; })
                .DoQueued("other", (actor, _, _) => { ran.Add($"other:{actor.Username}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        foreach (var name in new[] { "a", "b", "c" })
            await db.SaveAsync(new Actor { Username = name }, ct);
        await db.SaveAsync(new Note { NoteId = "not an actor" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        Assert.Equal(3, await db.Rules.BackfillAsync("redo", ct));
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
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob" }, ct);
        Assert.Equal(2, await WaitingOf<Actor>(db, ct));

        await db.ResetDatabaseAsync(ct);

        Assert.Equal(0, await WaitingOf<Actor>(db, ct));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, runs);
    }

    // === Opening ===

    [Fact]
    public async Task AnOpenThatFails_KeepsNothing_SoTheNextOpenRunsTheRules()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var inner = Catalog.QueueServiceClientFactory!;
        Catalog.QueueServiceClientFactory = () => new FailsOnceQueueServiceClient(inner());
        void Configure(ILottaConfiguration opts)
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }

        // Fails while looking for the rules' queues, the last step of opening.
        await Assert.ThrowsAsync<RequestFailedException>(() => CreateDbAsync(Configure, cancellationToken: ct));

        // A half-open database kept and handed out again would save, queue nothing and say nothing.
        var db = await CreateDbAsync(Configure, cancellationToken: ct);
        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));
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
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => Task.CompletedTask);
        }, ct);

        var both = await Task.WhenAll(Open(), Open());
        Assert.Same(both[0], both[1]);
        Assert.Same(both[0], await Open());
        Assert.Single((await both[0].Rules.StatusAsync(ct)).Rules);
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
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => Task.CompletedTask);
        }
        using var first = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var gaveUp = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var a = Task.Run(() => Catalog.GetDatabaseAsync("shared", Configure, first.Token), ct);
        await hold.Reached.WaitAsync(TimeSpan.FromSeconds(20), ct);
        // Each on its own thread, so that joining an open cannot block the test until it finishes.
        var b = Task.Run(() => Catalog.GetDatabaseAsync("shared", Configure, ct), ct);
        var c = Task.Run(() => Catalog.GetDatabaseAsync("shared", Configure, gaveUp.Token), ct);

        // Each caller waits with its own token: one who gives up stops waiting, and the starter
        // giving up does not fail the open for the others.
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
        Assert.Equal(1, await WaitingOf<Actor>(db, ct));
    }

    [Fact]
    public async Task DisposingTheCatalogDuringAnOpen_KeepsAndHandsOutNoDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var hold = new OpenHold(Catalog);
        // Auto, so a database kept would have its rules running.
        static void Configure(ILottaConfiguration opts)
        {
            opts.Store<Actor>();
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => Task.CompletedTask);
        }

        var opening = Task.Run(() => Catalog.GetDatabaseAsync("closing", Configure, ct), ct);
        await hold.Reached.WaitAsync(TimeSpan.FromSeconds(20), ct);
        Catalog.Dispose();
        hold.Release();

        // The open must not go on to keep the database in the disposed catalog and hand it out
        // live, its rules running and its index open, with nothing left to dispose it.
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
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => { runs++; return Task.CompletedTask; });
        }

        var opening = Task.Run(() => Catalog.GetDatabaseAsync("deleted", Configure, ct), ct);
        await hold.Reached.WaitAsync(TimeSpan.FromSeconds(20), ct);
        // Deletes the manifest the open has already written.
        var deleting = Catalog.DeleteAsync(ct);
        hold.Release();
        await deleting.WaitAsync(TimeSpan.FromSeconds(20), ct);

        // The open must not keep a database whose manifest the deletion removed.
        await Assert.ThrowsAsync<InvalidOperationException>(() => opening.WaitAsync(TimeSpan.FromSeconds(20), ct));
        var db = await Catalog.GetDatabaseAsync("deleted", Configure, ct);
        Assert.Contains("deleted", await Catalog.ListAsync(ct));
        var saved = await db.SaveAsync(new Actor { Username = "alice" }, ct);
        Assert.Empty(saved.Errors);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task QueuesLeftByAnEarlierRunOfTheProgram_AreFoundWhenTheDatabaseOpens()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        // What an earlier run of the program left: work for actors, which still have a rule, and
        // for notes, which no longer have one.
        async Task LeaveAsync<T>(string key, string rule)
        {
            var (queue, _) = QueuesOf<T>("found");
            await queue.CreateIfNotExistsAsync(
                new Dictionary<string, string> { [RuleQueueNames.TypeMetadata] = Uri.EscapeDataString(typeof(T).FullName!) }, ct);
            await queue.SendMessageAsync(new RuleMessage { Key = key, Rules = [rule] }.ToText(), ct);
        }
        await LeaveAsync<Actor>("alice", "Actor/count");
        await LeaveAsync<Note>("n1", "Note/gone");

        var db = await Catalog.GetDatabaseAsync("found", opts =>
        {
            opts.Store<Actor>();
            opts.Store<Note>();
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Actor>().DoQueued("count", (actor, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; });
        }, ct);

        var types = (await db.Rules.StatusAsync(ct)).Types.Select(t => (t.Type, t.Waiting, t.Orphaned)).ToList();
        Assert.Equal([(typeof(Actor).FullName!, 1, false), (typeof(Note).FullName!, 1, true)], types);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1" }, ct);
        // The work that was left and the save's own, for the actor; a type without rules is not run.
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["alice", "alice"], ran);
        Assert.Equal(1, await WaitingOf<Note>(db, ct));
    }

    /// <summary>
    /// Holds the catalog's next open at its last step, finding the rules' queues, until
    /// <see cref="Release"/>: the queue storage's first listing of queues waits for it.
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
            public override QueueClient GetQueueClient(string queueName) => inner.GetQueueClient(queueName);

            public override AsyncPageable<Azure.Storage.Queues.Models.QueueItem> GetQueuesAsync(
                Azure.Storage.Queues.Models.QueueTraits traits = default, string? prefix = null, CancellationToken cancellationToken = default)
            {
                if (Interlocked.Exchange(ref hold._taken, 1) == 0)
                {
                    hold._reached.TrySetResult();
                    hold._released.Wait();
                }
                return inner.GetQueuesAsync(traits, prefix, cancellationToken);
            }
        }
    }

    /// <summary>Queue storage whose first listing of queues fails, as a transient error would.</summary>
    private sealed class FailsOnceQueueServiceClient(QueueServiceClient inner) : QueueServiceClient, IDisposable
    {
        private int _calls;

        public override QueueClient GetQueueClient(string queueName) => inner.GetQueueClient(queueName);

        public override AsyncPageable<Azure.Storage.Queues.Models.QueueItem> GetQueuesAsync(
            Azure.Storage.Queues.Models.QueueTraits traits = default, string? prefix = null, CancellationToken cancellationToken = default) =>
            Interlocked.Increment(ref _calls) == 1
                ? throw new RequestFailedException(503, "Queue storage is unavailable.")
                : inner.GetQueuesAsync(traits, prefix, cancellationToken);

        public void Dispose() => (inner as IDisposable)?.Dispose();
    }

    // === Rules ===

    private readonly ConcurrentQueue<string> _namedRuns = new();

    private Task Summarize(Note note, RuleContext context, CancellationToken cancellationToken)
    {
        _namedRuns.Enqueue($"{context.Rule}:{note.NoteId}");
        return Task.CompletedTask;
    }

    private sealed class NoteHandlers(ConcurrentQueue<string> runs)
    {
        public Task NotifyFollowersAsync(Note note, RuleContext context, CancellationToken cancellationToken)
        {
            runs.Enqueue($"{context.Rule}:{note.NoteId}");
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task ARule_IsNamedAfterItsHandlerMethod_AndItsType()
    {
        var ct = TestContext.Current.CancellationToken;
        var handlers = new NoteHandlers(_namedRuns);
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().ModifyQueued(Summarize).DoQueued(handlers.NotifyFollowersAsync);
        }, cancellationToken: ct);
        _namedRuns.Clear();

        await db.SaveAsync(new Note { NoteId = "n1" }, ct);

        var status = await db.Rules.StatusAsync(ct);
        Assert.Equal(["Note/summarize", "Note/notify-followers"], status.Rules.Select(r => r.Rule));
        Assert.Equal((typeof(Note).FullName, 1), (Assert.Single(status.Types).Type, status.Types[0].Waiting));
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["summarize:n1", "notify-followers:n1"], _namedRuns);
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
                .Do((note, context, _) => { ran.Add($"inline:{note.NoteId}:{context.Rule}:{context.Attempt}"); return Task.CompletedTask; })
                .DoQueued("later", (note, context, _) => { ran.Add($"queued:{note.NoteId}:{context.Rule}:{context.Attempt}"); return Task.CompletedTask; })
                .Do((note, _, _) => { ran.Add($"inline2:{note.NoteId}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "short", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "long", Content = "a longer note" }, ct);
        Assert.Equal(["inline:long::1", "inline2:long"], ran);   // inline, in the save, in order, and with no name

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["inline:long::1", "inline2:long", "queued:long:later:1"], ran);
    }

    [Fact]
    public async Task ARuleChangedAfterTheDatabaseOpened_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        RuleBuilder<Actor>? rule = null;
        await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            rule = opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);

        // Read when the database opened: a change now would silently do nothing.
        Assert.Throws<InvalidOperationException>(() => rule!.Do((_, _, _) => Task.CompletedTask));
        Assert.Throws<InvalidOperationException>(() => rule!.DoQueued("more", (_, _, _) => Task.CompletedTask));
        Assert.Throws<InvalidOperationException>(() => rule!.ModifyQueued("other", (_, _, _) => Task.CompletedTask));
    }

    [Fact]
    public async Task ARule_IsNamedInFull_OrByItsNameAloneWhenOnlyOneRuleHasIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("count", (_, _, _) => Task.CompletedTask).DoQueued("only-actors", (_, _, _) => Task.CompletedTask);
            opts.Rules.When<Note>().DoQueued("count", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);

        Assert.Equal(0, await db.Rules.RetryAsync("only-actors", ct));
        Assert.Equal(0, await db.Rules.RetryAsync("Note/count", ct));
        var several = await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.RetryAsync("count", ct));
        Assert.Contains("Actor/count, Note/count", several.Message);
        await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.DiscardAsync("no-such-rule", ct));
        await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.BackfillAsync("no-such-rule", ct));
        await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.BackfillAsync("Actor/no-such-rule", ct));
    }

    // === Renamed and removed rules ===

    [Fact]
    public async Task AMessageNamingARuleThatIsGone_RunsAllOfTheTypesRules()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>()
                .DoQueued("first", (actor, _, _) => { ran.Add($"first:{actor.Username}"); return Task.CompletedTask; })
                .DoQueued("second", (actor, _, _) => { ran.Add($"second:{actor.Username}"); return Task.CompletedTask; });
        }, cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        // As a rule renamed between two runs of the program leaves it: work waiting under the old name.
        await QueuesOf<Actor>(db.DatabaseId).Queue.SendMessageAsync(new RuleMessage { Key = "alice", Rules = ["Actor/old-name"] }.ToText(), ct);

        // Which rule it was is not known any more; rules are safe to run twice, so all of them run and no work is lost.
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["first:alice", "second:alice"], ran);
    }

    [Fact]
    public async Task WorkSetAsideForARuleThatIsGone_IsReportedAsOrphaned_AndCanBeRetriedOrDiscarded()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new ConcurrentBag<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Actor>().DoQueued("new-name", (actor, _, _) => { ran.Add(actor.Username); return Task.CompletedTask; });
        }, cancellationToken: ct);
        await db.SaveManyAsync(new[] { new Actor { Username = "alice" }, new Actor { Username = "bob" } }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        var poison = QueuesOf<Actor>(db.DatabaseId).Poison;
        Task SetAsideAsync(string key) => poison.SendMessageAsync(
            new PoisonRecord { Type = typeof(Actor).FullName!, Key = key, Rule = "Actor/old-name", Error = "failed", Attempts = 5 }.ToText(), ct);
        await SetAsideAsync("alice");
        await SetAsideAsync("bob");

        var rules = (await db.Rules.StatusAsync(ct)).Rules.Select(r => (r.Rule, r.Poisoned, r.Orphaned)).ToList();
        Assert.Equal([("Actor/new-name", 0, false), ("Actor/old-name", 2, true)], rules);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));   // never run by itself

        // An orphaned rule is named in full; a backfill needs a rule that exists.
        await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.RetryAsync("old-name", ct));
        await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.BackfillAsync("Actor/old-name", ct));

        // Retried, the work is run by the type's rules as they are now.
        Assert.Equal(2, await db.Rules.RetryAsync("Actor/old-name", ct));
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["alice", "bob"], ran.Order());
        Assert.Single((await db.Rules.StatusAsync(ct)).Rules);

        await SetAsideAsync("alice");
        Assert.Equal(1, await db.Rules.DiscardAsync("Actor/old-name", ct));
        Assert.Single((await db.Rules.StatusAsync(ct)).Rules);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    // === Deletes ===

    private static Task RemoveFromSearch(Note note, RuleContext context, CancellationToken ct) => Task.CompletedTask;

    /// <summary>How many deleted objects are kept aside for rules still to run.</summary>
    private async Task<int> TombstonesOf(LottaDB db, CancellationToken ct)
    {
        var table = Catalog.GetTableServiceClient().GetTableClient(Catalog.Name);
        var count = 0;
        await foreach (var _ in table.QueryAsync<global::Azure.Data.Tables.TableEntity>($"PartitionKey eq '{db.DatabaseId}~deleted~'", cancellationToken: ct))
            count++;
        return count;
    }

    [Fact]
    public async Task WhenDeleted_DoQueued_RunsLater_WithTheObjectAsItWas()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.WhenDeleted<Note>(note => note.Domain == "public")
                .DoQueued("unpublish", (note, context, _) =>
                {
                    ran.Add($"{note.NoteId}:{note.GetKey()}:{note.Content}:{context.Rule}:{context.Attempt}");
                    return Task.CompletedTask;
                });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Domain = "public", Content = "hello" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Domain = "private", Content = "secret" }, ct);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));   // a save queues nothing for a rule of deletes

        Assert.Empty((await db.DeleteAsync<Note>("n1", ct)).Errors);
        await db.DeleteAsync<Note>("n2", ct);                          // the filter is false for it
        Assert.Empty(ran);                                             // nothing runs inside the delete
        Assert.Null(await db.GetAsync<Note>("n1", ct));
        var left = 0;
        await foreach (var _ in db.GetManyAsync<Note>(cancellationToken: ct))
            left++;
        Assert.Equal(0, left);                                         // the tombstone is no object of the database
        Assert.Equal(1, await TombstonesOf(db, ct));

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["n1:n1:hello:unpublish:1"], ran);                // under its own key, not the tombstone's
        Assert.Equal(0, await TombstonesOf(db, ct));                   // the last rule to run removed it

        // Deleted by key alone, with no type given.
        await db.SaveAsync(new Note { NoteId = "n3", Domain = "public", Content = "again" }, ct);
        await db.DeleteAsync("n3", ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal("n3:n3:again:unpublish:1", ran[^1]);
    }

    [Fact]
    public async Task WhenDeleted_DoQueued_TakesALargeObject_AndAKeySavedAgainBeforeItRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Store<LargeDocument>();
            opts.Rules.WhenDeleted<LargeDocument>().DoQueued("unpublish", (document, _, _) =>
            {
                ran.Add($"{document.Id}:{document.GetKey()}:{document.Payload.Length}:{document.Title}");
                return Task.CompletedTask;
            });
        }, cancellationToken: ct);

        // Far more than a queue message holds.
        await db.SaveAsync(new LargeDocument { Id = "big", Title = "old", Payload = new string('x', 300_000) }, ct);
        Assert.Empty((await db.DeleteAsync<LargeDocument>("big", ct)).Errors);
        // The key is free at once, and the rule still gets the object that was deleted.
        await db.SaveAsync(new LargeDocument { Id = "big", Title = "new", Payload = "short" }, ct);

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["big:big:300000:old"], ran);
        Assert.Equal("new", (await db.GetAsync<LargeDocument>("big", ct))!.Title);
        Assert.Equal(0, await TombstonesOf(db, ct));
    }

    [Fact]
    public async Task WhenDeleted_DoQueued_OneMessageForAllItsRules_AndARuleSetAsideKeepsTheObjectForItsRetry()
    {
        var ct = TestContext.Current.CancellationToken;
        var fail = true;
        var seen = new List<string>();
        var keys = new HashSet<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.RuleMaxAttempts = 2;
            opts.Rules.WhenDeleted<Note>()
                .DoQueued("unpublish", (note, context, _) =>
                {
                    seen.Add($"unpublish:{note.Content}:{context.Attempt}");
                    keys.Add(context.IdempotencyKey);
                    return fail ? throw new InvalidOperationException("service down") : Task.CompletedTask;
                })
                .DoQueued("audit", (note, _, _) => { seen.Add($"audit:{note.Content}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "hello" }, ct);
        await db.DeleteAsync<Note>("n1", ct);
        Assert.Equal(1, await WaitingOf<Note>(db, ct));   // one message, whatever number of rules
        Assert.Equal(1, await TombstonesOf(db, ct));

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["unpublish:hello:1", "audit:hello", "unpublish:hello:2"], seen);   // the rule that worked is not run again
        var status = await StatusOf<Note>(db, "Note/unpublish", ct);
        Assert.Equal((0, 1, "service down"), (status.Waiting, status.Poisoned, status.LastError));
        Assert.Equal(1, await TombstonesOf(db, ct));      // kept for the rule that was set aside

        fail = false;
        Assert.Equal(1, await db.Rules.RetryAsync("unpublish", ct));
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal("unpublish:hello:1", seen[^1]);
        Assert.Equal(0, (await StatusOf<Note>(db, "Note/unpublish", ct)).Poisoned);
        Assert.Single(keys);                              // the same work, every time
        Assert.Equal(0, await TombstonesOf(db, ct));
    }

    [Fact]
    public async Task WhenDeleted_DoQueued_DiscardingItsSetAsideWork_RemovesTheObjectKeptForIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.RuleMaxAttempts = 1;
            opts.Rules.WhenDeleted<Note>().DoQueued("unpublish", (_, _, _) => throw new InvalidOperationException("never"));
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1" }, ct);
        await db.DeleteAsync<Note>("n1", ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, await TombstonesOf(db, ct));

        Assert.Equal(1, await db.Rules.DiscardAsync("unpublish", ct));
        Assert.Equal(0, await TombstonesOf(db, ct));
    }

    [Fact]
    public async Task WhenDeleted_DoQueued_IsNamedAfterItsMethod_AndCannotBeBackfilled()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.WhenDeleted<Note>().DoQueued(RemoveFromSearch);
            opts.Rules.When<Note>().DoQueued("index", (_, _, _) => Task.CompletedTask);
        }, cancellationToken: ct);

        Assert.Contains((await db.Rules.StatusAsync(ct)).Rules, r => r.Rule == "Note/remove-from-search");
        await Assert.ThrowsAsync<ArgumentException>(() => db.Rules.BackfillAsync("remove-from-search", ct));

        // Each kind of message runs only its own kind of rule.
        await db.SaveAsync(new Note { NoteId = "n1" }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        await db.DeleteAsync<Note>("n1", ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
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
    public async Task Rules_RunByThemselves_SoonAfterTheSave()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Actor>().DoQueued("greet", async (actor, context, token) =>
            {
                // A save from inside the handler works: the handler is not inside anyone's write.
                await context.Db.SaveAsync(new Note { NoteId = $"hello-{actor.Username}" }, token);
                seen.TrySetResult(actor.Username);
            }), cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        Assert.Equal("alice", await seen.Task.WaitAsync(Patience, ct));
        Assert.NotNull(await db.GetAsync<Note>("hello-alice", ct));
    }

    [Fact]
    public async Task AModifyRule_ChangesTheObject_ByItself_AndTheRuleAfterItSeesTheChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Actor>()
                .ModifyQueued("describe", (actor, _, _) => { actor.DisplayName = $"#{actor.Counter}"; return Task.CompletedTask; })
                .DoQueued("notify", (actor, _, _) => { seen.TrySetResult(actor.DisplayName); return Task.CompletedTask; }),
            cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", Counter = 7 }, ct);

        Assert.Equal("#7", await seen.Task.WaitAsync(Patience, ct));
        Assert.Equal("#7", (await db.GetAsync<Actor>("alice", ct))!.DisplayName);
    }

    [Fact]
    public async Task AFailedRun_IsTriedAgainByItself_OnceItsWaitIsOver()
    {
        var ct = TestContext.Current.CancellationToken;
        var attempts = new ConcurrentQueue<int>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.RuleRetryDelay = TimeSpan.FromMilliseconds(200);
            opts.Rules.When<Actor>().DoQueued("flaky", (_, context, _) =>
            {
                attempts.Enqueue(context.Attempt);
                if (context.Attempt < 2)
                    throw new InvalidOperationException("not yet");
                done.TrySetResult();
                return Task.CompletedTask;
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        await done.Task.WaitAsync(Patience, ct);
        Assert.Equal([1, 2], attempts);
    }

    [Fact]
    public async Task ClosingTheDatabase_CancelsARunInProgress()
    {
        var ct = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Actor>().DoQueued("long", async (_, _, token) =>
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
