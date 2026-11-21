using System.Collections.Concurrent;
using Lotta.Internal;

namespace Lotta.Tests;

/// <summary>
/// Tests for queued rules with a priority or a concurrency limit of their own
/// (<see cref="QueuedRuleOptions"/>): slow work that cannot keep quick work waiting.
/// </summary>
public abstract class RulePriorityTestBase : LottaTestBase
{
    protected RulePriorityTestBase(Action<LottaCatalog> config) : base(config) { }

    private static readonly QueuedRuleOptions Low = new() { Priority = RulePriority.Low };

    private static async Task<int> WaitingOf<T>(LottaDB db, CancellationToken ct) =>
        (await db.Rules.StatusAsync(ct)).Types.SingleOrDefault(t => t.Type == typeof(T).FullName)?.Waiting ?? 0;

    [Fact]
    public async Task ALowPriorityRule_RunsAfterTheOtherRulesOfItsType_ThoughQueuedFirst()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("describe", (note, _, _) => { ran.Add($"describe:{note.NoteId}"); return Task.CompletedTask; }, Low);
            opts.Rules.When<Note>().DoQueued("people", (note, _, _) => { ran.Add($"people:{note.NoteId}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Content = "b" }, ct);
        // Two messages a save now: one for the shared queue, one for the low-priority rule's own. Status adds them up per type.
        Assert.Equal(4, await WaitingOf<Note>(db, ct));

        Assert.Equal(4, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["people:n1", "people:n2", "describe:n1", "describe:n2"], ran);
    }

    [Fact]
    public async Task ALowPriorityRule_WaitsForOtherTypesWork_SavedAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("describe", (note, _, _) => { ran.Add($"describe:{note.NoteId}"); return Task.CompletedTask; }, Low);
            opts.Rules.When<Actor>().DoQueued("embed", (actor, _, _) => { ran.Add($"embed:{actor.Username}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a" }, ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["embed:alice", "embed:bob", "describe:n1"], ran);
    }

    [Fact]
    public async Task AHighPriorityRule_RunsBeforeNormalWork()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("normal", (note, _, _) => { ran.Add($"normal:{note.NoteId}"); return Task.CompletedTask; });
            opts.Rules.When<Actor>().DoQueued("urgent", (actor, _, _) => { ran.Add($"urgent:{actor.Username}"); return Task.CompletedTask; },
                new QueuedRuleOptions { Priority = RulePriority.High });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a" }, ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["urgent:alice", "normal:n1"], ran);
    }

    [Fact]
    public async Task ARuleWithALimit_NeverRunsMoreAtOnce_AndOtherWorkUsesTheRest()
    {
        var ct = TestContext.Current.CancellationToken;
        var running = 0;
        var peak = 0;
        var finished = new ConcurrentQueue<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 4;
            opts.Rules.When<Note>().DoQueued("describe", async (note, _, token) =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref peak, now);
                await Task.Delay(150, token);
                Interlocked.Decrement(ref running);
                finished.Enqueue($"describe:{note.NoteId}");
            }, new QueuedRuleOptions { Priority = RulePriority.Low, MaxConcurrency = 2 });
            opts.Rules.When<Actor>().DoQueued("embed", (actor, _, _) => { finished.Enqueue($"embed:{actor.Username}"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        foreach (var id in new[] { "n1", "n2", "n3", "n4", "n5", "n6" })
            await db.SaveAsync(new Note { NoteId = id, Content = id }, ct);
        foreach (var name in new[] { "alice", "bob", "carol" })
            await db.SaveAsync(new Actor { Username = name }, ct);

        Assert.Equal(9, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(2, peak);
        // The quick work did not wait for the slow work to finish.
        var order = finished.ToList();
        Assert.True(order.FindLastIndex(f => f.StartsWith("embed:")) < order.FindLastIndex(f => f.StartsWith("describe:")), string.Join(", ", order));

        static void InterlockedMax(ref int target, int value)
        {
            int seen;
            while ((seen = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, seen) != seen) { }
        }
    }

    [Fact]
    public async Task SetAsideWorkOfALowPriorityRule_IsRetriedFromItsOwnQueue()
    {
        var ct = TestContext.Current.CancellationToken;
        var fail = true;
        var ran = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleMaxAttempts = 1;
            opts.Rules.When<Note>().DoQueued("describe", (_, _, _) =>
            {
                if (fail)
                    throw new InvalidOperationException("model down");
                ran++;
                return Task.CompletedTask;
            }, Low);
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "a" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var status = await db.Rules.StatusAsync(ct);
        Assert.Equal(1, status.Rules.Single(r => r.Rule == "Note/describe").Poisoned);
        Assert.Equal(1, status.Types.Single(t => t.Type == typeof(Note).FullName).Poisoned);

        fail = false;
        Assert.Equal(1, await db.Rules.RetryAsync("describe", ct));
        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(1, ran);
        Assert.Equal(0, await WaitingOf<Note>(db, ct));
    }

    [Fact]
    public async Task ABackfill_OfALowPriorityRule_WaitsBehindOtherWork()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        void Configure(ILottaConfiguration opts)
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("describe", (note, _, _) => { ran.Add($"describe:{note.NoteId}"); return Task.CompletedTask; }, Low);
            opts.Rules.When<Actor>().DoQueued("embed", (actor, _, _) => { ran.Add($"embed:{actor.Username}"); return Task.CompletedTask; });
        }
        var db = await CreateDbAsync(Configure, cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", Content = "a" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        ran.Clear();

        Assert.Equal(1, await db.Rules.BackfillAsync("describe", ct));
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal(["embed:alice", "describe:n1"], ran);
    }

    [Fact]
    public async Task WorkWaitingInARulesOwnQueue_IsRunAfterTheDatabaseIsOpenedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        // What an earlier run of the program left: work for the low-priority rule, in its own queue, and for the other rule.
        var queues = Catalog.GetQueueServiceClient()!;
        async Task LeaveAsync<T>(string key, string rule, bool own)
        {
            var name = own ? RuleQueueNames.Queue(Catalog.Name, "foundlow", typeof(T), rule) : RuleQueueNames.Queue(Catalog.Name, "foundlow", typeof(T));
            var about = new Dictionary<string, string> { [RuleQueueNames.TypeMetadata] = Uri.EscapeDataString(typeof(T).FullName!) };
            if (own)
                about[RuleQueueNames.RuleMetadata] = Uri.EscapeDataString(rule);
            var queue = queues.GetQueueClient(name);
            await queue.CreateIfNotExistsAsync(about, ct);
            await queue.SendMessageAsync(new RuleMessage { Key = key, Rules = [rule] }.ToText(), ct);
        }
        await LeaveAsync<Note>("n1", "Note/describe", own: true);
        await LeaveAsync<Actor>("alice", "Actor/embed", own: false);

        var db = await Catalog.GetDatabaseAsync("foundlow", opts =>
        {
            opts.Store<Actor>();
            opts.Store<Note>();
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>().DoQueued("describe", (note, _, _) => { ran.Add($"describe:{note.NoteId}"); return Task.CompletedTask; }, Low);
            opts.Rules.When<Actor>().DoQueued("embed", (actor, _, _) => { ran.Add($"embed:{actor.Username}"); return Task.CompletedTask; });
        }, ct);
        // The objects the work is for; saving them queues more of the same, which runs in the same order.
        Assert.Equal(1, await WaitingOf<Note>(db, ct));
        await db.SaveManyAsync([new Note { NoteId = "n1", Content = "a" }], ct);
        await db.SaveManyAsync([new Actor { Username = "alice" }], ct);
        ran.Clear();
        await db.DrainAsync(cancellationToken: ct);

        // Found again with its priority: the actor's work, left later, still runs first.
        Assert.Equal("describe:n1", ran[^1]);
        Assert.Contains("embed:alice", ran);
    }

    [Fact]
    public async Task WorkQueuedBeforeARuleHadAPriority_IsMovedToItsOwnQueue_AndKeepsItsAttempts()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        // Left in the type's shared queue by a run of the program in which "describe" had no options:
        // one message for it alone, with a failure counted, and one for it and "people" together.
        var queues = Catalog.GetQueueServiceClient()!;
        var shared = queues.GetQueueClient(RuleQueueNames.Queue(Catalog.Name, "movedlow", typeof(Note)));
        await shared.CreateIfNotExistsAsync(new Dictionary<string, string> { [RuleQueueNames.TypeMetadata] = Uri.EscapeDataString(typeof(Note).FullName!) }, ct);
        await shared.SendMessageAsync(new RuleMessage { Key = "n1", Rules = ["Note/describe"], Attempts = new() { ["Note/describe"] = 1 } }.ToText(), ct);
        await shared.SendMessageAsync(new RuleMessage { Key = "n2", Rules = ["Note/describe", "Note/people"] }.ToText(), ct);

        var db = await Catalog.GetDatabaseAsync("movedlow", opts =>
        {
            opts.Store<Note>();
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.RuleMaxAttempts = 2;
            opts.Rules.When<Note>().DoQueued("describe", (note, _, _) =>
            {
                ran.Add($"describe:{note.NoteId}");
                // Fails for n1: its second failure, so it is set aside, which shows its first was kept.
                return note.NoteId == "n1" ? throw new InvalidOperationException("model down") : Task.CompletedTask;
            }, Low);
            opts.Rules.When<Note>().DoQueued("people", (note, _, _) => { ran.Add($"people:{note.NoteId}"); return Task.CompletedTask; });
        }, ct);
        // The objects the work is for; saving them queues more of the same.
        await db.SaveManyAsync([new Note { NoteId = "n1", Content = "a" }, new Note { NoteId = "n2", Content = "b" }], ct);

        await db.DrainAsync(cancellationToken: ct);

        // Every quick run before any slow one: the work that was left in the shared queue was moved, not run there.
        Assert.True(ran.FindLastIndex(r => r.StartsWith("people:", StringComparison.Ordinal)) < ran.FindIndex(r => r.StartsWith("describe:", StringComparison.Ordinal)), string.Join(", ", ran));
        Assert.Contains("people:n2", ran);
        // n1's moved work kept its earlier failure: one more and it is set aside. (The save's own n1 work has failed once, and waits to be retried.)
        var status = await db.Rules.StatusAsync(ct);
        Assert.Equal(1, status.Rules.Single(r => r.Rule == "Note/describe").Poisoned);
    }

    [Fact]
    public void ARulesOwnQueue_AndItsPoisonQueue_FitInAQueueName()
    {
        var name = RuleQueueNames.Queue(new string('c', 63), new string('d', 63), typeof(AVeryLongTypeNameThatGoesOnAndOnForMoreThanThirtyLetters), "AVeryLongTypeNameThatGoesOnAndOnForMoreThanThirtyLetters/describe-every-message-ever");
        Assert.True(RuleQueueNames.Poison(name).Length <= 63, RuleQueueNames.Poison(name));
        Assert.NotEqual(name, RuleQueueNames.Queue(new string('c', 63), new string('d', 63), typeof(AVeryLongTypeNameThatGoesOnAndOnForMoreThanThirtyLetters), "AVeryLongTypeNameThatGoesOnAndOnForMoreThanThirtyLetters/other"));
    }

    [Fact]
    public async Task ALimitBelowOne_IsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => CreateDbAsync(opts =>
            opts.Rules.When<Note>().DoQueued("describe", (_, _, _) => Task.CompletedTask, new QueuedRuleOptions { MaxConcurrency = 0 }), cancellationToken: ct));
    }

    private sealed class AVeryLongTypeNameThatGoesOnAndOnForMoreThanThirtyLetters;
}
