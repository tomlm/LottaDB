using Lotta.Internal;

namespace Lotta.Tests;

/// <summary>
/// Tests for delayed rules: <c>When&lt;T&gt;().After(delay)</c> and <c>.At(item =&gt; time)</c>. The
/// delays are real, and short. Queued work is run by hand with <see cref="LottaDB.DrainAsync"/>.
/// </summary>
public abstract class TimedRuleTestBase : LottaTestBase
{
    protected TimedRuleTestBase(Action<LottaCatalog> config) : base(config) { }

    /// <summary>Drains again and again until <paramref name="done"/> is true, for at most a minute: a delayed rule's time comes when it comes.</summary>
    private static async Task DrainUntil(LottaDB db, Func<bool> done, CancellationToken ct)
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            await db.DrainAsync(cancellationToken: ct);
            if (done())
                return;
            Assert.True(waited.Elapsed < TimeSpan.FromMinutes(1), "The delayed work did not run within a minute.");
            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
        }
    }

    private static async Task<int> WaitingOf<T>(LottaDB db, CancellationToken ct) =>
        (await db.Rules.StatusAsync(ct)).Types.SingleOrDefault(t => t.Type == typeof(T).FullName)?.Waiting ?? 0;

    [Fact]
    public async Task After_RunsTheRuleOnceTheDelayHasPassed_NotBefore()
    {
        var ct = TestContext.Current.CancellationToken;
        var delay = TimeSpan.FromSeconds(2);
        var ran = new List<string>();
        var later = new List<(string Content, DateTimeOffset At)>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>()
                .Then("now", (note, _, _) => { ran.Add($"now:{note.NoteId}"); return Task.CompletedTask; })
                .After(delay)
                .Then("later", (note, _, _) => { later.Add((note.Content, DateTimeOffset.UtcNow)); return Task.CompletedTask; })
                .After(TimeSpan.FromHours(1))
                .Then("much-later", (note, _, _) => { ran.Add("much-later"); return Task.CompletedTask; });
        }, cancellationToken: ct);

        var saved = DateTimeOffset.UtcNow;
        await db.SaveAsync(new Note { NoteId = "n1", Content = "first" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", Content = "edited" }, ct);

        await DrainUntil(db, () => later.Count >= 2, ct);

        // The rule before After ran at once, for each save. The ones after it each had a message of their own.
        Assert.Equal(["now:n1", "now:n1"], ran);
        // Each save started a wait of its own, and each run sees the note as it is when its time comes.
        Assert.Equal(["edited", "edited"], later.Select(run => run.Content));
        Assert.All(later, run => Assert.True(run.At >= saved + delay, $"ran {run.At - saved} after the save, before the {delay} delay had passed"));
        // The hour-long waits are still waiting.
        Assert.Equal(2, await WaitingOf<Note>(db, ct));
    }

    [Fact]
    public async Task After_ChecksTheFilterAgainWhenTheTimeComes()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Note>(note => note.Domain == "pending")
                .After(TimeSpan.FromSeconds(1))
                .Then("remind", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Domain = "pending" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", Domain = "done" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Domain = "pending" }, ct);

        // n1's wait began first, so by the time n2's has run, n1's has come up and been dropped.
        await DrainUntil(db, () => ran.Count > 0, ct);
        Assert.Equal(["n2"], ran);
        Assert.Equal(0, await WaitingOf<Note>(db, ct));
    }

    [Fact]
    public async Task At_RunsAtTheTimeTheObjectGives_AndWaitsWhenItNowGivesALaterOne()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<(string Username, DateTimeOffset At)>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.MaxConcurrency = 1;
            opts.Rules.When<Actor>()
                .At(actor => actor.LastSeenAt == default ? null : actor.LastSeenAt)
                .Then("expire", (actor, _, _) => { ran.Add((actor.Username, DateTimeOffset.UtcNow)); return Task.CompletedTask; });
        }, cancellationToken: ct);

        // No time: nothing is queued. A time already past: as soon as it can be.
        await db.SaveAsync(new Actor { Username = "never" }, ct);
        Assert.Equal(0, await WaitingOf<Actor>(db, ct));
        await db.SaveAsync(new Actor { Username = "past", LastSeenAt = DateTimeOffset.UtcNow.AddMinutes(-1) }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["past"], ran.Select(run => run.Username));
        ran.Clear();

        var first = DateTimeOffset.UtcNow.AddSeconds(1);
        var extended = first.AddSeconds(3);
        await db.SaveAsync(new Actor { Username = "alice", LastSeenAt = first }, ct);
        // Extended before it came up.
        await db.SaveAsync(new Actor { Username = "alice", LastSeenAt = extended }, ct);

        await DrainUntil(db, () => ran.Count >= 2, ct);

        // The message for the first time came up when that time did, found a later one, and waited for it:
        // nothing ran before the time the actor gives now.
        Assert.All(ran, run => Assert.True(run.At >= extended, $"ran {extended - run.At} before the time the actor gives"));
        Assert.Equal(0, await WaitingOf<Actor>(db, ct));
    }

    [Fact]
    public async Task AMessageThatComesUpBeforeItsTime_IsHiddenAgain_WithNothingRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>()
                .After(TimeSpan.FromDays(30))
                .Then("much-later", (note, _, _) => { ran.Add(note.NoteId); return Task.CompletedTask; });
        }, cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1" }, ct);

        // As a wait longer than a queue can hide a message for leaves it: visible, with its time still to come.
        var databaseId = string.Join("", nameof(AMessageThatComesUpBeforeItsTime_IsHiddenAgain_WithNothingRun).Where(char.IsLetterOrDigit).Take(60));
        var queue = Catalog.GetQueueServiceClient()!.GetQueueClient(RuleQueueNames.Queue(Catalog.Name, databaseId, typeof(Note)));
        var early = new RuleMessage { Key = "n1", Rules = ["Note/much-later"], Due = DateTimeOffset.UtcNow.AddHours(1) };
        await queue.SendMessageAsync(early.ToText(), ct);

        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Empty(ran);
        Assert.Equal(2, await WaitingOf<Note>(db, ct));
        // And it is hidden, not handed out again and again.
        Assert.Empty((await queue.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(1), ct)).Value);
    }
}

/// <summary>What <c>After</c> and <c>At</c> accept, and how long a wait is hidden for. Needs no database.</summary>
public class TimedRuleRegistrationTests
{
    [Fact]
    public void AfterAndAt_DelayThenAndUpdate_ButNotDoOrMaintain()
    {
        var config = new LottaConfiguration();
        static Task Nothing(Note note, RuleContext context, CancellationToken ct) => Task.CompletedTask;

        config.Rules.When<Note>().After(TimeSpan.FromDays(3)).Then("remind", Nothing).Update("tidy", Nothing);
        config.Rules.When<Note>().At(note => note.Published).Then("publish", Nothing);
        config.Rules.When<Note>().Then("at-once", Nothing);

        Assert.Equal([TimeSpan.FromDays(3), TimeSpan.FromDays(3), null, null], config.QueuedRules.Select(r => r.After));
        Assert.Equal([false, false, true, false], config.QueuedRules.Select(r => r.At != null));

        Assert.Throws<InvalidOperationException>(() => config.Rules.When<Note>().After(TimeSpan.FromDays(1)).Do(Nothing));
        Assert.Throws<InvalidOperationException>(() => config.Rules.When<Note>().At(note => note.Published).Maintain(note => new NoteView { Id = note.NoteId }));
        Assert.Throws<ArgumentOutOfRangeException>(() => config.Rules.When<Note>().After(TimeSpan.FromSeconds(-1)));
    }

    [Fact]
    public void AWaitLongerThanAQueueCanHideAMessageFor_IsMadeInSteps()
    {
        var now = DateTimeOffset.UtcNow;

        Assert.Equal(TimeSpan.FromHours(2), LottaDB.HideFor(now.AddHours(2), now));
        Assert.Equal(TimeSpan.FromDays(7), LottaDB.HideFor(now.AddDays(30), now));
        Assert.Null(LottaDB.HideFor(now, now));
        Assert.Null(LottaDB.HideFor(now.AddSeconds(-1), now));
    }
}
