namespace Lotta.Tests;

/// <summary>
/// Tests for schedules: <c>Rules.Every(...).Then(...)</c>. The intervals are real, and short. Except
/// where a test says otherwise, schedules are run by hand with <see cref="LottaDB.DrainAsync"/>.
/// </summary>
public abstract class ScheduleTestBase : LottaTestBase
{
    protected ScheduleTestBase(Action<LottaCatalog> config) : base(config) { }

    [Fact]
    public async Task Every_RunsWhenItsTimeComes_AndOnceForTheTimesItMissed()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.Every(TimeSpan.FromHours(1)).Then("tick", (context, _) =>
            {
                Assert.Equal("tick", context.Rule);
                ran++;
                return Task.CompletedTask;
            });
        }, cancellationToken: ct);
        var now = DateTimeOffset.UtcNow;
        db.ScheduleClock = () => now;

        // First seen: it runs when its time first comes round, not at once.
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        var record = await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("tick"), ct);
        Assert.Equal(now.AddHours(1), record!.NextRun);
        Assert.Null(record.LastRun);

        now = now.AddMinutes(59);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));

        now = now.AddMinutes(2);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));   // not again until the next time
        Assert.Equal(1, ran);
        record = await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("tick"), ct);
        Assert.Equal(now, record!.LastRun);
        Assert.Equal(now.AddHours(1), record.NextRun);

        // Five times pass with nothing running: made up for with one run, and the next is counted from now.
        now = now.AddHours(5);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(2, ran);
        Assert.Equal(now.AddHours(1), (await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("tick"), ct))!.NextRun);
    }

    [Fact]
    public async Task ARunThatFails_IsRecorded_AndTheWorkRunsAgainAtTheNextTime()
    {
        var ct = TestContext.Current.CancellationToken;
        var fail = true;
        var ran = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.Every(TimeSpan.FromHours(1)).Then("sync", (_, _) =>
            {
                ran++;
                return fail ? throw new InvalidOperationException("the service is down") : Task.CompletedTask;
            });
        }, cancellationToken: ct);
        var now = DateTimeOffset.UtcNow;
        db.ScheduleClock = () => now;
        await db.DrainAsync(cancellationToken: ct);

        now = now.AddMinutes(61);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal("the service is down", (await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("sync"), ct))!.LastError);
        Assert.Equal("the service is down", (await db.Rules.StatusAsync(ct)).Rules.Single(r => r.Rule == "schedule/sync").LastError);
        // Not retried before its next time.
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));

        fail = false;
        now = now.AddMinutes(61);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(2, ran);
        Assert.Null((await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("sync"), ct))!.LastError);
        Assert.Null((await db.Rules.StatusAsync(ct)).Rules.Single(r => r.Rule == "schedule/sync").LastError);
    }

    [Fact]
    public async Task ATimeClaimedElsewhere_IsNotRunHere()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.Every(TimeSpan.FromHours(1)).Then("hourly", (_, _) => { ran++; return Task.CompletedTask; });
        }, cancellationToken: ct);
        await db.DrainAsync(cancellationToken: ct);

        // As another process would leave the record after claiming a time that had come: moved on.
        await db.ChangeAsync<RuleSchedule>(RuleSchedule.KeyOf("hourly"), record => { record.NextRun = DateTimeOffset.UtcNow.AddMinutes(30); }, ct);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));

        // And a time that has come, which nobody claimed, is run.
        await db.ChangeAsync<RuleSchedule>(RuleSchedule.KeyOf("hourly"), record => { record.NextRun = DateTimeOffset.UtcNow.AddSeconds(-1); }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, ran);
        var next = (await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("hourly"), ct))!.NextRun;
        Assert.InRange(next, DateTimeOffset.UtcNow.AddMinutes(59), DateTimeOffset.UtcNow.AddMinutes(61));
    }

    [Fact]
    public async Task Every_RunsByItself_WhileTheDatabaseIsOpen()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = 0;
        var saved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.Every(TimeSpan.FromSeconds(1)).Then("beat", async (context, token) =>
            {
                // The handler has the database, like any rule's.
                await context.Db.SaveAsync(new Note { NoteId = "beat", Content = $"{++ran}" }, token);
                if (ran >= 2)
                    saved.TrySetResult();
            });
        }, cancellationToken: ct);

        await saved.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
        Assert.NotNull(await db.GetAsync<Note>("beat", ct));
    }
}

/// <summary>How schedules are registered, and when a cron expression comes round. Needs no database.</summary>
public class ScheduleRegistrationTests
{
    private static Task NightlyCleanupAsync(RuleContext context, CancellationToken ct) => Task.CompletedTask;

    [Fact]
    public void ASchedule_IsNamedAfterItsMethod_OrByHand()
    {
        var config = new LottaConfiguration();
        config.Rules.Every(TimeSpan.FromHours(24)).Then(NightlyCleanupAsync);
        config.Rules.Every("*/5 * * * *").Then("poll", (_, _) => Task.CompletedTask);

        Assert.Equal(["schedule/nightly-cleanup", "schedule/poll"], config.Schedules.Select(s => s.Id));
        Assert.Throws<ArgumentException>(() => config.Rules.Every(TimeSpan.FromHours(1)).Then("poll", (_, _) => Task.CompletedTask));
        Assert.Throws<ArgumentException>(() => config.Rules.Every(TimeSpan.FromHours(1)).Then((_, _) => Task.CompletedTask));
    }

    [Fact]
    public void Every_RejectsWhatIsNotASchedule()
    {
        var config = new LottaConfiguration();

        Assert.Throws<ArgumentOutOfRangeException>(() => config.Rules.Every(TimeSpan.Zero));
        Assert.Throws<ArgumentException>(() => config.Rules.Every("every tuesday"));
    }

    [Fact]
    public void AScheduleWithNoWork_IsCaughtWhenTheDatabaseOpens()
    {
        var config = new LottaConfiguration();
        config.Rules.Every(TimeSpan.FromHours(1));

        Assert.Contains("no work", Assert.Throws<InvalidOperationException>(config.CheckRules).Message);
    }

    [Fact]
    public void ACronExpression_ComesRoundInItsTimeZone()
    {
        var config = new LottaConfiguration();
        var pacific = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");
        config.Rules.Every("0 2 * * *", pacific).Then("nightly", (_, _) => Task.CompletedTask);
        config.Rules.Every("0 2 * * *").Then("nightly-utc", (_, _) => Task.CompletedTask);
        config.Rules.Every("30 0 2 * * *").Then("with-seconds", (_, _) => Task.CompletedTask);

        // Noon UTC on a winter day, when Pacific time is eight hours behind.
        var from = new DateTimeOffset(2026, 1, 15, 12, 0, 0, TimeSpan.Zero);
        Assert.Equal(new DateTimeOffset(2026, 1, 16, 10, 0, 0, TimeSpan.Zero), config.Schedules[0].Next(from).ToUniversalTime());
        Assert.Equal(new DateTimeOffset(2026, 1, 16, 2, 0, 0, TimeSpan.Zero), config.Schedules[1].Next(from).ToUniversalTime());
        Assert.Equal(new DateTimeOffset(2026, 1, 16, 2, 0, 30, TimeSpan.Zero), config.Schedules[2].Next(from).ToUniversalTime());
    }
}
