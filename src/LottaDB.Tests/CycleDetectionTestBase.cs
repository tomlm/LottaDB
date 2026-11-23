using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public abstract class CycleDetectionTestBase : LottaTestBase
{
    protected CycleDetectionTestBase(Action<LottaCatalog> config) : base(config) { }
    private async Task<LottaDB> CreateCycleDbAsync(CancellationToken cancellationToken = default, [CallerMemberName] string? testName = null)
    {
        return await CreateDbAsync(opts =>
        {
            opts.Rules.When<CycleA>().Do(async (a, context, _) =>
            {
                await context.Db.SaveAsync(new CycleB { Id = $"cb-{a.Id}", Value = $"from-a-{a.Value}" });
            });

            opts.Rules.When<CycleB>().Do(async (b, context, _) =>
            {
                await context.Db.SaveAsync(new CycleA { Id = $"ca-{b.Id}", Value = $"from-b-{b.Value}" });
            });
        }, testName: testName, cancellationToken: cancellationToken);
    }

    [Fact]
    public async Task CycleDetection_DirectCycle_Stops()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateCycleDbAsync(ct);
        var result = await db.SaveAsync(new CycleA { Id = "c1", Value = "start" }, ct);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task CycleDetection_ProducesFirstLevel()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateCycleDbAsync(ct);
        await db.SaveAsync(new CycleA { Id = "c2", Value = "start" }, ct);

        var b = await db.GetAsync<CycleB>("cb-c2", ct);
        Assert.NotNull(b);
    }

    [Fact]
    public async Task CycleDetection_NoExceptionThrown()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateCycleDbAsync(ct);
        var exception = await Record.ExceptionAsync(() =>
            db.SaveAsync(new CycleA { Id = "c3", Value = "safe" }, ct));
        Assert.Null(exception);
    }

    [Fact]
    public async Task CycleDetection_ResultContainsAllChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateCycleDbAsync(ct);
        var result = await db.SaveAsync(new CycleA { Id = "c4", Value = "chain" }, ct);

        Assert.Contains(result.Changes, c => c.Type == typeof(CycleA));
        Assert.Contains(result.Changes, c => c.Type == typeof(CycleB));
    }

    [Fact]
    public async Task CycleDetection_DifferentKeys_NotACycle()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<CycleA>().Do(async (a, context, _) =>
            {
                await context.Db.SaveAsync(new CycleB { Id = $"cb-{a.Id}", Value = $"from-a-{a.Value}" });
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new CycleA { Id = "x1", Value = "one" }, ct);
        await db.SaveAsync(new CycleA { Id = "x2", Value = "two" }, ct);

        Assert.NotNull(await db.GetAsync<CycleB>("cb-x1", ct));
        Assert.NotNull(await db.GetAsync<CycleB>("cb-x2", ct));
    }

    /// <summary>
    /// The lock stripe a key had when key locks were 1,024 hashed stripes, so a test can pick keys
    /// that shared one. String hashes are randomized per process, so this holds within one run.
    /// </summary>
    private static int OldStripeOf(string key) => (key.GetHashCode() & 0x7FFFFFFF) % 1024;

    [Fact]
    public async Task CrossingNestedSaves_OfUnrelatedKeys_DoNotDeadlock()
    {
        var ct = TestContext.Current.CancellationToken;

        // Two saves side by side, each of whose rules saves another object while the save holds its
        // own key's lock. Each inner key shared a stripe with the other save's outer key, so with
        // striped locks each waited on the other forever (as a message's save and its passages'
        // did). The keys are unrelated: only the exact key's lock may be waited on.
        const string outer1 = "crossing-outer-1", outer2 = "crossing-outer-2";
        static string Colliding(string with, string prefix) => Enumerable.Range(0, int.MaxValue)
            .Select(i => $"{prefix}-{i}")
            .First(k => OldStripeOf(k) == OldStripeOf(with));
        var inners = new Dictionary<string, string>
        {
            [outer1] = Colliding(outer2, "crossing-inner-1"),
            [outer2] = Colliding(outer1, "crossing-inner-2"),
        };

        // Both outer saves hold their locks before either inner save starts.
        using var bothHolding = new CountdownEvent(2);
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<CycleA>().Do(async (a, context, token) =>
            {
                bothHolding.Signal();
                await Task.Run(() => bothHolding.Wait(TimeSpan.FromSeconds(10), token), token);
                await context.Db.SaveAsync(new CycleB { Id = inners[a.Id], Value = "nested" }, token);
            });
        }, cancellationToken: ct);

        var saves = Task.WhenAll(
            Task.Run(() => db.SaveAsync(new CycleA { Id = outer1, Value = "outer" }, ct), ct),
            Task.Run(() => db.SaveAsync(new CycleA { Id = outer2, Value = "outer" }, ct), ct));
        Assert.True(await Task.WhenAny(saves, Task.Delay(TimeSpan.FromSeconds(20), ct)) == saves,
            "Two saves whose rules save unrelated keys waited on each other's locks.");
        await saves;

        Assert.NotNull(await db.GetAsync<CycleB>(inners[outer1], ct));
        Assert.NotNull(await db.GetAsync<CycleB>(inners[outer2], ct));
        Assert.Equal(0, db.KeyLocksInUse); // a key's lock goes once no one holds or waits for it
    }
}

