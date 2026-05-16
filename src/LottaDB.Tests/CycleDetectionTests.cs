using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class CycleDetectionTests
{
    private async Task<LottaDB> CreateDbAsync(CancellationToken cancellationToken = default, [CallerMemberName] string? testName = null)
    {
        return await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<CycleA>(async (a, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleB { Id = $"cb-{a.Id}", Value = $"from-a-{a.Value}" });
            });

            opts.On<CycleB>(async (b, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleA { Id = $"ca-{b.Id}", Value = $"from-b-{b.Value}" });
            });
        }, testName: testName, cancellationToken: cancellationToken);
    }

    [Fact]
    public async Task CycleDetection_DirectCycle_Stops()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        var result = await db.SaveAsync(new CycleA { Id = "c1", Value = "start" }, ct);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task CycleDetection_ProducesFirstLevel()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new CycleA { Id = "c2", Value = "start" }, ct);

        var b = await db.GetAsync<CycleB>("cb-c2", ct);
        Assert.NotNull(b);
    }

    [Fact]
    public async Task CycleDetection_NoExceptionThrown()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        var exception = await Record.ExceptionAsync(() =>
            db.SaveAsync(new CycleA { Id = "c3", Value = "safe" }, ct));
        Assert.Null(exception);
    }

    [Fact]
    public async Task CycleDetection_ResultContainsAllChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        var result = await db.SaveAsync(new CycleA { Id = "c4", Value = "chain" }, ct);

        Assert.Contains(result.Changes, c => c.Type == typeof(CycleA));
        Assert.Contains(result.Changes, c => c.Type == typeof(CycleB));
    }

    [Fact]
    public async Task CycleDetection_DifferentKeys_NotACycle()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<CycleA>(async (a, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleB { Id = $"cb-{a.Id}", Value = $"from-a-{a.Value}" });
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new CycleA { Id = "x1", Value = "one" }, ct);
        await db.SaveAsync(new CycleA { Id = "x2", Value = "two" }, ct);

        Assert.NotNull(await db.GetAsync<CycleB>("cb-x1", ct));
        Assert.NotNull(await db.GetAsync<CycleB>("cb-x2", ct));
    }
}
