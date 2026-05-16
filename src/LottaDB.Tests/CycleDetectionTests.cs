using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class CycleDetectionTests
{
    private async Task<LottaDB> CreateDbAsync([CallerMemberName] string? testName = null)
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
        }, testName: testName);
    }

    [Fact]
    public async Task CycleDetection_DirectCycle_Stops()
    {
        using var db = await CreateDbAsync();
        var result = await db.SaveAsync(new CycleA { Id = "c1", Value = "start" }, TestContext.Current.CancellationToken);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task CycleDetection_ProducesFirstLevel()
    {
        using var db = await CreateDbAsync();
        await db.SaveAsync(new CycleA { Id = "c2", Value = "start" }, TestContext.Current.CancellationToken);

        var b = await db.GetAsync<CycleB>("cb-c2", TestContext.Current.CancellationToken);
        Assert.NotNull(b);
    }

    [Fact]
    public async Task CycleDetection_NoExceptionThrown()
    {
        using var db = await CreateDbAsync();
        var exception = await Record.ExceptionAsync(() =>
            db.SaveAsync(new CycleA { Id = "c3", Value = "safe" }, TestContext.Current.CancellationToken));
        Assert.Null(exception);
    }

    [Fact]
    public async Task CycleDetection_ResultContainsAllChanges()
    {
        using var db = await CreateDbAsync();
        var result = await db.SaveAsync(new CycleA { Id = "c4", Value = "chain" }, TestContext.Current.CancellationToken);

        Assert.Contains(result.Changes, c => c.Type == typeof(CycleA));
        Assert.Contains(result.Changes, c => c.Type == typeof(CycleB));
    }

    [Fact]
    public async Task CycleDetection_DifferentKeys_NotACycle()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<CycleA>(async (a, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleB { Id = $"cb-{a.Id}", Value = $"from-a-{a.Value}" });
            });
        });

        await db.SaveAsync(new CycleA { Id = "x1", Value = "one" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new CycleA { Id = "x2", Value = "two" }, TestContext.Current.CancellationToken);

        Assert.NotNull(await db.GetAsync<CycleB>("cb-x1", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<CycleB>("cb-x2", TestContext.Current.CancellationToken));
    }
}
