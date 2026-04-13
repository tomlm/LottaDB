namespace Lotta.Tests;

public class CycleDetectionTests
{
    private LottaDB CreateDbWithCycles()
    {
        return LottaDBFixture.CreateDb(opts =>
        {
            // CycleA → save CycleB
            opts.On<CycleA>(async (a, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleB { Id = a.Id, Value = $"from-a-{a.Value}" });
            });

            // CycleB → save CycleA (creates cycle!)
            opts.On<CycleB>(async (b, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleA { Id = b.Id, Value = $"from-b-{b.Value}" });
            });
        });
    }

    [Fact]
    public async Task CycleDetection_DirectCycle_Stops()
    {
        var db = CreateDbWithCycles();
        var result = await db.SaveAsync(new CycleA { Id = "c1", Value = "start" });
        Assert.NotNull(result);
    }

    [Fact]
    public async Task CycleDetection_ProducesFirstLevel()
    {
        var db = CreateDbWithCycles();
        await db.SaveAsync(new CycleA { Id = "c2", Value = "start" });

        var b = await db.GetAsync<CycleB>("c2");
        Assert.NotNull(b);
    }

    [Fact]
    public async Task CycleDetection_NoExceptionThrown()
    {
        var db = CreateDbWithCycles();
        var exception = await Record.ExceptionAsync(() =>
            db.SaveAsync(new CycleA { Id = "c3", Value = "safe" }));
        Assert.Null(exception);
    }

    [Fact]
    public async Task CycleDetection_ResultContainsAllChanges()
    {
        var db = CreateDbWithCycles();
        var result = await db.SaveAsync(new CycleA { Id = "c4", Value = "chain" });

        Assert.Contains(result.Changes, c => c.TypeName == nameof(CycleA));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(CycleB));
    }

    [Fact]
    public async Task CycleDetection_DifferentKeys_NotACycle()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.On<CycleA>(async (a, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new CycleB { Id = a.Id, Value = $"from-a-{a.Value}" });
            });
        });

        await db.SaveAsync(new CycleA { Id = "x1", Value = "one" });
        await db.SaveAsync(new CycleA { Id = "x2", Value = "two" });

        Assert.NotNull(await db.GetAsync<CycleB>("x1"));
        Assert.NotNull(await db.GetAsync<CycleB>("x2"));
    }
}
