namespace LottaDB.Tests;

public class CycleDetectionTests
{
    private LottaDB CreateDbWithCycles()
    {
        return TestLottaDBFactory.CreateWithBuilders(opts =>
        {
            opts.AddBuilder<CycleA, CycleB, CycleABuilder>();
            opts.AddBuilder<CycleB, CycleA, CycleBBuilder>();
        });
    }

    [Fact]
    public async Task CycleDetection_DirectCycle_Stops()
    {
        var db = CreateDbWithCycles();

        // Save CycleA → produces CycleB → produces CycleA → should stop (cycle detected)
        var result = await db.SaveAsync(new CycleA { Id = "c1", Value = "start" });

        // Should not throw, should not infinite loop
        Assert.NotNull(result);
    }

    [Fact]
    public async Task CycleDetection_ProducesFirstLevel()
    {
        var db = CreateDbWithCycles();
        await db.SaveAsync(new CycleA { Id = "c2", Value = "start" });

        // CycleA should produce CycleB
        var b = await db.GetAsync<CycleB>("c2");
        Assert.NotNull(b);
    }

    [Fact]
    public async Task CycleDetection_DifferentKeys_NotACycle()
    {
        // Two different CycleA objects are not a cycle
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
        {
            opts.AddBuilder<CycleA, CycleB, CycleABuilder>();
            // No CycleB→CycleA builder, so no cycle
        });

        await db.SaveAsync(new CycleA { Id = "x1", Value = "one" });
        await db.SaveAsync(new CycleA { Id = "x2", Value = "two" });

        // Both should produce CycleB objects
        Assert.NotNull(await db.GetAsync<CycleB>("x1"));
        Assert.NotNull(await db.GetAsync<CycleB>("x2"));
    }

    [Fact]
    public async Task CycleDetection_NoExceptionThrown()
    {
        var db = CreateDbWithCycles();

        // Should complete without throwing
        var exception = await Record.ExceptionAsync(() =>
            db.SaveAsync(new CycleA { Id = "c3", Value = "safe" }));

        Assert.Null(exception);
    }

    [Fact]
    public async Task CycleDetection_ResultContainsAllChanges()
    {
        var db = CreateDbWithCycles();
        var result = await db.SaveAsync(new CycleA { Id = "c4", Value = "chain" });

        // Should contain at least CycleA (source) and CycleB (first derived)
        Assert.Contains(result.Changes, c => c.TypeName == nameof(CycleA));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(CycleB));
    }
}
