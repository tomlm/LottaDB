namespace LottaDB.Tests;

public class ChangeAsyncTests : IClassFixture<LottaDBFixture>
{
    private readonly ILottaDB _db;

    public ChangeAsyncTests(LottaDBFixture fixture)
    {
        _db = fixture.Db;
    }

    [Fact]
    public async Task ChangeAsync_MutatesAndSaves()
    {
        var actor = new Actor { Domain = "change.test", Username = "mutate", DisplayName = "Before" };
        await _db.SaveAsync(actor);

        await _db.ChangeAsync<Actor>("change.test", "mutate", a =>
        {
            a.DisplayName = "After";
            return a;
        });

        var loaded = await _db.GetAsync<Actor>("change.test", "mutate");
        Assert.NotNull(loaded);
        Assert.Equal("After", loaded.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ByObject_ExtractsKeys()
    {
        var actor = new Actor { Domain = "change.test", Username = "by-obj", DisplayName = "Before" };
        await _db.SaveAsync(actor);

        // Get the object so it has tracked ETag
        var loaded = await _db.GetAsync<Actor>("change.test", "by-obj");

        await _db.ChangeAsync(loaded!, a =>
        {
            a.DisplayName = "After";
            return a;
        });

        var updated = await _db.GetAsync<Actor>("change.test", "by-obj");
        Assert.Equal("After", updated!.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ReturnsObjectResult()
    {
        var actor = new Actor { Domain = "change.test", Username = "result", DisplayName = "Before" };
        await _db.SaveAsync(actor);

        var result = await _db.ChangeAsync<Actor>("change.test", "result", a =>
        {
            a.DisplayName = "After";
            return a;
        });

        Assert.NotEmpty(result.Changes);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Saved);
    }

    [Fact]
    public async Task ChangeAsync_NonExistent_Throws()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _db.ChangeAsync<Actor>("change.test", "ghost", a =>
            {
                a.DisplayName = "impossible";
                return a;
            }));
    }

    [Fact]
    public async Task ChangeAsync_MutationIsPure_CalledAtLeastOnce()
    {
        var actor = new Actor { Domain = "change.test", Username = "pure", DisplayName = "Before" };
        await _db.SaveAsync(actor);

        int callCount = 0;
        await _db.ChangeAsync<Actor>("change.test", "pure", a =>
        {
            Interlocked.Increment(ref callCount);
            a.DisplayName = "After";
            return a;
        });

        Assert.True(callCount >= 1);
    }

    [Fact]
    public async Task ChangeAsync_ConcurrentModification_Retries()
    {
        // Save an actor, then simulate a concurrent write between the
        // ChangeAsync's read and write by modifying it in the mutation.
        var actor = new Actor { Domain = "change.test", Username = "concurrent", DisplayName = "V1" };
        await _db.SaveAsync(actor);

        int callCount = 0;
        await _db.ChangeAsync<Actor>("change.test", "concurrent", a =>
        {
            int call = Interlocked.Increment(ref callCount);
            if (call == 1)
            {
                // On first call, simulate a concurrent write by saving directly
                // This changes the ETag, forcing a retry
                _db.SaveAsync(new Actor { Domain = "change.test", Username = "concurrent", DisplayName = "Concurrent" }).Wait();
            }
            a.DisplayName = "Final";
            return a;
        });

        // Mutation should have been called at least twice (first attempt + retry)
        Assert.True(callCount >= 2, $"Expected at least 2 calls, got {callCount}");

        var loaded = await _db.GetAsync<Actor>("change.test", "concurrent");
        Assert.Equal("Final", loaded!.DisplayName);
    }
}
