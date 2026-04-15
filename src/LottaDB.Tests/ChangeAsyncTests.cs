namespace Lotta.Tests;

public class ChangeAsyncTests : IClassFixture<LottaDBFixture>
{
    private readonly LottaDB _db;

    public ChangeAsyncTests(LottaDBFixture fixture)
    {
        _db = fixture.Db;
    }

    [Fact]
    public async Task ChangeAsync_MutatesAndSaves()
    {
        var actor = new Actor { Domain = "change.test", Username = "mutate", DisplayName = "Before" };
        await _db.SaveAsync(actor, TestContext.Current.CancellationToken);

        await _db.ChangeAsync<Actor>("mutate", a =>
        {
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        var loaded = await _db.GetAsync<Actor>("mutate", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("After", loaded.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ByObject_ExtractsKeys()
    {
        var actor = new Actor { Domain = "change.test", Username = "by-obj", DisplayName = "Before" };
        await _db.SaveAsync(actor, TestContext.Current.CancellationToken);

        // Get the object so it has tracked ETag
        var loaded = await _db.GetAsync<Actor>("by-obj", TestContext.Current.CancellationToken);

        await _db.ChangeAsync<Actor>(loaded!.Username, a =>
        {
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        var updated = await _db.GetAsync<Actor>("by-obj", TestContext.Current.CancellationToken);
        Assert.Equal("After", updated!.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ReturnsObjectResult()
    {
        var actor = new Actor { Domain = "change.test", Username = "result", DisplayName = "Before" };
        await _db.SaveAsync(actor, TestContext.Current.CancellationToken);

        var result = await _db.ChangeAsync<Actor>("result", a =>
        {
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        Assert.NotEmpty(result.Changes);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Saved);
    }

    [Fact]
    public async Task ChangeAsync_NonExistent_Throws()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _db.ChangeAsync<Actor>("ghost", a =>
            {
                a.DisplayName = "impossible";
                return a;
            }, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ChangeAsync_MutationIsPure_CalledAtLeastOnce()
    {
        var actor = new Actor { Domain = "change.test", Username = "pure", DisplayName = "Before" };
        await _db.SaveAsync(actor, TestContext.Current.CancellationToken);

        int callCount = 0;
        await _db.ChangeAsync<Actor>("pure", a =>
        {
            Interlocked.Increment(ref callCount);
            a.DisplayName = "After";
            return a;
        }, TestContext.Current.CancellationToken);

        Assert.True(callCount >= 1);
    }

}
