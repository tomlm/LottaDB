namespace LottaDB.Tests;

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
        await _db.SaveAsync(actor);

        await _db.ChangeAsync<Actor>("mutate", a =>
        {
            a.DisplayName = "After";
            return a;
        });

        var loaded = await _db.GetAsync<Actor>("mutate");
        Assert.NotNull(loaded);
        Assert.Equal("After", loaded.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ByObject_ExtractsKeys()
    {
        var actor = new Actor { Domain = "change.test", Username = "by-obj", DisplayName = "Before" };
        await _db.SaveAsync(actor);

        // Get the object so it has tracked ETag
        var loaded = await _db.GetAsync<Actor>("by-obj");

        await _db.ChangeAsync<Actor>(loaded!.Username, a =>
        {
            a.DisplayName = "After";
            return a;
        });

        var updated = await _db.GetAsync<Actor>("by-obj");
        Assert.Equal("After", updated!.DisplayName);
    }

    [Fact]
    public async Task ChangeAsync_ReturnsObjectResult()
    {
        var actor = new Actor { Domain = "change.test", Username = "result", DisplayName = "Before" };
        await _db.SaveAsync(actor);

        var result = await _db.ChangeAsync<Actor>("result", a =>
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
            _db.ChangeAsync<Actor>("ghost", a =>
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
        await _db.ChangeAsync<Actor>("pure", a =>
        {
            Interlocked.Increment(ref callCount);
            a.DisplayName = "After";
            return a;
        });

        Assert.True(callCount >= 1);
    }

}
