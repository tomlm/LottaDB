namespace LottaDB.Tests;

public class CrudTests : IClassFixture<LottaDBFixture>
{
    private readonly ILottaDB _db;

    public CrudTests(LottaDBFixture fixture)
    {
        _db = fixture.Db;
    }

    [Fact]
    public async Task SaveAsync_NewObject_CanGetBack()
    {
        var actor = new Actor { Domain = "crud.test", Username = "save-get", DisplayName = "Test" };
        await _db.SaveAsync(actor);
        var loaded = await _db.GetAsync<Actor>("crud.test", "save-get");
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_WithExplicitKeys_Works()
    {
        var actor = new Actor { Domain = "crud.test", Username = "explicit-keys", DisplayName = "Explicit" };
        await _db.SaveAsync("crud.test", "explicit-keys", actor);
        var loaded = await _db.GetAsync<Actor>("crud.test", "explicit-keys");
        Assert.NotNull(loaded);
        Assert.Equal("Explicit", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_ExistingObject_Overwrites()
    {
        var actor = new Actor { Domain = "crud.test", Username = "overwrite", DisplayName = "V1" };
        await _db.SaveAsync(actor);

        actor.DisplayName = "V2";
        await _db.SaveAsync(actor);

        var loaded = await _db.GetAsync<Actor>("crud.test", "overwrite");
        Assert.NotNull(loaded);
        Assert.Equal("V2", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_ReturnsObjectResult_WithSavedChange()
    {
        var actor = new Actor { Domain = "crud.test", Username = "result-check", DisplayName = "Test" };
        var result = await _db.SaveAsync(actor);

        Assert.NotEmpty(result.Changes);
        Assert.Contains(result.Changes, c => c.TypeName == nameof(Actor) && c.Kind == ChangeKind.Saved);
    }

    [Fact]
    public async Task GetAsync_ByKeys_ReturnsObject()
    {
        var actor = new Actor { Domain = "crud.test", Username = "get-by-keys", DisplayName = "Found" };
        await _db.SaveAsync(actor);
        var loaded = await _db.GetAsync<Actor>("crud.test", "get-by-keys");
        Assert.NotNull(loaded);
        Assert.Equal("Found", loaded.DisplayName);
    }

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        var loaded = await _db.GetAsync<Actor>("crud.test", "does-not-exist");
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByKeys_RemovesObject()
    {
        var actor = new Actor { Domain = "crud.test", Username = "delete-me", DisplayName = "Gone" };
        await _db.SaveAsync(actor);
        await _db.DeleteAsync<Actor>("crud.test", "delete-me");
        var loaded = await _db.GetAsync<Actor>("crud.test", "delete-me");
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByObject_RemovesObject()
    {
        var actor = new Actor { Domain = "crud.test", Username = "delete-obj", DisplayName = "Gone" };
        await _db.SaveAsync(actor);
        await _db.DeleteAsync(actor);
        var loaded = await _db.GetAsync<Actor>("crud.test", "delete-obj");
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ReturnsObjectResult_WithDeletedChange()
    {
        var actor = new Actor { Domain = "crud.test", Username = "delete-result", DisplayName = "Gone" };
        await _db.SaveAsync(actor);
        var result = await _db.DeleteAsync(actor);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Deleted);
    }

    [Fact]
    public async Task DeleteAsync_NonExistent_NoError()
    {
        // Should not throw
        var result = await _db.DeleteAsync<Actor>("crud.test", "never-existed");
        Assert.NotNull(result);
    }

    [Fact]
    public async Task SaveAsync_JsonPreservesFullPoco()
    {
        var order = new OrderWithLines
        {
            TenantId = "crud.test",
            OrderId = "order-1",
            Total = 150.50m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "p1", Quantity = 2, Price = 50.25m },
                new() { ProductId = "p2", Quantity = 1, Price = 50.00m },
            },
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "web",
                ["campaign"] = "summer"
            }
        };
        await _db.SaveAsync(order);

        var loaded = await _db.GetAsync<OrderWithLines>("crud.test", "order-1");
        Assert.NotNull(loaded);
        Assert.Equal(150.50m, loaded.Total);
        Assert.Equal(2, loaded.Lines.Count);
        Assert.Equal("p1", loaded.Lines[0].ProductId);
        Assert.Equal(2, loaded.Metadata.Count);
        Assert.Equal("web", loaded.Metadata["source"]);
    }

    [Fact]
    public async Task GetAsync_ByObject_ConditionalGet_ReturnsNullWhenUnchanged()
    {
        var actor = new Actor { Domain = "crud.test", Username = "etag-cond", DisplayName = "Stable" };
        await _db.SaveAsync(actor);
        var loaded = await _db.GetAsync<Actor>("crud.test", "etag-cond");
        Assert.NotNull(loaded);

        // Conditional get: object hasn't changed, should return null
        var unchanged = await _db.GetAsync(loaded!);
        Assert.Null(unchanged);
    }

    [Fact]
    public async Task GetAsync_ByObject_ForceTrue_AlwaysFetches()
    {
        var actor = new Actor { Domain = "crud.test", Username = "etag-force", DisplayName = "Forced" };
        await _db.SaveAsync(actor);
        var loaded = await _db.GetAsync<Actor>("crud.test", "etag-force");
        Assert.NotNull(loaded);

        // Force: true should always return the object even if unchanged
        var fetched = await _db.GetAsync(loaded!, force: true);
        Assert.NotNull(fetched);
        Assert.Equal("Forced", fetched.DisplayName);
    }
}
