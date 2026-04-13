namespace LottaDB.Tests;

public class CrudTests : IClassFixture<LottaDBFixture>
{
    private readonly LottaDB _db;

    public CrudTests(LottaDBFixture fixture)
    {
        _db = fixture.Db;
    }

    [Fact]
    public async Task SaveAsync_NewObject_CanGetBack()
    {
        var actor = new Actor { Domain = "crud.test", Username = "save-get", DisplayName = "Test" };
        await _db.SaveAsync(actor);
        var loaded = await _db.GetAsync<Actor>("save-get");
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_WithExplicitKeys_Works()
    {
        // Actor has [Key] on Username, so saving with Username="explicit-keys" uses that as the key
        var actor = new Actor { Domain = "crud.test", Username = "explicit-keys", DisplayName = "Explicit" };
        await _db.SaveAsync(actor);
        var loaded = await _db.GetAsync<Actor>("explicit-keys");
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

        var loaded = await _db.GetAsync<Actor>("overwrite");
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
        var loaded = await _db.GetAsync<Actor>("get-by-keys");
        Assert.NotNull(loaded);
        Assert.Equal("Found", loaded.DisplayName);
    }

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        var loaded = await _db.GetAsync<Actor>("does-not-exist");
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByKeys_RemovesObject()
    {
        var actor = new Actor { Domain = "crud.test", Username = "delete-me", DisplayName = "Gone" };
        await _db.SaveAsync(actor);
        await _db.DeleteAsync<Actor>("delete-me");
        var loaded = await _db.GetAsync<Actor>("delete-me");
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByObject_RemovesObject()
    {
        var actor = new Actor { Domain = "crud.test", Username = "delete-obj", DisplayName = "Gone" };
        await _db.SaveAsync(actor);
        await _db.DeleteAsync(actor);
        var loaded = await _db.GetAsync<Actor>("delete-obj");
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
        var result = await _db.DeleteAsync<Actor>("never-existed");
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

        var loaded = await _db.GetAsync<OrderWithLines>("order-1");
        Assert.NotNull(loaded);
        Assert.Equal(150.50m, loaded.Total);
        Assert.Equal(2, loaded.Lines.Count);
        Assert.Equal("p1", loaded.Lines[0].ProductId);
        Assert.Equal(2, loaded.Metadata.Count);
        Assert.Equal("web", loaded.Metadata["source"]);
    }

}
