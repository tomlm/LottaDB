namespace Lotta.Tests;

public class CrudTests : IClassFixture<LottaDBFixture>
{

    [Fact]
    public async Task SaveAsync_NewObject_CanGetBack()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "save-get", DisplayName = "Test" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        var loaded = await db.GetAsync<Actor>("save-get", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_WithExplicitKeys_Works()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        // Actor has [Key] on Username, so saving with Username="explicit-keys" uses that as the key
        var actor = new Actor { Domain = "crud.test", Username = "explicit-keys", DisplayName = "Explicit" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        var loaded = await db.GetAsync<Actor>("explicit-keys", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Explicit", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_ExistingObject_Overwrites()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "overwrite", DisplayName = "V1" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        actor.DisplayName = "V2";
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<Actor>("overwrite", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("V2", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_ReturnsObjectResult_WithSavedChange()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "result-check", DisplayName = "Test" };
        var result = await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        Assert.NotEmpty(result.Changes);
        Assert.Contains(result.Changes, c => c.Type == typeof(Actor) && c.Kind == ChangeKind.Saved);
    }

    [Fact]
    public async Task GetAsync_ByKeys_ReturnsObject()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "get-by-keys", DisplayName = "Found" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        var loaded = await db.GetAsync<Actor>("get-by-keys", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Found", loaded.DisplayName);
    }

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var loaded = await db.GetAsync<Actor>("does-not-exist", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByKeys_RemovesObject()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "delete-me", DisplayName = "Gone" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        await db.DeleteAsync<Actor>("delete-me", TestContext.Current.CancellationToken);
        var loaded = await db.GetAsync<Actor>("delete-me", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ByObject_RemovesObject()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "delete-obj", DisplayName = "Gone" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        await db.DeleteAsync(actor, TestContext.Current.CancellationToken);
        var loaded = await db.GetAsync<Actor>("delete-obj", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task DeleteAsync_ReturnsObjectResult_WithDeletedChange()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Domain = "crud.test", Username = "delete-result", DisplayName = "Gone" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        var result = await db.DeleteAsync(actor, TestContext.Current.CancellationToken);
        Assert.Contains(result.Changes, c => c.Kind == ChangeKind.Deleted);
    }

    [Fact]
    public async Task DeleteAsync_NonExistent_NoError()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        // Should not throw
        var result = await db.DeleteAsync<Actor>("never-existed", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task DeleteDatabaseAsync_RemovesTableAndIndex()
    {
        using (var db = await LottaDBFixture.CreateDbAsync(reset: true))
        {
            await db.SaveAsync(new Actor
            {
                Domain = "crud.test",
                Username = "delete-db",
                DisplayName = "Delete Database"
            }, TestContext.Current.CancellationToken);

            Assert.NotNull(await db.GetAsync<Actor>("delete-db", TestContext.Current.CancellationToken));
            Assert.Single(db.Search<Actor>().Where(a => a.Username == "delete-db"));

            await db.DeleteDatabaseAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task SaveAsync_JsonPreservesFullPoco()
    {
        var db = await LottaDBFixture.CreateDbAsync();
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
        await db.SaveAsync(order, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync<OrderWithLines>("order-1", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal(150.50m, loaded.Total);
        Assert.Equal(2, loaded.Lines.Count);
        Assert.Equal("p1", loaded.Lines[0].ProductId);
        Assert.Equal(2, loaded.Metadata.Count);
        Assert.Equal("web", loaded.Metadata["source"]);
    }

    [Fact]
    public async Task DeleteAsync_WithPredicate_DeletesMatchingObjects()
    {
        var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Note { NoteId = "pred1", AuthorId = "alice", Content = "A", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "pred2", AuthorId = "alice", Content = "B", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "pred3", AuthorId = "bob", Content = "C", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.NotNull(await db.GetAsync<Note>("pred1", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<Note>("pred2", TestContext.Current.CancellationToken));

        var result = await db.DeleteAsync<Note>(n => n.AuthorId == "alice", TestContext.Current.CancellationToken);

        // Alice's notes gone
        Assert.Null(await db.GetAsync<Note>("pred1", TestContext.Current.CancellationToken));
        Assert.Null(await db.GetAsync<Note>("pred2", TestContext.Current.CancellationToken));
        // Bob's note still there
        Assert.NotNull(await db.GetAsync<Note>("pred3", TestContext.Current.CancellationToken));
        // Result contains 2 deletions
        Assert.Equal(2, result.Changes.Count(c => c.Kind == ChangeKind.Deleted));
    }

    [Fact]
    public async Task DeleteAsync_WithPredicate_RunsOnHandlers()
    {
        int deleteCount = 0;
        var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, d) =>
            {
                if (kind == TriggerKind.Deleted)
                    Interlocked.Increment(ref deleteCount);
            });
        });

        await db.SaveAsync(new Note { NoteId = "h1", AuthorId = "alice", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "h2", AuthorId = "alice", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        await db.DeleteAsync<Note>(n => n.AuthorId == "alice", TestContext.Current.CancellationToken);

        Assert.Equal(2, deleteCount);
    }
}
