namespace LottaDB.Tests;

public class QueryTests
{
    [Fact]
    public async Task QueryAsync_ReturnsAllOfType()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice" });
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob" });

        var all = await db.QueryAsync<Actor>().ToListAsync();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task QueryAsync_FilterByTag_ServerSide()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow });

        var aliceNotes = await db.QueryAsync<Note>()
            .Where(n => n.AuthorId == "alice")
            .ToListAsync();

        Assert.Single(aliceNotes);
        Assert.Equal("n1", aliceNotes[0].NoteId);
    }

    [Fact]
    public async Task QueryAsync_Take_LimitsResults()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "query.test", Username = $"user-{i}" });

        var limited = await db.QueryAsync<Actor>().Take(3).ToListAsync();
        Assert.Equal(3, limited.Count);
    }

    [Fact]
    public async Task QueryAsync_EmptyTable_ReturnsEmpty()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        var all = await db.QueryAsync<Actor>().ToListAsync();
        Assert.Empty(all);
    }

    [Fact]
    public async Task QueryAsync_WhereOnNonTag_EvaluatesClientSide()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" });
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "" });

        // AvatarUrl is not a tag — should still filter (client-side)
        var withAvatar = await db.QueryAsync<Actor>()
            .Where(a => a.AvatarUrl != "")
            .ToListAsync();
        Assert.Single(withAvatar);
        Assert.Equal("alice", withAvatar[0].Username);
    }
}
