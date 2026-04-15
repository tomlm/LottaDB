namespace Lotta.Tests;

public class QueryTests
{
    [Fact]
    public async Task QueryAsync_ReturnsAllOfType()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice" });
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob" });

        var all = db.Query<Actor>().ToList();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task QueryAsync_FilterByTag_ServerSide()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow });

        var aliceNotes = db.Query<Note>()
            .Where(n => n.AuthorId == "alice")
            .ToList();

        Assert.Single(aliceNotes);
        Assert.Equal("n1", aliceNotes[0].NoteId);
    }

    [Fact]
    public async Task QueryAsync_Take_LimitsResults()
    {
        var db = LottaDBFixture.CreateDb();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "query.test", Username = $"user-{i}" });

        var limited = db.Query<Actor>().Take(3).ToList();
        Assert.Equal(3, limited.Count);
    }

    [Fact]
    public async Task QueryAsync_EmptyTable_ReturnsEmpty()
    {
        var db = LottaDBFixture.CreateDb();
        var all = db.Query<Actor>().ToList();
        Assert.Empty(all);
    }

    [Fact]
    public async Task QueryAsync_WhereOnNonTag_EvaluatesClientSide()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" });
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "" });

        // AvatarUrl is not a tag — should still filter (client-side)
        var withAvatar = db.Query<Actor>()
            .Where(a => a.AvatarUrl != "")
            .ToList();
        Assert.Single(withAvatar);
        Assert.Equal("alice", withAvatar[0].Username);
    }
}
