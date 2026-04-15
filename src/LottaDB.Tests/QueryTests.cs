namespace Lotta.Tests;

public class QueryTests
{
    [Fact]
    public async Task QueryAsync_ReturnsAllOfType()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob" }, TestContext.Current.CancellationToken);

        var all = db.Query<Actor>().ToList();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task QueryAsync_FilterByTag_ServerSide()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

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
            await db.SaveAsync(new Actor { Domain = "query.test", Username = $"user-{i}" }, TestContext.Current.CancellationToken);

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
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "" }, TestContext.Current.CancellationToken);

        // AvatarUrl is not a tag — should still filter (client-side)
        var withAvatar = db.Query<Actor>()
            .Where(a => a.AvatarUrl != "")
            .ToList();
        Assert.Single(withAvatar);
        Assert.Equal("alice", withAvatar[0].Username);
    }

    [Fact]
    public async Task QueryAsync_CombinedTagAndNonTag_FiltersCorrectly()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alex", DisplayName = "Alice", AvatarUrl = "" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "https://example.com/bob.png" }, TestContext.Current.CancellationToken);

        var matches = db.Query<Actor>()
            .Where(a => a.DisplayName == "Alice" && a.AvatarUrl != "")
            .ToList();

        Assert.Single(matches);
        Assert.Equal("alice", matches[0].Username);
    }

    [Fact]
    public async Task QueryAsync_OrAcrossTags_FiltersCorrectly()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", DisplayName = "Carol" }, TestContext.Current.CancellationToken);

        var matches = db.Query<Actor>()
            .Where(a => a.DisplayName == "Alice" || a.DisplayName == "Bob")
            .OrderBy(a => a.Username)
            .ToList();

        Assert.Equal(2, matches.Count);
        Assert.Equal("alice", matches[0].Username);
        Assert.Equal("bob", matches[1].Username);
    }
}
