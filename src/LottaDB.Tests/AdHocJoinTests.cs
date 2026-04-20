namespace Lotta.Tests;

public class AdHocJoinTests
{
    [Fact]
    public async Task AdHocJoin_MaterializeThenJoinInMemory()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Domain = "join.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "join.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n1", AuthorId = "alice", Content = "Hello from Alice", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n2", AuthorId = "bob", Content = "Hello from Bob", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        // Ad-hoc join: materialize both sides via SearchAsync, then LINQ join in memory

        var joined = (
            from note in db.Search<Note>().ToList()
            join actor in db.Search<Actor>().ToList()
                on note.AuthorId equals actor.Username
            select new { note.NoteId, note.Content, actor.DisplayName }
        ).ToList();

        Assert.Equal(2, joined.Count);
        Assert.Contains(joined, j => j.NoteId == "n1" && j.DisplayName == "Alice");
        Assert.Contains(joined, j => j.NoteId == "n2" && j.DisplayName == "Bob");
    }

    [Fact]
    public async Task AdHocJoin_WithWhereClause()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Domain = "join.test", Username = "carol", DisplayName = "Carol" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n3", AuthorId = "carol", Content = "Important note", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n4", AuthorId = "carol", Content = "Boring note", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var joined = (
            from note in db.Search<Note>().Where(n => n.Content.Contains("Important")).ToList()
            join actor in db.Search<Actor>()
                on note.AuthorId equals actor.Username
            select new { note.NoteId, actor.DisplayName }
        ).ToList();

        Assert.Single(joined);
        Assert.Equal("n3", joined[0].NoteId);
    }

    [Fact]
    public async Task SearchAsync_WithQueryString_FiltersResults()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        // SearchAsync with no query returns all
        var all = db.Search<Actor>().ToList();
        Assert.Equal(2, all.Count);

        // SearchAsync with query parameter (basic test — full Lucene query support depends on indexer)
        var filtered = db.Search<Actor>("DisplayName:Alice").ToList();
        Assert.Single(filtered);
        Assert.Equal("Alice", filtered[0].DisplayName);
    }

    [Fact]
    public async Task SearchAsync_WithOpenQueryString_FiltersResults()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        // SearchAsync with no query returns all
        var all = db.Search<Actor>().ToList();
        Assert.Equal(2, all.Count);

        // SearchAsync with query parameter (basic test — full Lucene query support depends on indexer)
        var filtered = db.Search<Actor>("Alice").ToList();
        Assert.Single(filtered);
        Assert.Equal("Alice", filtered[0].DisplayName);
    }
}
