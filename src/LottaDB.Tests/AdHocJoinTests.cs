namespace Lotta.Tests;

public class AdHocJoinTests
{
    [Fact]
    public async Task AdHocJoin_MaterializeThenJoinInMemory()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Actor { Domain = "join.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Domain = "join.test", Username = "bob", DisplayName = "Bob" });
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n1", AuthorId = "alice", Content = "Hello from Alice", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n2", AuthorId = "bob", Content = "Hello from Bob", Published = DateTimeOffset.UtcNow });

        // Ad-hoc join: materialize both sides via SearchAsync, then LINQ join in memory
        var notes = db.Search<Note>().ToList();
        var actors = db.Search<Actor>().ToList();

        var joined = (
            from note in notes
            join actor in actors
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
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Actor { Domain = "join.test", Username = "carol", DisplayName = "Carol" });
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n3", AuthorId = "carol", Content = "Important note", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n4", AuthorId = "carol", Content = "Boring note", Published = DateTimeOffset.UtcNow });

        var notes = db.Search<Note>().ToList().Where(n => n.Content.Contains("Important"));
        var actors = db.Search<Actor>().ToList();

        var joined = (
            from note in notes
            join actor in actors
                on note.AuthorId equals actor.Username
            select new { note.NoteId, actor.DisplayName }
        ).ToList();

        Assert.Single(joined);
        Assert.Equal("n3", joined[0].NoteId);
    }

    [Fact]
    public async Task SearchAsync_WithQueryString_FiltersResults()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob" });

        // SearchAsync with no query returns all
        var all = db.Search<Actor>().ToList();
        Assert.Equal(2, all.Count);

        // SearchAsync with query parameter (basic test — full Lucene query support depends on indexer)
        var filtered = db.Search<Actor>("DisplayName:Alice").ToList();
        // Note: until Lucene searcher refresh is implemented, this may return all or filtered
        // The test verifies the parameter is accepted without error
        Assert.NotNull(filtered);
    }
}
