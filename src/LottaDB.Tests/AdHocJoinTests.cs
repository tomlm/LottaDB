namespace LottaDB.Tests;

public class AdHocJoinTests
{
    [Fact]
    public async Task AdHocJoin_SearchJoinAtQueryTime()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Actor { Domain = "join.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Domain = "join.test", Username = "bob", DisplayName = "Bob" });
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n1", AuthorId = "alice", Content = "Hello from Alice", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "join.test", NoteId = "n2", AuthorId = "bob", Content = "Hello from Bob", Published = DateTimeOffset.UtcNow });

        // Ad-hoc join at query time — Search<T>() returns IQueryable, LINQ hash join in memory
        var joined = (
            from note in db.Search<Note>()
            join actor in db.Search<Actor>()
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

        var joined = (
            from note in db.Search<Note>().Where(n => n.Content.Contains("Important"))
            join actor in db.Search<Actor>()
                on note.AuthorId equals actor.Username
            select new { note.NoteId, actor.DisplayName }
        ).ToList();

        Assert.Single(joined);
        Assert.Equal("n3", joined[0].NoteId);
    }

    [Fact]
    public async Task SearchAndSearchAsync_ReturnSameData()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Actor { Domain = "both.test", Username = "dave", DisplayName = "Dave" });
        await db.SaveAsync(new Actor { Domain = "both.test", Username = "eve", DisplayName = "Eve" });

        // Search<T>() — sync IQueryable
        var syncResults = db.Search<Actor>().ToList();

        // SearchAsync<T>() — async enumerable
        var asyncResults = await db.SearchAsync<Actor>().ToListAsync();

        Assert.Equal(syncResults.Count, asyncResults.Count);
        Assert.Equal(
            syncResults.Select(a => a.Username).OrderBy(u => u),
            asyncResults.Select(a => a.Username).OrderBy(u => u));
    }
}
