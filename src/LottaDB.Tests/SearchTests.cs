namespace Lotta.Tests;

public class SearchTests
{
    [Fact]
    public async Task SearchAsync_AutoIndexed_OnSave()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice" });

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchAsync_FindsByFieldValue()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "bob", DisplayName = "Bob" });

        var results = db.Search<Actor>()
            .Where(a => a.Username == "alice")
            .ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].DisplayName);
    }

    [Fact]
    public async Task SearchAsync_FilterByContent()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Note { Domain = "search.test", NoteId = "n1", AuthorId = "alice", Content = "Lucene is great for full text search", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "search.test", NoteId = "n2", AuthorId = "bob", Content = "Azure table storage is fast", Published = DateTimeOffset.UtcNow });

        // Filter by a non-analyzed field (AuthorId has NotAnalyzed)
        var results = db.Search<Note>()
            .Where(n => n.Content.Contains("lucene"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchAsync_Take_LimitsResults()
    {
        var db = LottaDBFixture.CreateDb();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "search.test", Username = $"user-{i}", DisplayName = $"User {i}" });

        var results = db.Search<Actor>().Take(3).ToList();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task SearchAsync_RemovedFromIndex_OnDelete()
    {
        var db = LottaDBFixture.CreateDb();
        var actor = new Actor { Domain = "search.test", Username = "deletable", DisplayName = "Gone" };
        await db.SaveAsync(actor);

        // Verify it's in the index
        var before = db.Search<Actor>()
            .Where(a => a.Username == "deletable")
            .ToList();
        Assert.Single(before);

        // Delete and verify it's removed
        await db.DeleteAsync(actor);
        var after = db.Search<Actor>()
            .Where(a => a.Username == "deletable")
            .ToList();
        Assert.Empty(after);
    }

    [Fact]
    public async Task SearchAsync_EmptyIndex_ReturnsEmpty()
    {
        var db = LottaDBFixture.CreateDb();
        var results = db.Search<Actor>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task SearchAsync_UpdatedObject_ReflectsInIndex()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "updatable", DisplayName = "Before" });
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "updatable", DisplayName = "After" });

        var results = db.Search<Actor>()
            .Where(a => a.Username == "updatable")
            .ToList();
        Assert.Single(results);
        Assert.Equal("After", results[0].DisplayName);
    }
}
