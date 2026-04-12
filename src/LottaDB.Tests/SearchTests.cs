namespace LottaDB.Tests;

public class SearchTests
{
    [Fact]
    public async Task SearchAsync_AutoIndexed_OnSave()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice" });

        var results = await db.SearchAsync<Actor>().ToListAsync();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchAsync_FindsByFieldValue()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "bob", DisplayName = "Bob" });

        var results = await db.SearchAsync<Actor>()
            .Where(a => a.Username == "alice")
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].DisplayName);
    }

    [Fact]
    public async Task SearchAsync_FilterByContent()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Note { Domain = "search.test", NoteId = "n1", AuthorId = "alice", Content = "Lucene is great for full text search", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "search.test", NoteId = "n2", AuthorId = "bob", Content = "Azure table storage is fast", Published = DateTimeOffset.UtcNow });

        // Filter by content containing a substring
        var results = await db.SearchAsync<Note>()
            .Where(n => n.Content.Contains("Lucene"))
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchAsync_Take_LimitsResults()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "search.test", Username = $"user-{i}", DisplayName = $"User {i}" });

        var results = await db.SearchAsync<Actor>().Take(3).ToListAsync();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task SearchAsync_RemovedFromIndex_OnDelete()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        var actor = new Actor { Domain = "search.test", Username = "deletable", DisplayName = "Gone" };
        await db.SaveAsync(actor);

        // Verify it's in the index
        var before = await db.SearchAsync<Actor>()
            .Where(a => a.Username == "deletable")
            .ToListAsync();
        Assert.Single(before);

        // Delete and verify it's removed
        await db.DeleteAsync(actor);
        var after = await db.SearchAsync<Actor>()
            .Where(a => a.Username == "deletable")
            .ToListAsync();
        Assert.Empty(after);
    }

    [Fact]
    public async Task SearchAsync_EmptyIndex_ReturnsEmpty()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        var results = await db.SearchAsync<Actor>().ToListAsync();
        Assert.Empty(results);
    }

    [Fact]
    public async Task SearchAsync_UpdatedObject_ReflectsInIndex()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "updatable", DisplayName = "Before" });
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "updatable", DisplayName = "After" });

        var results = await db.SearchAsync<Actor>()
            .Where(a => a.Username == "updatable")
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("After", results[0].DisplayName);
    }
}
