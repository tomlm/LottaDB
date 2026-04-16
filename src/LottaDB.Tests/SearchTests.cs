namespace Lotta.Tests;

public class SearchTests
{
    [Fact]
    public async Task SearchAsync_AutoIndexed_OnSave()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchAsync_FindsByFieldValue()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice", Counter=5 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "bob", DisplayName = "Bob", Counter=30 }, TestContext.Current.CancellationToken);

        var results = db.Search<Actor>()
            .Where(a => a.Username == "alice")
            .ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task SearchAsync_FindsByNumericCounterComparisons()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice", Counter = 5 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "bob", DisplayName = "Bob", Counter = 30 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "carol", DisplayName = "Carol", Counter = 30 }, TestContext.Current.CancellationToken);

        var equalsResults = db.Search<Actor>()
            .Where(a => a.Counter == 30)
            .OrderBy(a => a.Username)
            .ToList();
        Assert.Equal(2, equalsResults.Count);
        Assert.Equal(["bob", "carol"], equalsResults.Select(a => a.Username).ToArray());

        var lessThanResults = db.Search<Actor>()
            .Where(a => a.Counter < 10)
            .ToList();
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = db.Search<Actor>()
            .Where(a => a.Counter > 10)
            .OrderBy(a => a.Username)
            .ToList();
        Assert.Equal(2, greaterThanResults.Count);
        Assert.Equal(["bob", "carol"], greaterThanResults.Select(a => a.Username).ToArray());
    }

    [Fact]
    public async Task SearchAsync_FindsByDateTimeComparisons()
    {
        var db = LottaDBFixture.CreateDb();
        var created1 = new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc);
        var created2 = new DateTime(2024, 1, 2, 8, 0, 0, DateTimeKind.Utc);
        var created3 = new DateTime(2024, 1, 3, 8, 0, 0, DateTimeKind.Utc);

        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice", CreatedAt = created1 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "bob", DisplayName = "Bob", CreatedAt = created2 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "carol", DisplayName = "Carol", CreatedAt = created3 }, TestContext.Current.CancellationToken);

        var equalsResults = db.Search<Actor>()
            .Where(a => a.CreatedAt == created2)
            .ToList();
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = db.Search<Actor>()
            .Where(a => a.CreatedAt < created2)
            .ToList();
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = db.Search<Actor>()
            .Where(a => a.CreatedAt > created2)
            .ToList();
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }

    [Fact]
    public async Task SearchAsync_FindsByDateTimeOffsetComparisons()
    {
        var db = LottaDBFixture.CreateDb();
        var seen1 = new DateTimeOffset(2024, 2, 1, 8, 0, 0, TimeSpan.Zero);
        var seen2 = new DateTimeOffset(2024, 2, 2, 8, 0, 0, TimeSpan.Zero);
        var seen3 = new DateTimeOffset(2024, 2, 3, 8, 0, 0, TimeSpan.Zero);

        await db.SaveAsync(new Actor { Domain = "search.test", Username = "alice", DisplayName = "Alice", LastSeenAt = seen1 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "bob", DisplayName = "Bob", LastSeenAt = seen2 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "carol", DisplayName = "Carol", LastSeenAt = seen3 }, TestContext.Current.CancellationToken);

        var equalsResults = db.Search<Actor>()
            .Where(a => a.LastSeenAt == seen2)
            .ToList();
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = db.Search<Actor>()
            .Where(a => a.LastSeenAt < seen2)
            .ToList();
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = db.Search<Actor>()
            .Where(a => a.LastSeenAt > seen2)
            .ToList();
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }

    [Fact]
    public async Task SearchAsync_FilterByContent()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Note { Domain = "search.test", NoteId = "n1", AuthorId = "alice", Content = "Lucene is great for full text search", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "search.test", NoteId = "n2", AuthorId = "bob", Content = "Azure table storage is fast", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        // Filter by a non-analyzed field (AuthorId has NotAnalyzed)
        var results = db.Search<Note>()
            .Where(n => n.Content.Contains("lucene"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
        results = db.Search<Note>("lucene")
            .ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchAsync_Take_LimitsResults()
    {
        var db = LottaDBFixture.CreateDb();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "search.test", Username = $"user-{i}", DisplayName = $"User {i}" }, TestContext.Current.CancellationToken);

        var results = db.Search<Actor>().Take(3).ToList();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task SearchAsync_RemovedFromIndex_OnDelete()
    {
        var db = LottaDBFixture.CreateDb();
        var actor = new Actor { Domain = "search.test", Username = "deletable", DisplayName = "Gone" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);

        // Verify it's in the index
        var before = db.Search<Actor>()
            .Where(a => a.Username == "deletable")
            .ToList();
        Assert.Single(before);

        // Delete and verify it's removed
        await db.DeleteAsync(actor, TestContext.Current.CancellationToken);
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
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "updatable", DisplayName = "Before" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "updatable", DisplayName = "After" }, TestContext.Current.CancellationToken);

        var results = db.Search<Actor>()
            .Where(a => a.Username == "updatable")
            .ToList();
        Assert.Single(results);
        Assert.Equal("After", results[0].DisplayName);
    }

    [Fact]
    public async Task SearchAsync_ChangeAsync_ReflectsInIndex()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Domain = "search.test", Username = "changed", DisplayName = "Before", Counter = 1 }, TestContext.Current.CancellationToken);

        await db.ChangeAsync<Actor>("changed", actor =>
        {
            actor.DisplayName = "After";
            actor.Counter = 2;
            return actor;
        }, TestContext.Current.CancellationToken);

        var results = db.Search<Actor>()
            .Where(a => a.Username == "changed")
            .ToList();
        Assert.Single(results);
        Assert.Equal("After", results[0].DisplayName);
        Assert.Equal(2, results[0].Counter);
    }
}
