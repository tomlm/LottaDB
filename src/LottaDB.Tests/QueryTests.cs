namespace Lotta.Tests;

public class QueryTests
{
    [Fact]
    public async Task QueryAsync_ReturnsAllOfType()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob" }, TestContext.Current.CancellationToken);

        var all = db.GetMany<Actor>().ToList();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task QueryAsync_FilterByTag_ServerSide()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var aliceNotes = db.GetMany<Note>(n => n.AuthorId == "alice")
            .ToList();

        Assert.Single(aliceNotes);
        Assert.Equal("n1", aliceNotes[0].NoteId);
    }

    [Fact]
    public async Task QueryAsync_Take_LimitsResults()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "query.test", Username = $"user-{i}" }, TestContext.Current.CancellationToken);

        var limited = db.GetMany<Actor>().Take(3).ToList();
        Assert.Equal(3, limited.Count);
    }

    [Fact]
    public async Task QueryAsync_EmptyTable_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var all = db.GetMany<Actor>().ToList();
        Assert.Empty(all);
    }

    [Fact]
    public async Task QueryAsync_WhereOnNonTag_EvaluatesClientSide()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "" }, TestContext.Current.CancellationToken);

        // AvatarUrl is not a tag — should still filter (client-side)
        var withAvatar = db.GetMany<Actor>()
            .Where(a => a.AvatarUrl != "")
            .ToList();
        Assert.Single(withAvatar);
        Assert.Equal("alice", withAvatar[0].Username);
    }

    [Fact]
    public async Task QueryAsync_CombinedTagAndNonTag_FiltersCorrectly()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alex", DisplayName = "Alice", AvatarUrl = "" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "https://example.com/bob.png" }, TestContext.Current.CancellationToken);

        var matches = db.GetMany<Actor>(a => a.DisplayName == "Alice")
            .Where(a => a.AvatarUrl != "")
            .ToList();

        Assert.Single(matches);
        Assert.Equal("alice", matches[0].Username);
    }

    [Fact]
    public async Task QueryAsync_OrAcrossTags_FiltersCorrectly()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", DisplayName = "Carol" }, TestContext.Current.CancellationToken);

        var matches = db.GetMany<Actor>(a => a.DisplayName == "Alice" || a.DisplayName == "Bob")
            .OrderBy(a => a.Username)
            .ToList();

        Assert.Equal(2, matches.Count);
        Assert.Equal("alice", matches[0].Username);
        Assert.Equal("bob", matches[1].Username);
    }

    [Fact]
    public async Task QueryAsync_FindsByNumericComparisons()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", Counter = 5 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", Counter = 10 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", Counter = 15 }, TestContext.Current.CancellationToken);

        var equalsResults = db.GetMany<Actor>(a => a.Counter == 10)
            .ToList();
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = db.GetMany<Actor>(a => a.Counter < 10)
            .ToList();
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = db.GetMany<Actor>(a => a.Counter > 10)
            .ToList();
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }

    [Fact]
    public async Task QueryAsync_FindsByDateTimeComparisons()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var created1 = new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc);
        var created2 = new DateTime(2024, 1, 2, 8, 0, 0, DateTimeKind.Utc);
        var created3 = new DateTime(2024, 1, 3, 8, 0, 0, DateTimeKind.Utc);

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", CreatedAt = created1 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", CreatedAt = created2 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", CreatedAt = created3 }, TestContext.Current.CancellationToken);

        var equalsResults = db.GetMany<Actor>(a => a.CreatedAt == created2)
            .ToList();
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = db.GetMany<Actor>(a => a.CreatedAt < created2)
            .ToList();
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = db.GetMany<Actor>(a => a.CreatedAt > created2)
            .ToList();
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }

    [Fact]
    public async Task QueryAsync_FindsByDateTimeOffsetComparisons()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var seen1 = new DateTimeOffset(2024, 2, 1, 8, 0, 0, TimeSpan.Zero);
        var seen2 = new DateTimeOffset(2024, 2, 2, 8, 0, 0, TimeSpan.Zero);
        var seen3 = new DateTimeOffset(2024, 2, 3, 8, 0, 0, TimeSpan.Zero);

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", LastSeenAt = seen1 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", LastSeenAt = seen2 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", LastSeenAt = seen3 }, TestContext.Current.CancellationToken);

        var equalsResults = db.GetMany<Actor>(a => a.LastSeenAt == seen2)
            .ToList();
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = db.GetMany<Actor>(a => a.LastSeenAt < seen2)
            .ToList();
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = db.GetMany<Actor>(a => a.LastSeenAt > seen2)
            .ToList();
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }
}
