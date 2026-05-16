namespace Lotta.Tests;

public class QueryTests
{
    [Fact]
    public async Task QueryAsync_ReturnsAllOfType()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob" }, ct);

        var all = await db.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task QueryAsync_FilterByTag_ServerSide()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, ct);
        await db.SaveAsync(new Note { Domain = "query.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow }, ct);

        var aliceNotes = await db.GetManyAsync<Note>(n => n.AuthorId == "alice", cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Single(aliceNotes);
        Assert.Equal("n1", aliceNotes[0].NoteId);
    }

    [Fact]
    public async Task QueryAsync_Take_LimitsResults()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "query.test", Username = $"user-{i}" }, ct);

        var limited = await db.GetManyAsync<Actor>(cancellationToken: ct).Take(3).ToListAsync(ct);
        Assert.Equal(3, limited.Count);
    }

    [Fact]
    public async Task QueryAsync_EmptyTable_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var all = await db.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Empty(all);
    }

    [Fact]
    public async Task QueryAsync_WhereOnNonTag_EvaluatesClientSide()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "" }, ct);

        // AvatarUrl is not a tag — should still filter (client-side)
        var withAvatar = await db.GetManyAsync<Actor>(cancellationToken: ct)
            .Where(a => a.AvatarUrl != "")
            .ToListAsync(ct);
        Assert.Single(withAvatar);
        Assert.Equal("alice", withAvatar[0].Username);
    }

    [Fact]
    public async Task QueryAsync_CombinedTagAndNonTag_FiltersCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://example.com/alice.png" }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alex", DisplayName = "Alice", AvatarUrl = "" }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob", AvatarUrl = "https://example.com/bob.png" }, ct);

        var matches = await db.GetManyAsync<Actor>(a => a.DisplayName == "Alice", cancellationToken: ct)
            .Where(a => a.AvatarUrl != "")
            .ToListAsync(ct);

        Assert.Single(matches);
        Assert.Equal("alice", matches[0].Username);
    }

    [Fact]
    public async Task QueryAsync_OrAcrossTags_FiltersCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", DisplayName = "Bob" }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", DisplayName = "Carol" }, ct);

        var matches = await db.GetManyAsync<Actor>(a => a.DisplayName == "Alice" || a.DisplayName == "Bob", cancellationToken: ct)
            .OrderBy(a => a.Username)
            .ToListAsync(ct);

        Assert.Equal(2, matches.Count);
        Assert.Equal("alice", matches[0].Username);
        Assert.Equal("bob", matches[1].Username);
    }

    [Fact]
    public async Task QueryAsync_FindsByNumericComparisons()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", Counter = 5 }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", Counter = 10 }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", Counter = 15 }, ct);

        var equalsResults = await db.GetManyAsync<Actor>(a => a.Counter == 10, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = await db.GetManyAsync<Actor>(a => a.Counter < 10, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = await db.GetManyAsync<Actor>(a => a.Counter > 10, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }

    [Fact]
    public async Task QueryAsync_FindsByDateTimeComparisons()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var created1 = new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc);
        var created2 = new DateTime(2024, 1, 2, 8, 0, 0, DateTimeKind.Utc);
        var created3 = new DateTime(2024, 1, 3, 8, 0, 0, DateTimeKind.Utc);

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", CreatedAt = created1 }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", CreatedAt = created2 }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", CreatedAt = created3 }, ct);

        var equalsResults = await db.GetManyAsync<Actor>(a => a.CreatedAt == created2, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = await db.GetManyAsync<Actor>(a => a.CreatedAt < created2, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = await db.GetManyAsync<Actor>(a => a.CreatedAt > created2, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }

    [Fact]
    public async Task QueryAsync_FindsByDateTimeOffsetComparisons()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var seen1 = new DateTimeOffset(2024, 2, 1, 8, 0, 0, TimeSpan.Zero);
        var seen2 = new DateTimeOffset(2024, 2, 2, 8, 0, 0, TimeSpan.Zero);
        var seen3 = new DateTimeOffset(2024, 2, 3, 8, 0, 0, TimeSpan.Zero);

        await db.SaveAsync(new Actor { Domain = "query.test", Username = "alice", LastSeenAt = seen1 }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "bob", LastSeenAt = seen2 }, ct);
        await db.SaveAsync(new Actor { Domain = "query.test", Username = "carol", LastSeenAt = seen3 }, ct);

        var equalsResults = await db.GetManyAsync<Actor>(a => a.LastSeenAt == seen2, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(equalsResults);
        Assert.Equal("bob", equalsResults[0].Username);

        var lessThanResults = await db.GetManyAsync<Actor>(a => a.LastSeenAt < seen2, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(lessThanResults);
        Assert.Equal("alice", lessThanResults[0].Username);

        var greaterThanResults = await db.GetManyAsync<Actor>(a => a.LastSeenAt > seen2, cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(greaterThanResults);
        Assert.Equal("carol", greaterThanResults[0].Username);
    }
}
