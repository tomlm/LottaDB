using Lotta.Internal;

namespace Lotta.Tests;

public class GetManyAsyncTests
{
    [Fact]
    public async Task GetManyAsync_NoKeys_ReturnsAllEntities()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_EmptyTable_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(all);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_DeserializesPolymorphically()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Alice", Email = "alice@test.com", Department = "Eng" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person1", Name = "Bob", Email = "bob@test.com" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, all.Count);
        Assert.Contains(all, o => o is Employee);
        Assert.Contains(all, o => o is Person && o is not Employee);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_WithMaxPerPage_ReturnsAll()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 5; i++)
            await db.SaveAsync(new Actor { Domain = "bulk.test", Username = $"user-{i}" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, maxPerPage: 2,
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(5, all.Count);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_ReturnsOnlyMatchingEntities()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "carol" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "carol" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        var actors = results.Cast<Actor>().OrderBy(a => a.Username).ToList();
        Assert.Equal("alice", actors[0].Username);
        Assert.Equal("carol", actors[1].Username);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_NonExistentKeys_ReturnsOnlyExisting()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "nonexistent" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(results);
        Assert.Equal("alice", ((Actor)results[0]).Username);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_EmptyKeyList_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, Enumerable.Empty<string>(),
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(results);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_DeserializesPolymorphically()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Alice", Email = "alice@test.com", Department = "Eng" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person1", Name = "Bob", Email = "bob@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BaseEntity { Id = "base1", Name = "Carol" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "emp1", "person1" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is Employee e && e.Department == "Eng");
        Assert.Contains(results, o => o is Person p && p.Email == "bob@test.com" && o is not Employee);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_SingleKey_ReturnsSingleResult()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "bob" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(results);
        Assert.Equal("Bob", ((Actor)results[0]).DisplayName);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_MixedTypes_ReturnsAllRequested()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "n1" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is Actor);
        Assert.Contains(results, o => o is Note);
    }
}
