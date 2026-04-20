
namespace Lotta.Tests;

public class ToAsyncEnumerableTests
{
    // ===== QueryAsync<T>().ToAsyncEnumerable() =====

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_ReturnsAllItems()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "async.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "async.test", Username = "bob" }, TestContext.Current.CancellationToken);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(cancellationToken: TestContext.Current.CancellationToken))
            results.Add(item);

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_PagesAcrossMultiplePages()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "async.test", Username = $"user-{i}" }, TestContext.Current.CancellationToken);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(maxPerPage: 3, cancellationToken: TestContext.Current.CancellationToken))
            results.Add(item);

        Assert.Equal(10, results.Count);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_EmptySource_YieldsNothing()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(cancellationToken: TestContext.Current.CancellationToken))
            results.Add(item);

        Assert.Empty(results);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_WithFilter_ReturnsFiltered()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Note { Domain = "async.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "async.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var results = new List<Note>();
        await foreach (var item in db.GetManyAsync<Note>(n => n.AuthorId == "alice", maxPerPage: 10, cancellationToken: TestContext.Current.CancellationToken))
            results.Add(item);

        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_ExactlyOnePage_YieldsAllItems()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 5; i++)
            await db.SaveAsync(new Actor { Domain = "async.test", Username = $"user-{i}" }, TestContext.Current.CancellationToken);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(maxPerPage: 5, cancellationToken: TestContext.Current.CancellationToken))
            results.Add(item);

        Assert.Equal(5, results.Count);
    }


}
