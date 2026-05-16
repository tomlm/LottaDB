
namespace Lotta.Tests;

public class ToAsyncEnumerableTests
{
    // ===== QueryAsync<T>().ToAsyncEnumerable() =====

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_ReturnsAllItems()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "async.test", Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "async.test", Username = "bob" }, ct);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(cancellationToken: ct))
            results.Add(item);

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_PagesAcrossMultiplePages()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Domain = "async.test", Username = $"user-{i}" }, ct);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(maxPerPage: 3, cancellationToken: ct))
            results.Add(item);

        Assert.Equal(10, results.Count);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_EmptySource_YieldsNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(cancellationToken: ct))
            results.Add(item);

        Assert.Empty(results);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_WithFilter_ReturnsFiltered()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Note { Domain = "async.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, ct);
        await db.SaveAsync(new Note { Domain = "async.test", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow }, ct);

        var results = new List<Note>();
        await foreach (var item in db.GetManyAsync<Note>(n => n.AuthorId == "alice", maxPerPage: 10, cancellationToken: ct))
            results.Add(item);

        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task QueryAsync_ToAsyncEnumerable_ExactlyOnePage_YieldsAllItems()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 0; i < 5; i++)
            await db.SaveAsync(new Actor { Domain = "async.test", Username = $"user-{i}" }, ct);

        var results = new List<Actor>();
        await foreach (var item in db.GetManyAsync<Actor>(maxPerPage: 5, cancellationToken: ct))
            results.Add(item);

        Assert.Equal(5, results.Count);
    }


}
