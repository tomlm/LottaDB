namespace Lotta.Tests;

public class RebuildIndexTests
{
    [Fact]
    public async Task RebuildIndex_RestoresSearchResults()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        // Verify search works
        var before = db.Search<Actor>().ToList();
        Assert.Equal(2, before.Count);

        // Rebuild the index (simulates recovery after Lucene data loss)
        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        // Should still find everything
        var after = db.Search<Actor>().ToList();
        Assert.Equal(2, after.Count);
    }

    [Fact]
    public async Task RebuildIndex_EmptyTable_CreatesEmptyIndex()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        // Rebuild with no data — should not throw
        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        var results = db.Search<Actor>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task RebuildIndex_DoesNotRerunBuilders()
    {
        // Rebuild only re-indexes from table storage, does not re-run On<T> handlers
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, AuthorDisplay = actor?.DisplayName ?? "", Content = note.Content });
            });
        });

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "auth", DisplayName = "Auth" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { Domain = "rebuild.test", NoteId = "rn1", AuthorId = "auth", Content = "Test", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        // NoteView should exist from the builder
        var viewBefore = db.Search<NoteView>().ToList();
        Assert.Single(viewBefore);

        // Rebuild only Actor index — should not affect NoteViews
        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        // NoteView should still be searchable (its index wasn't rebuilt/cleared)
        var viewAfter = db.Search<NoteView>().ToList();
        Assert.Single(viewAfter);
    }

    [Fact]
    public async Task ResetAsync_ClearsTableAndIndex()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Username = "reset1", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "rn1", AuthorId = "reset1", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        // Verify data exists
        Assert.NotNull(await db.GetAsync<Actor>("reset1", TestContext.Current.CancellationToken));
        Assert.Equal(1, db.Search<Actor>().Count());

        await db.ResetDatabaseAsync(TestContext.Current.CancellationToken);

        // Table storage cleared
        Assert.Null(await db.GetAsync<Actor>("reset1", TestContext.Current.CancellationToken));
        Assert.Null(await db.GetAsync<Note>("rn1", TestContext.Current.CancellationToken));

        // Lucene index cleared
        Assert.Empty(db.Search<Actor>().ToList());
        Assert.Empty(db.Search<Note>().ToList());
    }

    [Fact]
    public async Task ResetAsync_CanSaveAfterReset()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Actor { Username = "before", DisplayName = "Before" }, TestContext.Current.CancellationToken);
        await db.ResetDatabaseAsync(TestContext.Current.CancellationToken);

        // DB should be fully functional after reset
        await db.SaveAsync(new Actor { Username = "after", DisplayName = "After" }, TestContext.Current.CancellationToken);
        var loaded = await db.GetAsync<Actor>("after", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("After", loaded.DisplayName);

        // Old data still gone
        Assert.Null(await db.GetAsync<Actor>("before", TestContext.Current.CancellationToken));

        // Search works for new data
        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("after", results[0].Username);
    }
}
