namespace Lotta.Tests;

public class RebuildIndexTests
{
    [Fact]
    public async Task RebuildIndex_RestoresSearchResults()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "bob", DisplayName = "Bob" }, ct);

        // Verify search works
        var before = db.Search<Actor>().ToList();
        Assert.Equal(2, before.Count);

        // Wipe the Lucene index to prove RebuildSearchIndex actually repopulates it
        db.DeleteSearchIndex();
        Assert.Empty(db.Search<Actor>().ToList());

        // Rebuild the index from Table Storage
        await db.RebuildSearchIndex(ct);

        // Should find everything again
        var after = db.Search<Actor>().ToList();
        Assert.Equal(2, after.Count);
    }

    [Fact]
    public async Task RebuildIndex_EmptyTable_CreatesEmptyIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // Rebuild with no data — should not throw
        await db.RebuildSearchIndex(ct);

        var results = db.Search<Actor>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task RebuildIndex_DoesNotRerunBuilders()
    {
        var ct = TestContext.Current.CancellationToken;
        // Rebuild only re-indexes from table storage, does not re-run On<T> handlers
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, AuthorDisplay = actor?.DisplayName ?? "", Content = note.Content });
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "auth", DisplayName = "Auth" }, ct);
        await db.SaveAsync(new Note { Domain = "rebuild.test", NoteId = "rn1", AuthorId = "auth", Content = "Test", Published = DateTimeOffset.UtcNow }, ct);

        // NoteView should exist from the builder
        var viewBefore = db.Search<NoteView>().ToList();
        Assert.Single(viewBefore);

        // Rebuild only Actor index — should not affect NoteViews
        await db.RebuildSearchIndex(ct);

        // NoteView should still be searchable (its index wasn't rebuilt/cleared)
        var viewAfter = db.Search<NoteView>().ToList();
        Assert.Single(viewAfter);
    }

    [Fact]
    public async Task ResetAsync_ClearsTableAndIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "reset1", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "rn1", AuthorId = "reset1", Content = "Hello", Published = DateTimeOffset.UtcNow }, ct);

        // Verify data exists
        Assert.NotNull(await db.GetAsync<Actor>("reset1", ct));
        Assert.Equal(1, db.Search<Actor>().Count());

        await db.ResetDatabaseAsync(ct);

        // Table storage cleared
        Assert.Null(await db.GetAsync<Actor>("reset1", ct));
        Assert.Null(await db.GetAsync<Note>("rn1", ct));

        // Lucene index cleared
        Assert.Empty(db.Search<Actor>().ToList());
        Assert.Empty(db.Search<Note>().ToList());
    }

    [Fact]
    public async Task ResetAsync_CanSaveAfterReset()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "before", DisplayName = "Before" }, ct);
        await db.ResetDatabaseAsync(ct);

        // DB should be fully functional after reset
        await db.SaveAsync(new Actor { Username = "after", DisplayName = "After" }, ct);
        var loaded = await db.GetAsync<Actor>("after", ct);
        Assert.NotNull(loaded);
        Assert.Equal("After", loaded.DisplayName);

        // Old data still gone
        Assert.Null(await db.GetAsync<Actor>("before", ct));

        // Search works for new data
        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("after", results[0].Username);
    }
}
