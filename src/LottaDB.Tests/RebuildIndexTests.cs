namespace Lotta.Tests;

public class RebuildIndexTests
{
    [Fact]
    public async Task RebuildIndex_RestoresSearchResults()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "bob", DisplayName = "Bob" });

        // Verify search works
        var before = db.Search<Actor>().ToList();
        Assert.Equal(2, before.Count);

        // Rebuild the index (simulates recovery after Lucene data loss)
        await db.RebuildIndex();

        // Should still find everything
        var after = db.Search<Actor>().ToList();
        Assert.Equal(2, after.Count);
    }

    [Fact]
    public async Task RebuildIndex_EmptyTable_CreatesEmptyIndex()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        // Rebuild with no data — should not throw
        await db.RebuildIndex();

        var results = db.Search<Actor>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task RebuildIndex_DoesNotRerunBuilders()
    {
        // Rebuild only re-indexes from table storage, does not re-derive
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, NoteView, NoteViewExplicitBuilder>());

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "auth", DisplayName = "Auth" });
        await db.SaveAsync(new Note { Domain = "rebuild.test", NoteId = "rn1", AuthorId = "auth", Content = "Test", Published = DateTimeOffset.UtcNow });

        // NoteView should exist from the builder
        var viewBefore = db.Search<NoteView>().ToList();
        Assert.Single(viewBefore);

        // Rebuild only Actor index — should not affect NoteViews
        await db.RebuildIndex();

        // NoteView should still be searchable (its index wasn't rebuilt/cleared)
        var viewAfter = db.Search<NoteView>().ToList();
        Assert.Single(viewAfter);
    }
}
