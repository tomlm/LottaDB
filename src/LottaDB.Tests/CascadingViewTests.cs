namespace Lotta.Tests;

public class CascadingViewTests
{
    private LottaDB CreateDbWithCascadingHandlers()
    {
        return LottaDBFixture.CreateDb(opts =>
        {
            // Note → NoteView
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<NoteView>(note.NoteId);
                    return;
                }
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                if (actor == null) return;
                await db.SaveAsync(new NoteView
                {
                    Domain = note.Domain, NoteId = note.NoteId,
                    AuthorUsername = actor.Username, AuthorDisplay = actor.DisplayName,
                    AvatarUrl = actor.AvatarUrl, Content = note.Content,
                    Published = note.Published,
                });
            });

            // Actor → rebuild NoteViews
            opts.On<Actor>(async (actor, kind, db) =>
            {
                var affected = db.Search<NoteView>()
                    .Where(v => v.AuthorUsername == actor.Username).ToList();
                foreach (var view in affected)
                {
                    if (kind == TriggerKind.Deleted)
                    {
                        await db.DeleteAsync<NoteView>(view.NoteId);
                        continue;
                    }
                    var note = await db.GetAsync<Note>(view.NoteId);
                    if (note == null) continue;
                    await db.SaveAsync(new NoteView
                    {
                        Domain = note.Domain, NoteId = note.NoteId,
                        AuthorUsername = actor.Username, AuthorDisplay = actor.DisplayName,
                        Content = note.Content, Published = note.Published,
                    });
                }
            });

            // NoteView → FeedEntry (cascading!)
            opts.On<NoteView>(async (nv, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<FeedEntry>(nv.NoteId);
                    return;
                }
                await db.SaveAsync(new FeedEntry
                {
                    Domain = nv.Domain, FeedEntryId = nv.NoteId,
                    Title = $"{nv.AuthorDisplay}: {nv.Content}",
                    Published = nv.Published,
                });
            });
        });
    }

    [Fact]
    public async Task CascadingView_NoteCreates_NoteViewAndFeedEntry()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow });

        Assert.NotNull(await db.GetAsync<NoteView>("c1"));
        Assert.NotNull(await db.GetAsync<FeedEntry>("c1"));
    }

    [Fact]
    public async Task CascadingView_ActorChange_UpdatesBothViews()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "updater", DisplayName = "Before" });
        await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c2", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow });

        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "updater", DisplayName = "After" });

        Assert.Equal("After", (await db.GetAsync<NoteView>("c2"))!.AuthorDisplay);
        Assert.Contains("After", (await db.GetAsync<FeedEntry>("c2"))!.Title);
    }

    [Fact]
    public async Task CascadingView_NoteDeleted_DeletesBothViews()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "deleter", DisplayName = "D" });
        var note = new Note { Domain = "cascade.test", NoteId = "c3", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);

        Assert.NotNull(await db.GetAsync<NoteView>("c3"));
        Assert.NotNull(await db.GetAsync<FeedEntry>("c3"));

        await db.DeleteAsync(note);

        Assert.Null(await db.GetAsync<NoteView>("c3"));
        Assert.Null(await db.GetAsync<FeedEntry>("c3"));
    }

    [Fact]
    public async Task CascadingView_ActorDeleted_DeletesAllViews()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "gone", DisplayName = "Gone" });
        await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c4", AuthorId = "gone", Content = "A", Published = DateTimeOffset.UtcNow });

        Assert.NotNull(await db.GetAsync<NoteView>("c4"));
        Assert.NotNull(await db.GetAsync<FeedEntry>("c4"));

        await db.DeleteAsync<Actor>("gone");

        Assert.Null(await db.GetAsync<NoteView>("c4"));
        Assert.Null(await db.GetAsync<FeedEntry>("c4"));
    }

    [Fact]
    public async Task CascadingView_ResultContainsAllChanges()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "result", DisplayName = "R" });
        var result = await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c5", AuthorId = "result", Content = "Chain", Published = DateTimeOffset.UtcNow });

        Assert.Contains(result.Changes, c => c.TypeName == nameof(Note));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(NoteView));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(FeedEntry));
    }
}
