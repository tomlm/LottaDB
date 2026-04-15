namespace Lotta.Tests;

public class CascadingViewTests
{
    private LottaDB CreateDbWithCascadingHandlers()
    {
        return LottaDBFixture.CreateDb(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<NoteView>($"nv-{note.NoteId}");
                    return;
                }
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                if (actor == null) return;
                await db.SaveAsync(new NoteView
                {
                    Domain = note.Domain, Id = $"nv-{note.NoteId}", NoteId = note.NoteId,
                    AuthorUsername = actor.Username, AuthorDisplay = actor.DisplayName,
                    AvatarUrl = actor.AvatarUrl, Content = note.Content,
                    Published = note.Published,
                });
            });

            opts.On<Actor>(async (actor, kind, db) =>
            {
                var affected = db.Search<NoteView>()
                    .Where(v => v.AuthorUsername == actor.Username).ToList();
                foreach (var view in affected)
                {
                    if (kind == TriggerKind.Deleted)
                    {
                        await db.DeleteAsync<NoteView>(view.Id);
                        continue;
                    }
                    var noteKey = view.NoteId;
                    var note = await db.GetAsync<Note>(noteKey);
                    if (note == null) continue;
                    await db.SaveAsync(new NoteView
                    {
                        Domain = note.Domain, Id = view.Id, NoteId = view.NoteId,
                        AuthorUsername = actor.Username, AuthorDisplay = actor.DisplayName,
                        Content = note.Content, Published = note.Published,
                    });
                }
            });

            opts.On<NoteView>(async (nv, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<FeedEntry>($"fe-{nv.Id}");
                    return;
                }
                await db.SaveAsync(new FeedEntry
                {
                    Domain = nv.Domain, Id = $"fe-{nv.Id}", NoteViewId = nv.Id,
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
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Note { NoteId = "c1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow });

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c1"));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c1"));
    }

    [Fact]
    public async Task CascadingView_ActorChange_UpdatesBothViews()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "Before" });
        await db.SaveAsync(new Note { NoteId = "c2", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow });

        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "After" });

        Assert.Equal("After", (await db.GetAsync<NoteView>("nv-c2"))!.AuthorDisplay);
        Assert.Contains("After", (await db.GetAsync<FeedEntry>("fe-nv-c2"))!.Title);
    }

    [Fact]
    public async Task CascadingView_NoteDeleted_DeletesBothViews()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Username = "deleter", DisplayName = "D" });
        var note = new Note { NoteId = "c3", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c3"));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c3"));

        await db.DeleteAsync(note);

        Assert.Null(await db.GetAsync<NoteView>("nv-c3"));
        Assert.Null(await db.GetAsync<FeedEntry>("fe-nv-c3"));
    }

    [Fact]
    public async Task CascadingView_ActorDeleted_DeletesAllViews()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Username = "gone-actor", DisplayName = "Gone" });
        await db.SaveAsync(new Note { NoteId = "c4", AuthorId = "gone-actor", Content = "A", Published = DateTimeOffset.UtcNow });

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c4"));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c4"));

        await db.DeleteAsync<Actor>("gone-actor");

        Assert.Null(await db.GetAsync<NoteView>("nv-c4"));
        Assert.Null(await db.GetAsync<FeedEntry>("fe-nv-c4"));
    }

    [Fact]
    public async Task CascadingView_ResultContainsAllChanges()
    {
        var db = CreateDbWithCascadingHandlers();
        await db.SaveAsync(new Actor { Username = "result-actor", DisplayName = "R" });
        var result = await db.SaveAsync(new Note { NoteId = "c5", AuthorId = "result-actor", Content = "Chain", Published = DateTimeOffset.UtcNow });

        Assert.Contains(result.Changes, c => c.Type == typeof(Note));
        Assert.Contains(result.Changes, c => c.Type == typeof(NoteView));
        Assert.Contains(result.Changes, c => c.Type == typeof(FeedEntry));
    }
}
