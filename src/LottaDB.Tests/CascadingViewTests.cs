namespace LottaDB.Tests;

public class CascadingViewTests
{
    private ILottaDB CreateDbWithCascadingViews()
    {
        return TestLottaDBFactory.CreateWithBuilders(opts =>
        {
            // NoteView depends on Note + Actor
            opts.CreateView<NoteView>(db =>
                from note in db.Query<Note>()
                join actor in db.Query<Actor>()
                    on note.AuthorId equals actor.Username
                select new NoteView
                {
                    Domain = note.Domain,
                    NoteId = note.NoteId,
                    AuthorUsername = actor.Username,
                    AuthorDisplay = actor.DisplayName,
                    AvatarUrl = actor.AvatarUrl,
                    Content = note.Content,
                    Published = note.Published,
                    Tags = note.Tags.ToArray(),
                }
            );

            // FeedEntry depends on NoteView (cascading!)
            opts.CreateView<FeedEntry>(db =>
                from nv in db.Query<NoteView>()
                select new FeedEntry
                {
                    Domain = nv.Domain,
                    FeedEntryId = nv.NoteId,
                    Title = $"{nv.AuthorDisplay}: {nv.Content}",
                    Published = nv.Published,
                }
            );
        });
    }

    [Fact]
    public async Task CascadingView_NoteCreates_NoteViewAndFeedEntry()
    {
        var db = CreateDbWithCascadingViews();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow });

        // NoteView should exist
        var noteView = await db.GetAsync<NoteView>("c1");
        Assert.NotNull(noteView);
        Assert.Equal("Alice", noteView.AuthorDisplay);

        // FeedEntry should also exist (cascaded from NoteView)
        var feedEntry = await db.GetAsync<FeedEntry>("c1");
        Assert.NotNull(feedEntry);
        Assert.Contains("Alice", feedEntry.Title);
    }

    [Fact]
    public async Task CascadingView_ActorChange_UpdatesBothViews()
    {
        var db = CreateDbWithCascadingViews();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "updater", DisplayName = "Before" });
        await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c2", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow });

        // Update actor
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "updater", DisplayName = "After" });

        // NoteView should reflect new display name
        var noteView = await db.GetAsync<NoteView>("c2");
        Assert.NotNull(noteView);
        Assert.Equal("After", noteView.AuthorDisplay);

        // FeedEntry should also update (cascaded)
        var feedEntry = await db.GetAsync<FeedEntry>("c2");
        Assert.NotNull(feedEntry);
        Assert.Contains("After", feedEntry.Title);
    }

    [Fact]
    public async Task CascadingView_ResultContainsAllChanges()
    {
        var db = CreateDbWithCascadingViews();
        await db.SaveAsync(new Actor { Domain = "cascade.test", Username = "result", DisplayName = "R" });
        var result = await db.SaveAsync(new Note { Domain = "cascade.test", NoteId = "c3", AuthorId = "result", Content = "Chain", Published = DateTimeOffset.UtcNow });

        // Result should contain Note + NoteView + FeedEntry
        Assert.Contains(result.Changes, c => c.TypeName == nameof(Note));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(NoteView));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(FeedEntry));
    }
}
