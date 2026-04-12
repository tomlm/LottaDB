namespace LottaDB.Tests;

public class CreateViewTests
{
    private ILottaDB CreateDbWithNoteView()
    {
        return TestLottaDBFactory.CreateWithBuilders(opts =>
        {
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
        });
    }

    [Fact]
    public async Task CreateView_NoteAndActor_ProducesNoteView()
    {
        var db = CreateDbWithNoteView();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://a.com/alice.png" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "n1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("view.test", "n1");
        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
        Assert.Equal("Hello world", view.Content);
    }

    [Fact]
    public async Task CreateView_ActorChange_UpdatesNoteView()
    {
        var db = CreateDbWithNoteView();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "updater", DisplayName = "Before" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "n-update", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow });

        var before = await db.GetAsync<NoteView>("view.test", "n-update");
        Assert.Equal("Before", before!.AuthorDisplay);

        // Update the actor
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "updater", DisplayName = "After" });

        var after = await db.GetAsync<NoteView>("view.test", "n-update");
        Assert.Equal("After", after!.AuthorDisplay);
    }

    [Fact]
    public async Task CreateView_NoteDeleted_DeletesNoteView()
    {
        var db = CreateDbWithNoteView();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "deleter", DisplayName = "D" });
        var note = new Note { Domain = "view.test", NoteId = "n-del", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);
        Assert.NotNull(await db.GetAsync<NoteView>("view.test", "n-del"));

        await db.DeleteAsync(note);
        Assert.Null(await db.GetAsync<NoteView>("view.test", "n-del"));
    }

    [Fact]
    public async Task CreateView_NoMatchingJoin_NoViewCreated()
    {
        var db = CreateDbWithNoteView();
        // Save a note without a matching actor
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "n-orphan", AuthorId = "nobody", Content = "Orphan", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("view.test", "n-orphan");
        Assert.Null(view); // No matching actor, so no NoteView produced
    }

    [Fact]
    public async Task CreateView_MultipleNotes_SameActor_AllViews()
    {
        var db = CreateDbWithNoteView();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "prolific", DisplayName = "Prolific" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "p1", AuthorId = "prolific", Content = "One", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "p2", AuthorId = "prolific", Content = "Two", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "p3", AuthorId = "prolific", Content = "Three", Published = DateTimeOffset.UtcNow });

        var views = db.Search<NoteView>()
            .Where(v => v.AuthorUsername == "prolific")
            .ToList();
        Assert.Equal(3, views.Count);
    }

    [Fact]
    public async Task CreateView_DerivedObjectInTableAndLucene()
    {
        var db = CreateDbWithNoteView();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "dual", DisplayName = "Dual" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "dual-n", AuthorId = "dual", Content = "Both stores", Published = DateTimeOffset.UtcNow });

        // Table storage
        var fromTable = await db.GetAsync<NoteView>("view.test", "dual-n");
        Assert.NotNull(fromTable);

        // Lucene
        var fromLucene = db.Search<NoteView>()
            .Where(v => v.NoteId == "dual-n")
            .ToList();
        Assert.Single(fromLucene);
    }

    [Fact]
    public async Task CreateView_WhereClause_FiltersBeforeJoin()
    {
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
        {
            // Only create views for notes with content containing "important"
            opts.CreateView<NoteView>(d =>
                from note in d.Query<Note>().Where(n => n.Content.Contains("important"))
                join actor in d.Query<Actor>()
                    on note.AuthorId equals actor.Username
                select new NoteView
                {
                    Domain = note.Domain,
                    NoteId = note.NoteId,
                    AuthorUsername = actor.Username,
                    AuthorDisplay = actor.DisplayName,
                    Content = note.Content,
                    Published = note.Published,
                }
            );
        });

        await db.SaveAsync(new Actor { Domain = "view.test", Username = "filter", DisplayName = "Filter" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "imp", AuthorId = "filter", Content = "This is important", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "boring", AuthorId = "filter", Content = "This is boring", Published = DateTimeOffset.UtcNow });

        var views = db.Search<NoteView>().ToList();
        Assert.Single(views);
        Assert.Equal("imp", views[0].NoteId);
    }

    [Fact]
    public async Task CreateView_SaveResult_ContainsDerivedChanges()
    {
        var db = CreateDbWithNoteView();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "result", DisplayName = "R" });
        var result = await db.SaveAsync(new Note { Domain = "view.test", NoteId = "result-n", AuthorId = "result", Content = "Check result", Published = DateTimeOffset.UtcNow });

        // Should contain both the Note save and the NoteView save
        Assert.Contains(result.Changes, c => c.TypeName == nameof(Note));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(NoteView));
    }
}
