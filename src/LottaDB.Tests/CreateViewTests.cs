namespace Lotta.Tests;

/// <summary>
/// Tests for On&lt;T&gt; handlers that create materialized views (NoteView from Note+Actor).
/// Replaces the old CreateView LINQ expression tests.
/// </summary>
public class CreateViewTests
{
    private LottaDB CreateDbWithNoteViewHandler()
    {
        return LottaDBFixture.CreateDb(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<NoteView>(note.NoteId);
                    return;
                }
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                if (actor == null) return; // no matching actor, no view
                await db.SaveAsync(new NoteView
                {
                    Domain = note.Domain, NoteId = note.NoteId,
                    AuthorUsername = actor.Username, AuthorDisplay = actor.DisplayName,
                    AvatarUrl = actor.AvatarUrl, Content = note.Content,
                    Published = note.Published, Tags = note.Tags.ToArray(),
                });
            });

            opts.On<Actor>(async (actor, kind, db) =>
            {
                // Find all NoteViews for this actor and rebuild or delete
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
                        AvatarUrl = actor.AvatarUrl, Content = note.Content,
                        Published = note.Published, Tags = note.Tags.ToArray(),
                    });
                }
            });
        });
    }

    [Fact]
    public async Task NoteAndActor_ProducesNoteView()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "alice", DisplayName = "Alice", AvatarUrl = "https://a.com/alice.png" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "n1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("n1");
        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
        Assert.Equal("Hello world", view.Content);
    }

    [Fact]
    public async Task ActorChange_UpdatesNoteView()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "updater", DisplayName = "Before" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "n-update", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow });

        Assert.Equal("Before", (await db.GetAsync<NoteView>("n-update"))!.AuthorDisplay);

        await db.SaveAsync(new Actor { Domain = "view.test", Username = "updater", DisplayName = "After" });

        Assert.Equal("After", (await db.GetAsync<NoteView>("n-update"))!.AuthorDisplay);
    }

    [Fact]
    public async Task NoteDeleted_DeletesNoteView()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "deleter", DisplayName = "D" });
        var note = new Note { Domain = "view.test", NoteId = "n-del", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);
        Assert.NotNull(await db.GetAsync<NoteView>("n-del"));

        await db.DeleteAsync(note);
        Assert.Null(await db.GetAsync<NoteView>("n-del"));
    }

    [Fact]
    public async Task ActorDeleted_DeletesRelatedNoteViews()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "gone", DisplayName = "Gone" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "orphan1", AuthorId = "gone", Content = "A", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "orphan2", AuthorId = "gone", Content = "B", Published = DateTimeOffset.UtcNow });

        Assert.NotNull(await db.GetAsync<NoteView>("orphan1"));
        Assert.NotNull(await db.GetAsync<NoteView>("orphan2"));

        await db.DeleteAsync<Actor>("gone");

        Assert.Null(await db.GetAsync<NoteView>("orphan1"));
        Assert.Null(await db.GetAsync<NoteView>("orphan2"));
    }

    [Fact]
    public async Task NoMatchingActor_NoViewCreated()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "n-orphan", AuthorId = "nobody", Content = "Orphan", Published = DateTimeOffset.UtcNow });

        Assert.Null(await db.GetAsync<NoteView>("n-orphan"));
    }

    [Fact]
    public async Task MultipleNotes_SameActor_AllViews()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "prolific", DisplayName = "Prolific" });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "p1", AuthorId = "prolific", Content = "One", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "p2", AuthorId = "prolific", Content = "Two", Published = DateTimeOffset.UtcNow });
        await db.SaveAsync(new Note { Domain = "view.test", NoteId = "p3", AuthorId = "prolific", Content = "Three", Published = DateTimeOffset.UtcNow });

        var views = db.Search<NoteView>()
            .Where(v => v.AuthorUsername == "prolific").ToList();
        Assert.Equal(3, views.Count);
    }

    [Fact]
    public async Task SaveResult_ContainsDerivedChanges()
    {
        var db = CreateDbWithNoteViewHandler();
        await db.SaveAsync(new Actor { Domain = "view.test", Username = "result", DisplayName = "R" });
        var result = await db.SaveAsync(new Note { Domain = "view.test", NoteId = "result-n", AuthorId = "result", Content = "Check result", Published = DateTimeOffset.UtcNow });

        Assert.Contains(result.Changes, c => c.TypeName == nameof(Note));
        Assert.Contains(result.Changes, c => c.TypeName == nameof(NoteView));
    }
}
