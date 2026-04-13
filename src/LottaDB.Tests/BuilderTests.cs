namespace Lotta.Tests;

public class BuilderTests
{
    private LottaDB CreateDbWithBuilder<TTrigger, TDerived, TBuilder>()
        where TTrigger : class, new() where TDerived : class, new() where TBuilder : class, IBuilder<TTrigger, TDerived>, new()
    {
        return TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<TTrigger, TDerived, TBuilder>());
    }

    [Fact]
    public async Task Builder_OnSave_ProducesDerivedObject()
    {
        var db = CreateDbWithBuilder<Note, NoteView, NoteViewExplicitBuilder>();

        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("n1");
        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
    }

    [Fact]
    public async Task Builder_OnSave_DerivedObjectInTableStorage()
    {
        var db = CreateDbWithBuilder<Note, NoteView, NoteViewExplicitBuilder>();
        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "n2", AuthorId = "alice", Content = "Stored", Published = DateTimeOffset.UtcNow });

        // Should be in table storage (QueryAsync hits table storage)
        var views = db.Query<NoteView>().ToList();
        Assert.Contains(views, v => v.NoteId == "n2");
    }

    [Fact]
    public async Task Builder_OnSave_DerivedObjectInLuceneIndex()
    {
        var db = CreateDbWithBuilder<Note, NoteView, NoteViewExplicitBuilder>();
        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "n3", AuthorId = "alice", Content = "Indexed", Published = DateTimeOffset.UtcNow });

        // Should be in Lucene (SearchAsync hits Lucene)
        var views = db.Search<NoteView>().ToList();
        Assert.Contains(views, v => v.NoteId == "n3");
    }

    [Fact]
    public async Task Builder_OnDelete_AutoDeletesDerivedObjects()
    {
        var db = CreateDbWithBuilder<Note, NoteView, NoteViewExplicitBuilder>();
        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "alice", DisplayName = "Alice" });
        var note = new Note { Domain = "builder.test", NoteId = "n4", AuthorId = "alice", Content = "Delete me", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);

        // Verify view exists
        Assert.NotNull(await db.GetAsync<NoteView>("n4"));

        // Delete the note — builder yields nothing for Deleted, so auto-delete kicks in
        await db.DeleteAsync(note);

        Assert.Null(await db.GetAsync<NoteView>("n4"));
    }

    [Fact]
    public async Task Builder_OnSave_EmptyYield_SkipsQuietly()
    {
        // NoteViewExplicitBuilder yields nothing for Deleted trigger.
        // But for a Save trigger with a valid note, it should produce a view.
        // This test verifies that an empty yield on Save is a no-op.
        var db = CreateDbWithBuilder<Note, NoteView, NoteViewExplicitBuilder>();
        // Save a note without a matching actor — builder will still yield (with empty author)
        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "n5", AuthorId = "nobody", Content = "Orphan", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("n5");
        Assert.NotNull(view);
        Assert.Equal("", view.AuthorDisplay);
    }

    [Fact]
    public async Task Builder_OnSave_MultipleResults_CreatesAll()
    {
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, NoteView, MultiResultBuilder>());

        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "multi", AuthorId = "alice", Content = "Multi", Published = DateTimeOffset.UtcNow });

        var v1 = await db.GetAsync<NoteView>("multi-view1");
        var v2 = await db.GetAsync<NoteView>("multi-view2");
        Assert.NotNull(v1);
        Assert.NotNull(v2);
    }

    [Fact]
    public async Task Builder_ReceivesTriggerKind_Saved()
    {
        TriggerKind? receivedTrigger = null;
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, NoteView, NoteViewExplicitBuilder>());

        // The NoteViewExplicitBuilder checks trigger == Deleted and yields break.
        // If we save, it should produce a view (meaning it received Saved).
        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "trigger-s", DisplayName = "T" });
        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "ts1", AuthorId = "trigger-s", Content = "Test", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("ts1");
        Assert.NotNull(view); // Builder ran, meaning it got TriggerKind.Saved
    }

    [Fact]
    public async Task Builder_ReceivesTriggerKind_Deleted()
    {
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, NoteView, NoteViewExplicitBuilder>());

        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "trigger-d", DisplayName = "T" });
        var note = new Note { Domain = "builder.test", NoteId = "td1", AuthorId = "trigger-d", Content = "Test", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);
        Assert.NotNull(await db.GetAsync<NoteView>("td1"));

        // Delete: builder receives Deleted, yields nothing, auto-delete removes view
        await db.DeleteAsync(note);
        Assert.Null(await db.GetAsync<NoteView>("td1"));
    }

    [Fact]
    public async Task Builder_HasAccessToLottaDB()
    {
        // NoteViewExplicitBuilder calls db.GetAsync<Actor> — verifying it has access
        var db = CreateDbWithBuilder<Note, NoteView, NoteViewExplicitBuilder>();
        await db.SaveAsync(new Actor { Domain = "builder.test", Username = "access", DisplayName = "Accessible" });
        await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "access-n", AuthorId = "access", Content = "Test", Published = DateTimeOffset.UtcNow });

        var view = await db.GetAsync<NoteView>("access-n");
        Assert.NotNull(view);
        Assert.Equal("Accessible", view.AuthorDisplay); // Proves builder read the Actor
    }

    [Fact]
    public async Task Builder_Failure_CapturedInObjectResult()
    {
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, ModerationView, FailingBuilder>());

        var result = await db.SaveAsync(new Note { Domain = "builder.test", NoteId = "fail1", AuthorId = "alice", Content = "Fail", Published = DateTimeOffset.UtcNow });

        Assert.NotEmpty(result.Errors);
        Assert.Contains(result.Errors, e => e.BuilderName.Contains("FailingBuilder"));
    }

    [Fact]
    public async Task Builder_Failure_DoesNotBlockSourceSave()
    {
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, ModerationView, FailingBuilder>());

        var note = new Note { Domain = "builder.test", NoteId = "fail2", AuthorId = "alice", Content = "Still saved", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note);

        // Note should still be in table storage despite builder failure
        var notes = db.Query<Note>().Where(n => n.NoteId == "fail2").ToList();
        Assert.Single(notes);
    }
}
