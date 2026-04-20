namespace Lotta.Tests;

/// <summary>
/// Tests for On&lt;T&gt; handler behavior: creating derived objects, handling deletes,
/// error handling, and accessing the DB from within handlers.
/// All derived object IDs are prefixed to ensure global uniqueness (e.g. "nv-" for NoteView).
/// </summary>
public class BuilderTests
{
    [Fact]
    public async Task OnHandler_OnSave_CreatesDerivedObject()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                await db.SaveAsync(new NoteView
                {
                    Domain = note.Domain,
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    AuthorUsername = actor?.Username ?? "",
                    AuthorDisplay = actor?.DisplayName ?? "",
                    Content = note.Content,
                    Published = note.Published,
                });
            });
        });

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var view = await db.GetAsync<NoteView>("nv-n1", TestContext.Current.CancellationToken);
        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
    }

    [Fact]
    public async Task OnHandler_OnSave_DerivedObjectInTableStorage()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        });

        await db.SaveAsync(new Note { NoteId = "n2", Content = "Stored", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var views = db.Query<NoteView>().ToList();
        Assert.Contains(views, v => v.Id == "nv-n2");
    }

    [Fact]
    public async Task OnHandler_OnSave_DerivedObjectInLuceneIndex()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        });

        await db.SaveAsync(new Note { NoteId = "n3", Content = "Indexed", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var views = db.Search<NoteView>().ToList();
        Assert.Contains(views, v => v.Id == "nv-n3");
    }

    [Fact]
    public async Task OnHandler_OnDelete_DeletesDerivedObject()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<NoteView>($"nv-{note.NoteId}");
                    return;
                }
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        });

        var note = new Note { NoteId = "n4", Content = "Delete me", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, TestContext.Current.CancellationToken);
        Assert.NotNull(await db.GetAsync<NoteView>("nv-n4", TestContext.Current.CancellationToken));

        await db.DeleteAsync(note, TestContext.Current.CancellationToken);
        Assert.Null(await db.GetAsync<NoteView>("nv-n4", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OnHandler_ReceivesTriggerKind_Saved()
    {
        TriggerKind? received = null;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) => { received = kind; });
        });

        await db.SaveAsync(new Note { NoteId = "ts1", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        Assert.Equal(TriggerKind.Saved, received);
    }

    [Fact]
    public async Task OnHandler_ReceivesTriggerKind_Deleted()
    {
        TriggerKind? received = null;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) => { received = kind; });
        });

        var note = new Note { NoteId = "td1", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, TestContext.Current.CancellationToken);
        await db.DeleteAsync(note, TestContext.Current.CancellationToken);
        Assert.Equal(TriggerKind.Deleted, received);
    }

    [Fact]
    public async Task OnHandler_HasAccessToDb()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                await db.SaveAsync(new NoteView
                {
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    AuthorDisplay = actor?.DisplayName ?? "unknown",
                });
            });
        });

        await db.SaveAsync(new Actor { Username = "access", DisplayName = "Accessible" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "access-n", AuthorId = "access", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var view = await db.GetAsync<NoteView>("nv-access-n", TestContext.Current.CancellationToken);
        Assert.NotNull(view);
        Assert.Equal("Accessible", view.AuthorDisplay);
    }

    [Fact]
    public async Task OnHandler_Error_DoesNotBlockSourceSave()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                throw new InvalidOperationException("Handler failed");
            });
        });

        var result = await db.SaveAsync(new Note { NoteId = "fail1", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.NotNull(await db.GetAsync<Note>("fail1", TestContext.Current.CancellationToken));
        Assert.NotEmpty(result.Errors);
    }

    [Fact]
    public async Task OnHandler_Error_CapturedInObjectResult()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                throw new InvalidOperationException("Handler failed intentionally");
            });
        });

        var result = await db.SaveAsync(new Note { NoteId = "fail2", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.Single(result.Errors);
        Assert.IsType<InvalidOperationException>(result.Errors[0]);
    }

    [Fact]
    public async Task OnHandler_MultipleHandlers_AllRun()
    {
        int count = 0;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) => { Interlocked.Increment(ref count); });
            opts.On<Note>(async (note, kind, db) => { Interlocked.Increment(ref count); });
        });

        await db.SaveAsync(new Note { NoteId = "multi", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task OnHandler_SaveResult_ContainsDerivedChanges()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        });

        var result = await db.SaveAsync(new Note { NoteId = "result-n", AuthorId = "alice", Content = "Test", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.Contains(result.Changes, c => c.Type == typeof(Note));
        Assert.Contains(result.Changes, c => c.Type == typeof(NoteView));
    }
}
