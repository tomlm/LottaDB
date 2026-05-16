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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
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
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, ct);

        var view = await db.GetAsync<NoteView>("nv-n1", ct);
        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
    }

    [Fact]
    public async Task OnHandler_OnSave_DerivedObjectInTableStorage()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n2", Content = "Stored", Published = DateTimeOffset.UtcNow }, ct);

        var views = await db.GetManyAsync<NoteView>(cancellationToken: ct).ToListAsync(ct);
        Assert.Contains(views, v => v.Id == "nv-n2");
    }

    [Fact]
    public async Task OnHandler_OnSave_DerivedObjectInLuceneIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n3", Content = "Indexed", Published = DateTimeOffset.UtcNow }, ct);

        var views = db.Search<NoteView>().ToList();
        Assert.Contains(views, v => v.Id == "nv-n3");
    }

    [Fact]
    public async Task OnHandler_OnDelete_DeletesDerivedObject()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<NoteView>($"nv-{note.NoteId}");
                    return;
                }
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        }, cancellationToken: ct);

        var note = new Note { NoteId = "n4", Content = "Delete me", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, ct);
        Assert.NotNull(await db.GetAsync<NoteView>("nv-n4", ct));

        await db.DeleteAsync(note, ct);
        Assert.Null(await db.GetAsync<NoteView>("nv-n4", ct));
    }

    [Fact]
    public async Task OnHandler_ReceivesTriggerKind_Saved()
    {
        var ct = TestContext.Current.CancellationToken;
        TriggerKind? received = null;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) => { received = kind; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "ts1", Published = DateTimeOffset.UtcNow }, ct);
        Assert.Equal(TriggerKind.Saved, received);
    }

    [Fact]
    public async Task OnHandler_ReceivesTriggerKind_Deleted()
    {
        var ct = TestContext.Current.CancellationToken;
        TriggerKind? received = null;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) => { received = kind; });
        }, cancellationToken: ct);

        var note = new Note { NoteId = "td1", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, ct);
        await db.DeleteAsync(note, ct);
        Assert.Equal(TriggerKind.Deleted, received);
    }

    [Fact]
    public async Task OnHandler_HasAccessToDb()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
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
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "access", DisplayName = "Accessible" }, ct);
        await db.SaveAsync(new Note { NoteId = "access-n", AuthorId = "access", Published = DateTimeOffset.UtcNow }, ct);

        var view = await db.GetAsync<NoteView>("nv-access-n", ct);
        Assert.NotNull(view);
        Assert.Equal("Accessible", view.AuthorDisplay);
    }

    [Fact]
    public async Task OnHandler_Error_DoesNotBlockSourceSave()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                throw new InvalidOperationException("Handler failed");
            });
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "fail1", Published = DateTimeOffset.UtcNow }, ct);

        Assert.NotNull(await db.GetAsync<Note>("fail1", ct));
        Assert.NotEmpty(result.Errors);
    }

    [Fact]
    public async Task OnHandler_Error_CapturedInObjectResult()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                throw new InvalidOperationException("Handler failed intentionally");
            });
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "fail2", Published = DateTimeOffset.UtcNow }, ct);

        Assert.Single(result.Errors);
        Assert.IsType<InvalidOperationException>(result.Errors[0]);
    }

    [Fact]
    public async Task OnHandler_MultipleHandlers_AllRun()
    {
        var ct = TestContext.Current.CancellationToken;
        int count = 0;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) => { Interlocked.Increment(ref count); });
            opts.On<Note>(async (note, kind, db, _) => { Interlocked.Increment(ref count); });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "multi", Published = DateTimeOffset.UtcNow }, ct);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task OnHandler_SaveResult_ContainsDerivedChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Content });
            });
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "result-n", AuthorId = "alice", Content = "Test", Published = DateTimeOffset.UtcNow }, ct);

        Assert.Contains(result.Changes, c => c.Type == typeof(Note));
        Assert.Contains(result.Changes, c => c.Type == typeof(NoteView));
    }
}
