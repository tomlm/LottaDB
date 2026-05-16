using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class CreateViewTests
{
    private async Task<LottaDB> CreateDbAsync(CancellationToken cancellationToken = default, [CallerMemberName] string? testName = null)
    {
        return await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db, _) =>
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
                    Domain = note.Domain,
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    AuthorUsername = actor.Username,
                    AuthorDisplay = actor.DisplayName,
                    AvatarUrl = actor.AvatarUrl,
                    Content = note.Content,
                    Published = note.Published,
                    Tags = note.Tags.ToArray(),
                });
            });

            opts.On<Actor>(async (actor, kind, db, _) =>
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
                        Domain = note.Domain,
                        Id = view.Id,
                        NoteId = view.NoteId,
                        AuthorUsername = actor.Username,
                        AuthorDisplay = actor.DisplayName,
                        AvatarUrl = actor.AvatarUrl,
                        Content = note.Content,
                        Published = note.Published,
                        Tags = note.Tags.ToArray(),
                    });
                }
            });
        }, testName: testName, cancellationToken: cancellationToken);
    }

    [Fact]
    public async Task NoteAndActor_ProducesNoteView()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow }, ct);

        var view = await db.GetAsync<NoteView>("nv-n1", ct);
        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
    }

    [Fact]
    public async Task ActorChange_UpdatesNoteView()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "Before" }, ct);
        await db.SaveAsync(new Note { NoteId = "n-update", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow }, ct);

        Assert.Equal("Before", (await db.GetAsync<NoteView>("nv-n-update", ct))!.AuthorDisplay);
        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "After" }, ct);
        Assert.Equal("After", (await db.GetAsync<NoteView>("nv-n-update", ct))!.AuthorDisplay);
    }

    [Fact]
    public async Task NoteDeleted_DeletesNoteView()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "deleter", DisplayName = "D" }, ct);
        var note = new Note { NoteId = "n-del", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, ct);
        Assert.NotNull(await db.GetAsync<NoteView>("nv-n-del", ct));

        await db.DeleteAsync(note, ct);
        Assert.Null(await db.GetAsync<NoteView>("nv-n-del", ct));
    }

    [Fact]
    public async Task ActorDeleted_DeletesRelatedNoteViews()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "gone", DisplayName = "Gone" }, ct);
        await db.SaveAsync(new Note { NoteId = "orphan1", AuthorId = "gone", Content = "A", Published = DateTimeOffset.UtcNow }, ct);
        await db.SaveAsync(new Note { NoteId = "orphan2", AuthorId = "gone", Content = "B", Published = DateTimeOffset.UtcNow }, ct);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-orphan1", ct));
        Assert.NotNull(await db.GetAsync<NoteView>("nv-orphan2", ct));

        await db.DeleteAsync<Actor>("gone", ct);

        Assert.Null(await db.GetAsync<NoteView>("nv-orphan1", ct));
        Assert.Null(await db.GetAsync<NoteView>("nv-orphan2", ct));
    }

    [Fact]
    public async Task NoMatchingActor_NoViewCreated()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Note { NoteId = "n-orphan", AuthorId = "nobody", Content = "Orphan", Published = DateTimeOffset.UtcNow }, ct);
        Assert.Null(await db.GetAsync<NoteView>("nv-n-orphan", ct));
    }

    [Fact]
    public async Task MultipleNotes_SameActor_AllViews()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "prolific", DisplayName = "Prolific" }, ct);
        await db.SaveAsync(new Note { NoteId = "p1", AuthorId = "prolific", Content = "One", Published = DateTimeOffset.UtcNow }, ct);
        await db.SaveAsync(new Note { NoteId = "p2", AuthorId = "prolific", Content = "Two", Published = DateTimeOffset.UtcNow }, ct);
        await db.SaveAsync(new Note { NoteId = "p3", AuthorId = "prolific", Content = "Three", Published = DateTimeOffset.UtcNow }, ct);

        var views = db.Search<NoteView>().Where(v => v.AuthorUsername == "prolific").ToList();
        Assert.Equal(3, views.Count);
    }

    [Fact]
    public async Task SaveResult_ContainsDerivedChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "result", DisplayName = "R" }, ct);
        var result = await db.SaveAsync(new Note { NoteId = "result-n", AuthorId = "result", Content = "Check", Published = DateTimeOffset.UtcNow }, ct);

        Assert.Contains(result.Changes, c => c.Type == typeof(Note));
        Assert.Contains(result.Changes, c => c.Type == typeof(NoteView));
    }
}
