using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class CascadingViewTests
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
                        Content = note.Content,
                        Published = note.Published,
                    });
                }
            });

            opts.On<NoteView>(async (nv, kind, db, _) =>
            {
                if (kind == TriggerKind.Deleted)
                {
                    await db.DeleteAsync<FeedEntry>($"fe-{nv.Id}");
                    return;
                }
                await db.SaveAsync(new FeedEntry
                {
                    Domain = nv.Domain,
                    Id = $"fe-{nv.Id}",
                    NoteViewId = nv.Id,
                    Title = $"{nv.AuthorDisplay}: {nv.Content}",
                    Published = nv.Published,
                });
            });
        }, testName: testName, cancellationToken: cancellationToken);
    }

    [Fact]
    public async Task CascadingView_NoteCreates_NoteViewAndFeedEntry()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "c1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow }, ct);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c1", ct));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c1", ct));
    }

    [Fact]
    public async Task CascadingView_ActorChange_UpdatesBothViews()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "Before" }, ct);
        await db.SaveAsync(new Note { NoteId = "c2", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow }, ct);

        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "After" }, ct);

        Assert.Equal("After", (await db.GetAsync<NoteView>("nv-c2", ct))!.AuthorDisplay);
        Assert.Contains("After", (await db.GetAsync<FeedEntry>("fe-nv-c2", ct))!.Title);
    }

    [Fact]
    public async Task CascadingView_NoteDeleted_DeletesBothViews()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "deleter", DisplayName = "D" }, ct);
        var note = new Note { NoteId = "c3", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, ct);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c3", ct));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c3", ct));

        await db.DeleteAsync(note, ct);

        Assert.Null(await db.GetAsync<NoteView>("nv-c3", ct));
        Assert.Null(await db.GetAsync<FeedEntry>("fe-nv-c3", ct));
    }

    [Fact]
    public async Task CascadingView_ActorDeleted_DeletesAllViews()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "gone-actor", DisplayName = "Gone" }, ct);
        await db.SaveAsync(new Note { NoteId = "c4", AuthorId = "gone-actor", Content = "A", Published = DateTimeOffset.UtcNow }, ct);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c4", ct));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c4", ct));

        await db.DeleteAsync<Actor>("gone-actor", ct);

        Assert.Null(await db.GetAsync<NoteView>("nv-c4", ct));
        Assert.Null(await db.GetAsync<FeedEntry>("fe-nv-c4", ct));
    }

    [Fact]
    public async Task CascadingView_ResultContainsAllChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(ct);
        await db.SaveAsync(new Actor { Username = "result-actor", DisplayName = "R" }, ct);
        var result = await db.SaveAsync(new Note { NoteId = "c5", AuthorId = "result-actor", Content = "Chain", Published = DateTimeOffset.UtcNow }, ct);

        Assert.Contains(result.Changes, c => c.Type == typeof(Note));
        Assert.Contains(result.Changes, c => c.Type == typeof(NoteView));
        Assert.Contains(result.Changes, c => c.Type == typeof(FeedEntry));
    }
}
