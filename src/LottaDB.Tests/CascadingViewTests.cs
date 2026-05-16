using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class CascadingViewTests
{
    private async Task<LottaDB> CreateDbAsync([CallerMemberName] string? testName = null)
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
        }, testName: testName);
    }

    [Fact]
    public async Task CascadingView_NoteCreates_NoteViewAndFeedEntry()
    {
        using var db = await CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "c1", AuthorId = "alice", Content = "Hello world", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c1", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c1", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CascadingView_ActorChange_UpdatesBothViews()
    {
        using var db = await CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "Before" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "c2", AuthorId = "updater", Content = "Test", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        await db.SaveAsync(new Actor { Username = "updater", DisplayName = "After" }, TestContext.Current.CancellationToken);

        Assert.Equal("After", (await db.GetAsync<NoteView>("nv-c2", TestContext.Current.CancellationToken))!.AuthorDisplay);
        Assert.Contains("After", (await db.GetAsync<FeedEntry>("fe-nv-c2", TestContext.Current.CancellationToken))!.Title);
    }

    [Fact]
    public async Task CascadingView_NoteDeleted_DeletesBothViews()
    {
        using var db = await CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "deleter", DisplayName = "D" }, TestContext.Current.CancellationToken);
        var note = new Note { NoteId = "c3", AuthorId = "deleter", Content = "Gone", Published = DateTimeOffset.UtcNow };
        await db.SaveAsync(note, TestContext.Current.CancellationToken);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c3", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c3", TestContext.Current.CancellationToken));

        await db.DeleteAsync(note, TestContext.Current.CancellationToken);

        Assert.Null(await db.GetAsync<NoteView>("nv-c3", TestContext.Current.CancellationToken));
        Assert.Null(await db.GetAsync<FeedEntry>("fe-nv-c3", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CascadingView_ActorDeleted_DeletesAllViews()
    {
        using var db = await CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "gone-actor", DisplayName = "Gone" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "c4", AuthorId = "gone-actor", Content = "A", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.NotNull(await db.GetAsync<NoteView>("nv-c4", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<FeedEntry>("fe-nv-c4", TestContext.Current.CancellationToken));

        await db.DeleteAsync<Actor>("gone-actor", TestContext.Current.CancellationToken);

        Assert.Null(await db.GetAsync<NoteView>("nv-c4", TestContext.Current.CancellationToken));
        Assert.Null(await db.GetAsync<FeedEntry>("fe-nv-c4", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CascadingView_ResultContainsAllChanges()
    {
        using var db = await CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "result-actor", DisplayName = "R" }, TestContext.Current.CancellationToken);
        var result = await db.SaveAsync(new Note { NoteId = "c5", AuthorId = "result-actor", Content = "Chain", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        Assert.Contains(result.Changes, c => c.Type == typeof(Note));
        Assert.Contains(result.Changes, c => c.Type == typeof(NoteView));
        Assert.Contains(result.Changes, c => c.Type == typeof(FeedEntry));
    }
}
