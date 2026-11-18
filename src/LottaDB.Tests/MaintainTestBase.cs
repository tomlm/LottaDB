using System.Text.Json;

namespace Lotta.Tests;

/// <summary>
/// Tests for <c>When&lt;T&gt;().Maintain(projection)</c>: objects kept in step with a source object,
/// inside the save or from the queue, and run again when an object the projection reads changes.
/// Queued work is run by hand with <see cref="LottaDB.DrainAsync"/>, so every step is in order.
/// </summary>
public abstract class MaintainTestBase : LottaTestBase
{
    protected MaintainTestBase(Action<LottaCatalog> config) : base(config) { }

    private static NoteView? ProjectView(Note note) =>
        new() { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, AuthorUsername = note.AuthorId, Content = note.Content };

    /// <summary>One entry per tag of the note.</summary>
    private static IEnumerable<FeedEntry> ProjectTags(Note note) =>
        note.Tags.Select(tag => new FeedEntry { Id = $"{note.NoteId}#{tag}", NoteViewId = note.NoteId, Title = tag });

    private static async Task<List<string>> EntriesOf(LottaDB db, string noteId, CancellationToken ct) =>
        [.. (await db.GetManyAsync<FeedEntry>(e => e.NoteViewId == noteId).ToListAsync(ct)).Select(e => e.Title).Order()];

    // === Inside the save ===

    [Fact]
    public async Task Maintain_SavesWhatTheObjectProduces_InsideTheSave()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain(ProjectView);
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" }, ct);

        Assert.Empty(result.Errors);
        Assert.Contains(result.Changes, c => c.Key == "nv-n1" && c.Kind == ChangeKind.Saved);
        var view = await db.GetAsync<NoteView>("nv-n1", ct);
        Assert.Equal("hello", view!.Content);
        Assert.Single(db.Search<NoteView>().Where(v => v.NoteId == "n1").ToList());
        // Nothing was left for the queue.
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task Maintain_OnALaterSave_WritesTheViewOnlyWhenItDiffers()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain(ProjectView);
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "one" }, ct);
        var first = (await db.GetAsync<NoteView>("nv-n1", ct))!.GetETag();

        // The view takes nothing from Domain: it is the same, so it is not written again.
        await db.SaveAsync(new Note { NoteId = "n1", Content = "one", Domain = "elsewhere" }, ct);
        Assert.Equal(first, (await db.GetAsync<NoteView>("nv-n1", ct))!.GetETag());

        await db.SaveAsync(new Note { NoteId = "n1", Content = "two", Domain = "elsewhere" }, ct);
        var view = await db.GetAsync<NoteView>("nv-n1", ct);
        Assert.Equal("two", view!.Content);
        Assert.NotEqual(first, view.GetETag());
    }

    [Fact]
    public async Task Maintain_WithSeveralObjects_SavesTheNewAndDeletesTheNoLongerProduced()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain("tags", ProjectTags);
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Tags = ["a", "b"] }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Tags = ["a"] }, ct);
        Assert.Equal(["a", "b"], await EntriesOf(db, "n1", ct));
        var kept = (await db.GetAsync<FeedEntry>("n1#b", ct))!.GetETag();

        var result = await db.SaveAsync(new Note { NoteId = "n1", Tags = ["b", "c"] }, ct);

        Assert.Empty(result.Errors);
        Assert.Equal(["b", "c"], await EntriesOf(db, "n1", ct));
        Assert.Contains(result.Changes, c => c.Key == "n1#a" && c.Kind == ChangeKind.Deleted);
        Assert.Equal(kept, (await db.GetAsync<FeedEntry>("n1#b", ct))!.GetETag());
        // What another object produced is not touched.
        Assert.Equal(["a"], await EntriesOf(db, "n2", ct));
    }

    [Fact]
    public async Task Maintain_WhenTheProjectionReturnsNothing_DeletesWhatWasProduced()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain(note => note.Content.Length == 0 ? null : ProjectView(note));
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "text" }, ct);
        Assert.NotNull(await db.GetAsync<NoteView>("nv-n1", ct));

        await db.SaveAsync(new Note { NoteId = "n1", Content = "" }, ct);
        Assert.Null(await db.GetAsync<NoteView>("nv-n1", ct));
    }

    [Fact]
    public async Task Maintain_WhenTheFilterBecomesFalse_DeletesWhatWasProduced()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => note.Domain == "public").Maintain(ProjectView);
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Domain = "public" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Domain = "private" }, ct);
        Assert.NotNull(await db.GetAsync<NoteView>("nv-n1", ct));
        Assert.Null(await db.GetAsync<NoteView>("nv-n2", ct));

        await db.SaveAsync(new Note { NoteId = "n1", Domain = "private" }, ct);
        Assert.Null(await db.GetAsync<NoteView>("nv-n1", ct));
    }

    [Fact]
    public async Task Maintain_WhenTheObjectIsDeleted_DeletesWhatItProduced_HoweverItIsDeleted()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain("tags", ProjectTags);
        }, cancellationToken: ct);
        foreach (var id in new[] { "n1", "n2", "n3", "n4" })
            await db.SaveAsync(new Note { NoteId = id, Tags = ["a", "b"] }, ct);

        var result = await db.DeleteAsync<Note>("n1", ct);
        Assert.Contains(result.Changes, c => c.Key == "n1#a" && c.Kind == ChangeKind.Deleted);
        await db.DeleteAsync("n2", ct);
        await db.DeleteManyAsync(["n3"], ct);

        Assert.Empty(await EntriesOf(db, "n1", ct));
        Assert.Empty(await EntriesOf(db, "n2", ct));
        Assert.Empty(await EntriesOf(db, "n3", ct));
        Assert.Equal(["a", "b"], await EntriesOf(db, "n4", ct));
    }

    [Fact]
    public async Task Maintain_ForABaseType_CoversTheTypesDerivedFromIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<BaseEntity>().Maintain(entity => new FeedEntry { Id = $"f-{entity.Id}", Title = entity.Name });
        }, cancellationToken: ct);

        await db.SaveAsync(new Employee { Id = "e1", Name = "Eve" }, ct);
        Assert.Equal("Eve", (await db.GetAsync<FeedEntry>("f-e1", ct))!.Title);

        await db.ChangeAsync<Employee>("e1", employee => { employee.Name = "Eva"; }, ct);
        Assert.Equal("Eva", (await db.GetAsync<FeedEntry>("f-e1", ct))!.Title);

        await db.DeleteAsync<Employee>("e1", ct);
        Assert.Null(await db.GetAsync<FeedEntry>("f-e1", ct));
    }

    // === Ownership ===

    [Fact]
    public async Task ProducedObjects_AreMarkedWithTheirSource_BesideTheObjectNotInIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain("tags", ProjectTags);
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Tags = ["a", "b"] }, ct);

        var stored = await db.GetAsync<FeedEntry>("n1#a", ct);
        Assert.Equal("Note/tags n1", stored!.GetSource());
        Assert.DoesNotContain("source", stored.GetJson()!, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("Note/tags n1", db.Search<FeedEntry>().Where(e => e.NoteViewId == "n1").ToList().First().GetSource());
        // The note itself, and what is saved by hand, carries no mark.
        Assert.Null((await db.GetAsync<Note>("n1", ct))!.GetSource());

        // The mark survives the object being loaded, changed and saved, and the index being rebuilt.
        stored.Domain = "edited";
        await db.SaveAsync(stored, ct);
        await db.RebuildSearchIndex(ct);
        await db.SaveAsync(new Note { NoteId = "n1", Tags = ["b"] }, ct);
        Assert.Equal(["b"], await EntriesOf(db, "n1", ct));
    }

    [Fact]
    public async Task TwoObjectsProducingOneKey_IsAnError_AndTheFirstKeepsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain(note => new NoteView { Id = "shared", NoteId = note.NoteId });
        }, cancellationToken: ct);

        Assert.Empty((await db.SaveAsync(new Note { NoteId = "n1" }, ct)).Errors);
        var result = await db.SaveAsync(new Note { NoteId = "n2" }, ct);

        var error = Assert.Single(result.Errors);
        Assert.Contains("belongs to 'Note/note-view n1'", error.Message);
        Assert.Equal("n1", (await db.GetAsync<NoteView>("shared", ct))!.NoteId);
    }

    [Fact]
    public async Task AnObjectSavedByHand_IsTakenOverByTheRuleThatProducesItsKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain(ProjectView);
        }, cancellationToken: ct);
        await db.SaveAsync(new NoteView { Id = "nv-n1", Content = "by hand" }, ct);

        Assert.Empty((await db.SaveAsync(new Note { NoteId = "n1", Content = "from the note" }, ct)).Errors);
        Assert.Equal("from the note", (await db.GetAsync<NoteView>("nv-n1", ct))!.Content);

        await db.DeleteAsync<Note>("n1", ct);
        Assert.Null(await db.GetAsync<NoteView>("nv-n1", ct));
    }

    [Fact]
    public async Task AProjection_CannotProduceObjectsOfTheTypeItIsFor()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain("copy", note => new Note { NoteId = $"copy-{note.NoteId}" });
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "n1" }, ct);

        Assert.Contains("cannot produce objects of the type it is for", Assert.Single(result.Errors).Message);
        Assert.Null(await db.GetAsync<Note>("copy-n1", ct));
    }

    // === From the queue ===

    [Fact]
    public async Task Queued_RunsTheProjectionLater_AndDeletesAtOnceWhenTheObjectIs()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>(note => note.Domain == "public").Maintain("tags", ProjectTags).Queued();
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Domain = "public", Tags = ["a", "b"] }, ct);
        Assert.Empty(await EntriesOf(db, "n1", ct));   // not inside the save

        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["a", "b"], await EntriesOf(db, "n1", ct));

        await db.SaveAsync(new Note { NoteId = "n1", Domain = "public", Tags = ["b", "c"] }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(["b", "c"], await EntriesOf(db, "n1", ct));

        // The rule is no longer for the note: a run still comes, to delete what the note produced.
        await db.SaveAsync(new Note { NoteId = "n1", Domain = "private", Tags = ["b", "c"] }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Empty(await EntriesOf(db, "n1", ct));

        await db.SaveAsync(new Note { NoteId = "n2", Domain = "public", Tags = ["x"] }, ct);
        await db.DrainAsync(cancellationToken: ct);
        await db.DeleteAsync<Note>("n2", ct);
        Assert.Empty(await EntriesOf(db, "n2", ct));   // inside the delete, with no run
    }

    [Fact]
    public async Task Queued_AProjectionThatFails_IsRetried_AndStatusNamesTheRule()
    {
        var ct = TestContext.Current.CancellationToken;
        var fail = true;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.RuleRetryDelay = TimeSpan.Zero;
            opts.Rules.When<Note>().Maintain(note => fail ? throw new InvalidOperationException("not yet") : ProjectView(note)).Queued();
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "text" }, ct);
        Assert.Equal(1, await db.DrainAsync(max: 1, cancellationToken: ct));
        var status = (await db.Rules.StatusAsync(ct)).Rules.Single(r => r.Rule == "Note/note-view");
        Assert.Equal("not yet", status.LastError);
        Assert.Null(await db.GetAsync<NoteView>("nv-n1", ct));

        fail = false;
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal("text", (await db.GetAsync<NoteView>("nv-n1", ct))!.Content);
    }

    // === RefreshWhen ===

    private static async Task<NoteView?> ProjectWithAuthor(Note note, RuleContext context, CancellationToken ct)
    {
        var author = await context.Db.GetAsync<Actor>(note.AuthorId, ct);
        return author == null
            ? null
            : new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, AuthorUsername = author.Username, AuthorDisplay = author.DisplayName, Content = note.Content };
    }

    [Fact]
    public async Task RefreshWhen_AChangeToTheOtherObject_RunsTheProjectionAgainForTheObjectsThatReadIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>()
                .Maintain(ProjectWithAuthor)
                .RefreshWhen<Actor>(actor => note => note.AuthorId == actor.Username);
        }, cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "one" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "two" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        Assert.Equal("Alice", (await db.GetAsync<NoteView>("nv-n1", ct))!.AuthorDisplay);
        var untouched = (await db.GetAsync<NoteView>("nv-n2", ct))!.GetETag();

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Liddell" }, ct);

        // The refresh runs from the queue, for the one note that reads alice.
        Assert.Equal("Alice", (await db.GetAsync<NoteView>("nv-n1", ct))!.AuthorDisplay);
        Assert.Equal("index", db.LastRefreshPath);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal("Alice Liddell", (await db.GetAsync<NoteView>("nv-n1", ct))!.AuthorDisplay);
        Assert.Equal(untouched, (await db.GetAsync<NoteView>("nv-n2", ct))!.GetETag());

        // A delete of the other object refreshes too: the projection now returns nothing.
        await db.DeleteAsync<Actor>("alice", ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Null(await db.GetAsync<NoteView>("nv-n1", ct));
        Assert.NotNull(await db.GetAsync<NoteView>("nv-n2", ct));
    }

    [Fact]
    public async Task RefreshWhen_WithAConditionTheIndexCannotAnswer_ReadsEveryObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>()
                .Maintain(ProjectWithAuthor)
                // Comparing two fields of the note is nothing the index can do.
                .RefreshWhen<Actor>(actor => note => note.AuthorId == actor.Username && note.Domain != note.AuthorId);
        }, cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob" }, ct);
        await db.DrainAsync(cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Liddell" }, ct);

        Assert.Equal("scan", db.LastRefreshPath);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal("Alice Liddell", (await db.GetAsync<NoteView>("nv-n1", ct))!.AuthorDisplay);
    }

    // === Backfill ===

    [Fact]
    public async Task Backfill_RunsTheProjectionAgainForEveryStoredObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var shout = false;
        var db = await CreateDbAsync(opts =>
        {
            opts.QueueMode = QueueMode.Manual;
            opts.Rules.When<Note>().Maintain(note =>
                new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = shout ? note.Content.ToUpperInvariant() : note.Content });
        }, cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", Content = "one" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Content = "two" }, ct);

        // The projection changed: what is stored is as the old one made it, until a backfill.
        shout = true;
        Assert.Equal(2, await db.Rules.BackfillAsync("note-view", ct));
        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));

        Assert.Equal("ONE", (await db.GetAsync<NoteView>("nv-n1", ct))!.Content);
        Assert.Equal("TWO", (await db.GetAsync<NoteView>("nv-n2", ct))!.Content);
    }
}

/// <summary>How a <c>Maintain</c> rule gets its name. Needs no database.</summary>
public class MaintainNamingTests
{
    private static NoteView? ProjectNoteAsync(Note note) => new() { Id = note.NoteId };

    [Fact]
    public void AMaintainRule_IsNamedAfterItsMethod_OrTheTypeALambdaProduces()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Note>().Maintain(ProjectNoteAsync);
        config.Rules.When<Note>().Maintain(note => new NoteView { Id = note.NoteId });
        config.Rules.When<Note>().Maintain(note => note.Tags.Select(tag => new FeedEntry { Id = tag }));
        config.Rules.When<Note>().Maintain(note => note.Tags.Select(tag => new ModerationView { NoteId = tag }).ToList());
        config.Rules.When<Note>().Maintain("by-hand", note => new CycleA { Id = note.NoteId });

        Assert.Equal(["Note/project-note", "Note/note-view", "Note/feed-entry", "Note/moderation-view", "Note/by-hand"],
            config.QueuedRules.Select(r => r.Id));
        Assert.All(config.QueuedRules, rule => Assert.True(rule.Inline));
    }

    [Fact]
    public void TwoLambdasProducingOneType_MustBeNamed()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Note>().Maintain(note => new NoteView { Id = note.NoteId });

        var error = Assert.Throws<ArgumentException>(() =>
            config.Rules.When<Note>().Maintain(note => new NoteView { Id = $"other-{note.NoteId}" }));
        Assert.Contains("Note/note-view", error.Message);
    }

    [Fact]
    public void Queued_MakesTheRuleRunFromTheQueue()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Note>().Maintain(note => new NoteView { Id = note.NoteId }).Queued();

        Assert.False(config.QueuedRules.Single().Inline);
    }
}
