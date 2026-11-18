namespace Lotta.Tests;

/// <summary>Inline rules: <c>Rules.When&lt;T&gt;().Do</c>, <c>Rules.WhenDeleted&lt;T&gt;().Do</c>, and rules added to an open database.</summary>
public abstract class ObserveTestBase : LottaTestBase
{
    protected ObserveTestBase(Action<LottaCatalog> config) : base(config) { }

    [Fact]
    public async Task On_ReceivesSavedNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        Actor? received = null;
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<Actor>().Do(async (actor, db, _) => { received = actor; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "saved", DisplayName = "Saved" }, ct);
        Assert.NotNull(received);
        Assert.Equal("Saved", received.DisplayName);
    }

    [Fact]
    public async Task On_ReceivesDeletedNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        var received = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<Actor>().Do(async (actor, db, _) => { received.Add($"saved:{actor.DisplayName}"); });
            opts.Rules.WhenDeleted<Actor>().Do(async (actor, db, _) => { received.Add($"deleted:{actor.DisplayName}"); });
        }, cancellationToken: ct);

        var actor = new Actor { Username = "deleted", DisplayName = "Gone" };
        await db.SaveAsync(actor, ct);
        await db.DeleteAsync(actor, ct);
        Assert.Equal(["saved:Gone", "deleted:Gone"], received);
    }

    [Fact]
    public async Task On_RuntimeRegistration_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);
        Actor? received = null;

        using var handle = db.Rules.When<Actor>().Do(async (actor, db, _) => { received = actor; });
        await db.SaveAsync(new Actor { Username = "runtime", DisplayName = "Runtime" }, ct);

        Assert.NotNull(received);
    }

    [Fact]
    public async Task On_Dispose_StopsNotifications()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);
        int count = 0;

        var handle = db.Rules.When<Actor>().Do(async (actor, db, _) => { Interlocked.Increment(ref count); });
        await db.SaveAsync(new Actor { Username = "before" }, ct);
        Assert.Equal(1, count);

        handle.Dispose();
        await db.SaveAsync(new Actor { Username = "after" }, ct);
        Assert.Equal(1, count); // Should not have incremented
    }

    [Fact]
    public async Task On_MultipleHandlers_AllFired()
    {
        var ct = TestContext.Current.CancellationToken;
        int count = 0;
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<Actor>().Do(async (a, d, _) => { Interlocked.Increment(ref count); });
            opts.Rules.When<Actor>().Do(async (a, d, _) => { Interlocked.Increment(ref count); });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "multi" }, ct);
        Assert.Equal(2, count);
    }

    // === Filters ===

    [Fact]
    public async Task When_AnInlineRulesFilter_ChoosesTheSavesItRunsFor()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<Note>(n => n.Content.Length > 5).Do(async (note, _, _) => { ran.Add(note.NoteId); });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "short", Content = "hi" }, ct);
        await db.SaveAsync(new Note { NoteId = "long", Content = "a longer note" }, ct);

        Assert.Equal(["long"], ran);
    }

    [Fact]
    public async Task WhenDeleted_AFilter_ChoosesTheDeletesItRunsFor_LookingAtTheDeletedObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.WhenDeleted<Note>(n => n.Content == "keep track").Do(async (note, _, _) => { ran.Add(note.NoteId); });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "a", Content = "keep track" }, ct);
        await db.SaveAsync(new Note { NoteId = "b", Content = "other" }, ct);
        Assert.Empty(ran);   // saves are not its business

        await db.DeleteAsync<Note>("a", ct);
        await db.DeleteAsync<Note>("b", ct);
        Assert.Equal(["a"], ran);
    }

    [Fact]
    public async Task When_AnInlineFilterThatThrows_IsReportedInTheSave_WhichStillHappens()
    {
        var ct = TestContext.Current.CancellationToken;
        var ran = false;
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<Note>(n => Throws(n)).Do(async (_, _, _) => { ran = true; });
        }, cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "n1", Content = "x" }, ct);

        Assert.Contains("bad filter", Assert.Single(result.Errors).Message);
        Assert.False(ran);
        Assert.NotNull(await db.GetAsync<Note>("n1", ct));
    }

    [Fact]
    public async Task ALiveRule_WithAFilter_ForDeletes_RunsUntilItsHandleIsDisposed()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);
        var ran = new List<string>();
        foreach (var name in new[] { "a1", "a2", "b1" })
            await db.SaveAsync(new Actor { Username = name }, ct);

        var handle = db.Rules.WhenDeleted<Actor>(a => a.Username.StartsWith('a')).Do(async (actor, _, _) => { ran.Add(actor.Username); });
        await db.DeleteAsync<Actor>("a1", ct);
        await db.DeleteAsync<Actor>("b1", ct);
        handle.Dispose();
        await db.DeleteAsync<Actor>("a2", ct);

        Assert.Equal(["a1"], ran);
    }

    /// <summary>A filter that fails; a method, since an expression cannot hold a throw.</summary>
    private static bool Throws(Note note) => throw new InvalidOperationException("bad filter");

    // === Rules with no action ===

    [Fact]
    public async Task ARuleWithNoAction_FailsTheOpen_NamingItsType()
    {
        var ct = TestContext.Current.CancellationToken;

        var e = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            CreateDbAsync(opts => opts.Rules.When<Note>(n => n.Content.Length > 5), cancellationToken: ct));
        Assert.Contains("Note", e.Message);

        e = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            CreateDbAsync(opts => opts.Rules.WhenDeleted<Actor>(), testName: "ARuleWithNoActionDeleted", cancellationToken: ct));
        Assert.Contains("Actor", e.Message);
    }

    // === Index before rules ===

    [Fact]
    public async Task AnInlineRule_SearchesAndFindsTheNewVersionOfItsObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.When<Note>().Do(async (note, db, _) =>
            {
                var found = db.Search<Note>().Where(n => n.NoteId == note.NoteId).ToList();
                seen.Add(found.SingleOrDefault()?.Content ?? "(not found)");
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "first" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", Content = "second" }, ct);
        await db.ChangeAsync<Note>("n1", n => { n.Content = "third"; return n; }, ct);
        await db.SaveManyAsync(new[] { new Note { NoteId = "n1", Content = "fourth" } }, ct);

        Assert.Equal(["first", "second", "third", "fourth"], seen);
    }

    [Fact]
    public async Task ADeleteRule_SearchesAndNoLongerFindsItsObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var found = new List<int>();
        var db = await CreateDbAsync(opts =>
        {
            opts.Rules.WhenDeleted<Note>().Do(async (note, db, _) =>
                found.Add(db.Search<Note>().Where(n => n.NoteId == note.NoteId).Count()));
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "x" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", Content = "y" }, ct);
        await db.DeleteAsync<Note>("n1", ct);
        await db.DeleteAsync((await db.GetAsync<Note>("n2", ct))!, ct);

        Assert.Equal([0, 0], found);
    }

    [Fact]
    public async Task ALiveRule_SearchesAndFindsTheNewVersionOfItsObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);
        var seen = new List<string>();
        await db.SaveAsync(new Note { NoteId = "n1", Content = "first" }, ct);

        using (db.Rules.When<Note>().Do(async (note, db, _) =>
            seen.Add(db.Search<Note>().Where(n => n.NoteId == note.NoteId).ToList().SingleOrDefault()?.Content ?? "(not found)")))
        {
            await db.SaveAsync(new Note { NoteId = "n1", Content = "second" }, ct);
        }

        Assert.Equal(["second"], seen);
    }

    [Fact]
    public async Task ARuleForABaseType_SearchesAndFindsTheDerivedObject()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            // Saving an Employee runs the index handlers of Employee and of its base types, then these.
            opts.Rules.When<Employee>().Do(async (e, db, _) =>
                seen.Add($"employee:{db.Search<Employee>().Where(x => x.Id == e.Id).ToList().SingleOrDefault()?.Name}"));
            opts.Rules.When<BaseEntity>().Do(async (e, db, _) =>
                seen.Add($"base:{db.Search<Employee>().Where(x => x.Id == e.Id).ToList().SingleOrDefault()?.Name}"));
        }, cancellationToken: ct);

        await db.SaveAsync(new Employee { Id = "e1", Name = "Eve" }, ct);
        await db.SaveAsync(new Employee { Id = "e1", Name = "Evelyn" }, ct);

        Assert.Equal(["employee:Eve", "base:Eve", "employee:Evelyn", "base:Evelyn"], seen);
    }
}
