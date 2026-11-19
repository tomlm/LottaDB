using static Lotta.Rule;

namespace Lotta.Tests;

/// <summary>
/// Tests for <c>When&lt;T&gt;().Modify(handler)</c>: a change made to an object as part of its save,
/// before its row is written.
/// </summary>
public abstract class ModifyRuleTestBase : LottaTestBase
{
    protected ModifyRuleTestBase(Action<LottaCatalog> config) : base(config) { }

    [Fact]
    public async Task Modify_ChangesTheObjectBeforeItIsWritten_HoweverItIsSaved()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Note>().Modify((note, _, _) =>
            {
                note.Domain = note.Content.ToUpperInvariant();
                return Task.CompletedTask;
            }), cancellationToken: ct);

        // SaveAsync: the caller's object carries the change, and so do the row and the index.
        var note = new Note { NoteId = "n1", Content = "one" };
        var result = await db.SaveAsync(note, ct);
        Assert.Empty(result.Errors);
        Assert.Equal("ONE", note.Domain);
        Assert.Equal("ONE", (await db.GetAsync<Note>("n1", ct))!.Domain);
        Assert.Equal("n1", db.Search<Note>().Where(n => n.Domain == "ONE").ToList().Single().NoteId);
        // One write: the object as saved is the stored version.
        note.Content = "uno";
        Assert.Empty((await db.SaveAsync(note, ct)).Errors);
        Assert.Equal("UNO", (await db.GetAsync<Note>("n1", ct))!.Domain);

        // SaveManyAsync
        await db.SaveManyAsync([new Note { NoteId = "n2", Content = "two" }, new Note { NoteId = "n3", Content = "three" }], ct);
        Assert.Equal("TWO", (await db.GetAsync<Note>("n2", ct))!.Domain);
        Assert.Equal("THREE", (await db.GetAsync<Note>("n3", ct))!.Domain);

        // ChangeAsync
        await db.ChangeAsync<Note>("n2", n => { n.Content = "deux"; return n; }, ct);
        Assert.Equal("DEUX", (await db.GetAsync<Note>("n2", ct))!.Domain);
    }

    [Fact]
    public async Task Modify_WithChangedInTheFilter_RunsOnlyWhenThoseValuesChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = 0;
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Note>(note => Changed(note.Content)).Modify((note, _, _) =>
            {
                runs++;
                note.NotQueryable = $"length {note.Content.Length}";
                return Task.CompletedTask;
            }), cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1", Content = "one" }, ct);
        Assert.Equal(1, runs);

        var loaded = (await db.GetAsync<Note>("n1", ct))!;
        loaded.AuthorId = "alice";
        await db.SaveAsync(loaded, ct);
        Assert.Equal(1, runs);

        loaded.Content = "longer";
        await db.SaveAsync(loaded, ct);
        Assert.Equal(2, runs);
        Assert.Equal("length 6", (await db.GetAsync<Note>("n1", ct))!.NotQueryable);
    }

    [Fact]
    public async Task Modify_RunsBeforeDoAndMaterialize_WhichSeeWhatItChanged()
    {
        var ct = TestContext.Current.CancellationToken;
        var seen = new List<string>();
        var db = await CreateDbAsync(opts =>
        {
            // Added last, run first.
            opts.Rules.When<Note>().Do((note, _, _) => { seen.Add(note.Domain); return Task.CompletedTask; });
            opts.Rules.When<Note>().Materialize(note => new NoteView { Id = $"nv-{note.NoteId}", NoteId = note.NoteId, Content = note.Domain });
            opts.Rules.When<Note>()
                .Modify((note, _, _) => { note.Domain = "first"; return Task.CompletedTask; })
                .Modify((note, _, _) => { note.Domain += "+second"; return Task.CompletedTask; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Note { NoteId = "n1" }, ct);

        Assert.Equal(["first+second"], seen);
        Assert.Equal("first+second", (await db.GetAsync<NoteView>("nv-n1", ct))!.Content);
    }

    [Fact]
    public async Task Modify_ThatThrows_IsInTheSavesErrors_AndTheObjectIsStillSaved()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
            opts.Rules.When<Note>().Modify((note, _, _) => throw new InvalidOperationException("cannot stamp")), cancellationToken: ct);

        var result = await db.SaveAsync(new Note { NoteId = "n1", Content = "kept" }, ct);
        Assert.Equal("cannot stamp", Assert.Single(result.Errors).Message);
        Assert.Equal("kept", (await db.GetAsync<Note>("n1", ct))!.Content);

        var many = await db.SaveManyAsync([new Note { NoteId = "n2" }, new Note { NoteId = "n3" }], ct);
        Assert.Equal(2, many.Errors.Count);
        Assert.NotNull(await db.GetAsync<Note>("n3", ct));
    }

    [Fact]
    public async Task Modify_ForABaseType_CoversTheTypesDerivedFromIt_AndNeedsNoQueueOrName()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(opts =>
        {
            opts.Store<SpecialNote>();
            opts.Rules.When<Note>().Modify((note, context, _) =>
            {
                note.NotQueryable = $"{context.Rule}|{context.Attempt}";
                return Task.CompletedTask;
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new SpecialNote { NoteId = "s1" }, ct);
        Assert.Equal("|1", (await db.GetAsync<SpecialNote>("s1", ct))!.NotQueryable);
        Assert.Empty((await db.Rules.StatusAsync(ct)).Rules);
    }

    private class SpecialNote : Note { }
}
