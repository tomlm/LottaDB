using Iciclecreek.Lucene.Net.Temporal;
using Lucene.Net.Linq;

namespace Lotta.Tests;

/// <summary>A note with a sent time and the TIMEX values its text refers to, declared with attributes.</summary>
public class DatedNote
{
    public string Id { get; set; } = "";

    [Temporal]
    public DateTime Sent { get; set; }

    [Timex(Anchor = nameof(Sent))]
    public List<string> Mentions { get; set; } = [];
}

/// <summary>The same, declared with the fluent API.</summary>
public class FluentDatedNote
{
    public string Id { get; set; } = "";

    public DateTimeOffset Sent { get; set; }

    public List<string> Mentions { get; set; } = [];
}

public class TimexWithoutAnchor
{
    public string Id { get; set; } = "";

    [Timex(Anchor = "Nowhere")]
    public List<string> Mentions { get; set; } = [];
}

/// <summary>
/// Temporal properties: a date ([Temporal], .Temporal()) and TIMEX values anchored on it
/// ([Timex], .Timex()), indexed with Iciclecreek.Lucene.Net.Temporal and queried with the
/// strategy <see cref="LottaDB.TemporalStrategy{T}(string)"/> returns.
/// </summary>
public abstract class TemporalTestBase : LottaTestBase
{
    protected TemporalTestBase(Action<LottaCatalog> config) : base(config) { }

    private static TemporalContext Now => new(TimeZoneInfo.Local, new DateTimeOffset(2026, 11, 22, 12, 0, 0, TimeSpan.Zero));

    private static DateTime Noon(int year, int month, int day) => new(year, month, day, 20, 0, 0, DateTimeKind.Utc);

    private static string[] Ids<T>(LottaDB db, Lucene.Net.Search.Query query) where T : class, new() =>
        [.. db.Search<T>().Where(query).ToList().Select(n => (string)typeof(T).GetProperty("Id")!.GetValue(n)!).Order()];

    [Fact]
    public async Task ATimexReference_MatchesTheDaysItMeans_PartialAndRecurring()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(c => c.Store<DatedNote>(), cancellationToken: ct);
        await db.SaveManyAsync([
            new DatedNote { Id = "thursdays", Sent = Noon(2026, 11, 2), Mentions = ["XXXX-WXX-4"] },      // "every Thursday"
            new DatedNote { Id = "trip", Sent = Noon(2026, 5, 13), Mentions = ["(2026-07-02,2026-07-10,P8D)"] },
            new DatedNote { Id = "none", Sent = Noon(2026, 11, 2) },
        ], ct);

        var mentions = db.TemporalStrategy<DatedNote>(nameof(DatedNote.Mentions));

        Assert.Equal(["thursdays"], Ids<DatedNote>(db, mentions.MakeQuery("2026-11-19", Now)));   // a Thursday
        Assert.Empty(Ids<DatedNote>(db, mentions.MakeQuery("2026-11-20", Now)));                // a Friday
        Assert.Equal(["trip"], Ids<DatedNote>(db, mentions.MakeQuery("2026-07-05", Now)));
        // About Thursdays: every Thursday, and the trip, whose days include Thursdays (July 2 and 9).
        Assert.Equal(["thursdays", "trip"], Ids<DatedNote>(db, mentions.MakeQuery("XXXX-WXX-4", Now)));
    }

    [Fact]
    public async Task ATemporalDate_MatchesPartialDates_AndRanges()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(c => c.Store<DatedNote>(), cancellationToken: ct);
        await db.SaveManyAsync([
            new DatedNote { Id = "christmas-2025", Sent = Noon(2025, 12, 25) },
            new DatedNote { Id = "christmas-2024", Sent = Noon(2024, 12, 25) },
            new DatedNote { Id = "march", Sent = Noon(2026, 3, 4) },
        ], ct);

        var sent = db.TemporalStrategy<DatedNote>(nameof(DatedNote.Sent));

        Assert.Equal(["christmas-2024", "christmas-2025"], Ids<DatedNote>(db, sent.MakeQuery("XXXX-12-25", Now)));
        Assert.Equal(["march"], Ids<DatedNote>(db, sent.MakeRangeQuery("2026-01", "2026-12", true, true, Now)));
    }

    [Fact]
    public async Task TheFluentDeclaration_IndexesAsTheAttributesDo()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(c => c.Store<FluentDatedNote>(s =>
        {
            s.AddField(n => n.Sent).Temporal();
            s.AddField(n => n.Mentions).Timex(nameof(FluentDatedNote.Sent));
        }), cancellationToken: ct);
        await db.SaveAsync(new FluentDatedNote { Id = "thursdays", Sent = new DateTimeOffset(Noon(2026, 11, 2)), Mentions = ["XXXX-WXX-4"] }, ct);

        Assert.Equal(["thursdays"], Ids<FluentDatedNote>(db, db.TemporalStrategy<FluentDatedNote>(nameof(FluentDatedNote.Mentions)).MakeQuery("2026-11-19", Now)));
        Assert.Equal(["thursdays"], Ids<FluentDatedNote>(db, db.TemporalStrategy<FluentDatedNote>(nameof(FluentDatedNote.Sent)).MakeQuery("2026-11-02", Now)));
    }

    [Fact]
    public async Task RebuildingTheIndex_MakesTheTemporalFieldsAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(c => c.Store<DatedNote>(), cancellationToken: ct);
        await db.SaveAsync(new DatedNote { Id = "thursdays", Sent = Noon(2026, 11, 2), Mentions = ["XXXX-WXX-4"] }, ct);

        await db.RebuildSearchIndex(ct);

        Assert.Equal(["thursdays"], Ids<DatedNote>(db, db.TemporalStrategy<DatedNote>(nameof(DatedNote.Mentions)).MakeQuery("2026-11-19", Now)));
    }

    [Fact]
    public async Task ATimexPropertyWithoutAnAnchor_IsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        var error = await Assert.ThrowsAnyAsync<Exception>(() => CreateDbAsync(c => c.Store<TimexWithoutAnchor>(), cancellationToken: ct));
        Assert.Contains("anchor", error.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task APropertyThatIsNotTemporal_HasNoStrategy()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(c => c.Store<DatedNote>(), cancellationToken: ct);
        Assert.Throws<InvalidOperationException>(() => db.TemporalStrategy<DatedNote>(nameof(DatedNote.Id)));
    }
}
