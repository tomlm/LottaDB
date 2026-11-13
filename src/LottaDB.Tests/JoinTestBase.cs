using System.Runtime.CompilerServices;
using Lucene.Net.Linq;

namespace Lotta.Tests;

/// <summary>
/// LINQ joins between two <c>db.Search&lt;T&gt;()</c> queries. A join from a <c>[Key]</c> to a
/// not-analyzed string property runs inside Lucene; any other join is done in memory. Either
/// way the rows must be the ones LINQ to Objects gives for the same data.
/// </summary>
public abstract class JoinTestBase : LottaTestBase
{
    protected JoinTestBase(Action<LottaCatalog> config) : base(config) { }

    private static readonly Gadget[] Gadgets = FacetJoinData.Gadgets;
    private static readonly Shop[] Shops = FacetJoinData.Shops;

    private async Task<LottaDB> SeedAsync(CancellationToken ct, [CallerMemberName] string? testName = null)
    {
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct, testName: testName);
        await db.SaveManyAsync(Gadgets, ct);
        await db.SaveManyAsync(Shops, ct);
        return db;
    }

    [Fact]
    public async Task Join_SelectingTheForeignKeySide()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var ids = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g.GadgetId).ToList();

        var expected = (from g in Gadgets join s in Shops on g.SoldBy equals s.ShopId select g.GadgetId).ToList();
        Assert.Equal(expected.Order(), ids.Order());
        Assert.Equal(4, ids.Count);   // g5's shop does not exist
    }

    [Fact]
    public async Task Join_SelectingTheKeySide_RepeatsItPerMatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var names = (from s in db.Search<Shop>() join g in db.Search<Gadget>() on s.ShopId equals g.SoldBy select s.Name).ToList();

        var expected = (from s in Shops join g in Gadgets on s.ShopId equals g.SoldBy select s.Name).ToList();
        Assert.Equal(expected.Order(), names.Order());
        Assert.Equal(["Alpha", "Alpha", "Beta", "Beta"], names.Order());
    }

    [Fact]
    public async Task Join_SelectingBothSides()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var rows = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select new { g.GadgetId, s.Country }).ToList();

        var expected = (from g in Gadgets join s in Shops on g.SoldBy equals s.ShopId select new { g.GadgetId, s.Country }).ToList();
        Assert.Equal(expected.OrderBy(r => r.GadgetId), rows.OrderBy(r => r.GadgetId));
    }

    [Fact]
    public async Task Join_CountAndAny()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.Equal(4, (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g).Count());
        Assert.Equal(4, (from s in db.Search<Shop>() join g in db.Search<Gadget>() on s.ShopId equals g.SoldBy select g).Count());

        Assert.True((from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g).Any());
        Assert.False((from g in db.Search<Gadget>(x => x.GadgetId == "g5") join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g).Any());
    }

    [Fact]
    public async Task Join_OrderByAndPaging_ApplyToTheJoinedRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var ids = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId orderby g.Rank descending select g.GadgetId)
            .Skip(1).Take(2).ToList();

        var expected = (from g in Gadgets join s in Shops on g.SoldBy equals s.ShopId orderby g.Rank descending select g.GadgetId)
            .Skip(1).Take(2).ToList();
        Assert.Equal(expected, ids);
        Assert.Equal(["g3", "g2"], ids);
    }

    [Fact]
    public async Task Join_FilterOnEitherSide()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var inGermany = (from g in db.Search<Gadget>() join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldBy equals s.ShopId select g.GadgetId).ToList();
        Assert.Equal(["g1", "g2"], inGermany.Order());

        var laptops = (from g in db.Search<Gadget>(x => x.Category == "laptop") join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select s.Name).ToList();
        Assert.Equal(["Beta", "Beta"], laptops);

        var after = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId where s.Country == "US" && g.Stars == 5 select g.GadgetId).ToList();
        Assert.Equal(["g3"], after);
    }

    [Fact]
    public async Task Join_OnAnAnalyzedProperty_StillGivesTheRightRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        // SoldByText is analyzed, so this join is done in memory.
        var ids = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldByText equals s.ShopId orderby g.Rank select g.GadgetId).ToList();
        Assert.Equal(["g1", "g2", "g3", "g4"], ids);

        Assert.Equal(4, (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldByText equals s.ShopId select g).Count());
    }

    [Fact]
    public async Task Join_KeyToNotAnalyzedProperty_RunsInsideLucene()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        // TotalHits is what the gadget search matched. When the join runs inside Lucene the
        // shops become a filter on that search, so it matches only the gadgets that join.
        // Joined in memory, the search would match all five gadgets.
        var hits = -1;
        var ids = (from g in db.Search<Gadget>().CaptureStatistics(stats => hits = stats.TotalHits)
                   join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldBy equals s.ShopId
                   select g.GadgetId).ToList();

        Assert.Equal(["g1", "g2"], ids.Order());
        Assert.Equal(2, hits);

        // The analyzed property cannot be joined inside Lucene: every gadget is loaded.
        hits = -1;
        var inMemory = (from g in db.Search<Gadget>().CaptureStatistics(stats => hits = stats.TotalHits)
                        join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldByText equals s.ShopId
                        select g.GadgetId).ToList();

        Assert.Equal(["g1", "g2"], inMemory.Order());
        Assert.Equal(5, hits);
    }

    [Fact]
    public async Task Join_ThreeWay()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        await db.SaveAsync(new Vehicle { Id = "phone", Maker = "Volvo" }, ct);   // keys are unique across types, so not "s1"

        var rows = (from g in db.Search<Gadget>()
                    join s in db.Search<Shop>() on g.SoldBy equals s.ShopId
                    join v in db.Search<Vehicle>() on g.Category equals v.Id
                    select new { g.GadgetId, s.Name, v.Maker }).ToList();

        Assert.Equal(["g1", "g2"], rows.Select(r => r.GadgetId).Order());
        Assert.All(rows, r => Assert.Equal(("Alpha", "Volvo"), (r.Name, r.Maker)));
    }

    [Fact]
    public async Task KeyProperty_IsStillQueriedAndReplacedByKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.Equal("Alpha", db.Search<Shop>(s => s.ShopId == "s1").Single().Name);
        Assert.Equal(["s1", "s2", "s3"], db.Search<Shop>().OrderBy(s => s.ShopId).Select(s => s.ShopId).ToList());

        await db.SaveAsync(new Shop { ShopId = "s1", Country = "FR", Name = "Alpha" }, ct);
        Assert.Equal(3, db.Search<Shop>().Count());
        Assert.Equal("FR", db.Search<Shop>(s => s.ShopId == "s1").Single().Country);
    }
}
