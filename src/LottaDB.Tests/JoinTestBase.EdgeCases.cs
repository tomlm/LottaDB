using Lucene.Net.Linq;

namespace Lotta.Tests;

// Joins away from the common path: derived types, keys that are not plain strings, missing
// keys, key shapes Lucene cannot join on, two databases, facets, awkward key text and size.
public abstract partial class JoinTestBase
{
    [Fact]
    public async Task Join_OnABaseType_IncludesDerivedTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        await db.SaveAsync(new Vehicle { Id = "phone", Maker = "Volvo" }, ct);
        await db.SaveAsync(new Truck { Id = "laptop", Maker = "Scania", Axles = 3 }, ct);

        // Vehicle as the filtering side: phones and laptops join, the tablet does not.
        var hits = -1;
        var all = (from g in db.Search<Gadget>().CaptureStatistics(stats => hits = stats.TotalHits)
                   join v in db.Search<Vehicle>() on g.Category equals v.Id
                   select g.GadgetId).ToList();
        Assert.Equal(["g1", "g2", "g3", "g4"], all.Order());
        Assert.Equal(4, hits);   // joined inside Lucene

        // Truck only: the plain Vehicle must not join.
        var trucks = (from g in db.Search<Gadget>() join t in db.Search<Truck>() on g.Category equals t.Id select g.GadgetId).ToList();
        Assert.Equal(["g3", "g4"], trucks.Order());

        // Vehicle as the returned side, and with both sides in the result.
        var makers = (from v in db.Search<Vehicle>() join g in db.Search<Gadget>() on v.Id equals g.Category select v.Maker).ToList();
        Assert.Equal(["Scania", "Scania", "Volvo", "Volvo"], makers.Order());

        var rows = (from g in db.Search<Gadget>() join v in db.Search<Vehicle>() on g.Category equals v.Id orderby g.Rank select new { g.GadgetId, v.Maker }).ToList();
        Assert.Equal(["g1:Volvo", "g2:Volvo", "g3:Scania", "g4:Scania"], rows.Select(r => $"{r.GadgetId}:{r.Maker}"));

        // A derived type on the returned side keeps its own properties.
        var axles = (from t in db.Search<Truck>() join g in db.Search<Gadget>() on t.Id equals g.Category select t.Axles).ToList();
        Assert.Equal([3, 3], axles);
    }

    [Fact]
    public async Task Join_OnANumericKey_StillGivesTheRightRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        Depot[] depots = [new Depot { DepotId = 5, Region = "north" }, new Depot { DepotId = 4, Region = "south" }, new Depot { DepotId = 2, Region = "east" }];
        await db.SaveManyAsync(depots, ct);

        // An int [Key] cannot be joined inside Lucene, so this is done in memory.
        var rows = (from g in db.Search<Gadget>() join d in db.Search<Depot>() on g.Stars equals d.DepotId orderby g.Rank select new { g.GadgetId, d.Region }).ToList();

        var expected = (from g in Gadgets join d in depots on g.Stars equals d.DepotId orderby g.Rank select new { g.GadgetId, d.Region }).ToList();
        Assert.Equal(expected, rows);
        Assert.Equal(["g1:north", "g2:south", "g3:north"], rows.Select(r => $"{r.GadgetId}:{r.Region}"));

        Assert.Equal(3, (from g in db.Search<Gadget>() join d in db.Search<Depot>() on g.Stars equals d.DepotId select g).Count());
        Assert.Equal(["north", "north", "south"], (from d in db.Search<Depot>() join g in db.Search<Gadget>() on d.DepotId equals g.Stars select d.Region).ToList().Order());
    }

    [Fact]
    public async Task Join_NullOrEmptyForeignKey_DoesNotJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        await db.SaveAsync(new Gadget { GadgetId = "g6", SoldBy = "", SoldByText = "", Category = "phone", Rank = 6 }, ct);
        await db.SaveAsync(new Gadget { GadgetId = "g7", SoldBy = null!, SoldByText = null!, Category = "phone", Rank = 7 }, ct);

        string[] joined = ["g1", "g2", "g3", "g4"];

        Assert.Equal(joined, (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g.GadgetId).ToList().Order());
        Assert.Equal(joined, (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select new { g.GadgetId, s.Name }).ToList().Select(r => r.GadgetId).Order());
        Assert.Equal(joined, (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldByText equals s.ShopId select g.GadgetId).ToList().Order());
        Assert.Equal(4, (from s in db.Search<Shop>() join g in db.Search<Gadget>() on s.ShopId equals g.SoldBy select s).Count());

        // The gadgets without a shop are still there.
        Assert.Equal(7, db.Search<Gadget>().Count());
    }

    [Fact]
    public async Task Join_CompositeAndComputedKeys_StillGiveTheRightRows()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var composite = (from g in db.Search<Gadget>()
                         join s in db.Search<Shop>() on new { Id = g.SoldBy, Country = "DE" } equals new { Id = s.ShopId, s.Country }
                         select g.GadgetId).ToList();
        Assert.Equal(["g1", "g2"], composite.Order());

        var computed = (from g in db.Search<Gadget>()
                        join s in db.Search<Shop>() on g.SoldBy.ToUpperInvariant() equals s.ShopId.ToUpperInvariant()
                        select g.GadgetId).ToList();
        Assert.Equal(["g1", "g2", "g3", "g4"], computed.Order());
    }

    [Fact]
    public async Task GroupJoin_IsNotSupported()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        // "join ... into" (group join, and the left join built on it) is not translated.
        // Load both sides with ToList() and join them with LINQ to Objects instead.
        Assert.Throws<InvalidOperationException>(() =>
            (from s in db.Search<Shop>() join g in db.Search<Gadget>() on s.ShopId equals g.SoldBy into sold select new { s.ShopId, Count = sold.Count() }).ToList());

        var byHand = (from s in db.Search<Shop>().ToList() join g in db.Search<Gadget>().ToList() on s.ShopId equals g.SoldBy into sold orderby s.ShopId select $"{s.ShopId}:{sold.Count()}").ToList();
        Assert.Equal(["s1:2", "s2:2", "s3:0"], byHand);
    }

    [Fact]
    public async Task Join_AcrossTwoDatabases_UsesEachDatabasesOwnObjects()
    {
        var ct = TestContext.Current.CancellationToken;
        var gadgetDb = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct, testName: "JoinTwoDbsGadgets");
        var shopDb = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct, testName: "JoinTwoDbsShops");
        await gadgetDb.SaveManyAsync(Gadgets, ct);
        await gadgetDb.SaveAsync(new Shop { ShopId = "s9", Country = "FR", Name = "Only here" }, ct);
        await shopDb.SaveManyAsync(Shops, ct);

        // Shops come from shopDb: s1 and s2 exist there, s9 does not.
        var across = (from g in gadgetDb.Search<Gadget>() join s in shopDb.Search<Shop>() on g.SoldBy equals s.ShopId orderby g.Rank select new { g.GadgetId, s.Name }).ToList();
        Assert.Equal(["g1:Alpha", "g2:Alpha", "g3:Beta", "g4:Beta"], across.Select(r => $"{r.GadgetId}:{r.Name}"));

        var acrossSemi = (from g in gadgetDb.Search<Gadget>() join s in shopDb.Search<Shop>() on g.SoldBy equals s.ShopId select g.GadgetId).ToList();
        Assert.Equal(["g1", "g2", "g3", "g4"], acrossSemi.Order());
        Assert.Equal(4, (from g in gadgetDb.Search<Gadget>() join s in shopDb.Search<Shop>() on g.SoldBy equals s.ShopId select g).Count());

        // Within gadgetDb only s9 exists.
        var within = (from g in gadgetDb.Search<Gadget>() join s in gadgetDb.Search<Shop>() on g.SoldBy equals s.ShopId select g.GadgetId).ToList();
        Assert.Equal(["g5"], within);
    }

    [Fact]
    public async Task Join_KeysWithCaseAndPunctuation_MatchExactly()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct);
        Shop[] shops =
        [
            new Shop { ShopId = "Mixed-Case", Name = "upper" },
            new Shop { ShopId = "mixed-case", Name = "lower" },
            new Shop { ShopId = "with space", Name = "space" },
            new Shop { ShopId = "a:b+c*(d) AND e", Name = "query syntax" },
            new Shop { ShopId = "ünï-cödé", Name = "unicode" },
        ];
        Gadget[] gadgets =
        [
            new Gadget { GadgetId = "k1", SoldBy = "Mixed-Case" },
            new Gadget { GadgetId = "k2", SoldBy = "mixed-case" },
            new Gadget { GadgetId = "k3", SoldBy = "MIXED-CASE" },   // no such shop
            new Gadget { GadgetId = "k4", SoldBy = "with space" },
            new Gadget { GadgetId = "k5", SoldBy = "with" },         // no such shop
            new Gadget { GadgetId = "k6", SoldBy = "a:b+c*(d) AND e" },
            new Gadget { GadgetId = "k7", SoldBy = "ünï-cödé" },
        ];
        await db.SaveManyAsync(shops, ct);
        await db.SaveManyAsync(gadgets, ct);

        var expected = (from g in gadgets join s in shops on g.SoldBy equals s.ShopId orderby g.GadgetId select $"{g.GadgetId}:{s.Name}").ToList();
        Assert.Equal(["k1:upper", "k2:lower", "k4:space", "k6:query syntax", "k7:unicode"], expected);

        var both = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select new { g.GadgetId, s.Name }).ToList();
        Assert.Equal(expected, both.Select(r => $"{r.GadgetId}:{r.Name}").Order());

        var gadgetSide = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g.GadgetId).ToList();
        Assert.Equal(["k1", "k2", "k4", "k6", "k7"], gadgetSide.Order());

        var shopSide = (from s in db.Search<Shop>() join g in db.Search<Gadget>() on s.ShopId equals g.SoldBy select s.Name).ToList();
        Assert.Equal(["lower", "query syntax", "space", "unicode", "upper"], shopSide.Order());
    }

    [Fact]
    public async Task Join_ThousandsOfKeys_MatchesLinqToObjects()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct);

        // More keys than a Lucene BooleanQuery accepts clauses (1024).
        var shops = Enumerable.Range(0, 1500).Select(i => new Shop { ShopId = "shop" + i, Country = i % 3 == 0 ? "DE" : "US", Name = "Shop " + i }).ToArray();
        var gadgets = Enumerable.Range(0, 4000).Select(i => new Gadget { GadgetId = "gadget" + i, SoldBy = "shop" + (i % 7 == 0 ? -1 : i % 2000), Rank = i }).ToArray();
        await db.SaveManyAsync(shops, ct);
        await db.SaveManyAsync(gadgets, ct);

        var expectedIds = (from g in gadgets join s in shops on g.SoldBy equals s.ShopId orderby g.Rank select g.GadgetId).ToList();
        Assert.True(expectedIds.Count > 1024);

        var ids = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId orderby g.Rank select g.GadgetId).ToList();
        Assert.Equal(expectedIds, ids);

        Assert.Equal(expectedIds.Count, (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId select g).Count());

        var expectedGerman = (from g in gadgets join s in shops.Where(x => x.Country == "DE") on g.SoldBy equals s.ShopId orderby g.Rank select $"{g.GadgetId}:{s.Name}").ToList();
        var german = (from g in db.Search<Gadget>() join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldBy equals s.ShopId orderby g.Rank select new { g.GadgetId, s.Name }).ToList();
        Assert.Equal(expectedGerman, german.Select(r => $"{r.GadgetId}:{r.Name}"));

        var expectedPage = expectedIds.Skip(1000).Take(25).ToList();
        var page = (from g in db.Search<Gadget>() join s in db.Search<Shop>() on g.SoldBy equals s.ShopId orderby g.Rank select g.GadgetId).Skip(1000).Take(25).ToList();
        Assert.Equal(expectedPage, page);

        var expectedShops = (from s in shops join g in gadgets.Where(x => x.Rank < 3000) on s.ShopId equals g.SoldBy select s.ShopId).Order().ToList();
        var shopIds = (from s in db.Search<Shop>() join g in db.Search<Gadget>(x => x.Rank < 3000) on s.ShopId equals g.SoldBy select s.ShopId).ToList();
        Assert.Equal(expectedShops, shopIds.Order());
    }

    [Fact]
    public async Task Join_WithFacets_CountsTheGadgetsThatJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        // The join runs inside Lucene, so it narrows the gadget search and the counts with it.
        // (A join done in memory does not narrow the search: its counts cover every gadget.)
        var ids = (from g in db.Search<Gadget>().Facets(out var facets, f => f.Count(x => x.Category).Count(x => x.Tags))
                   join s in db.Search<Shop>(x => x.Country == "US") on g.SoldBy equals s.ShopId
                   select g.GadgetId).ToList();

        Assert.Equal(["g3", "g4"], ids.Order());
        Assert.Equal(["laptop=2"], facets.For(x => x.Category).Select(v => $"{v.Label}={v.Count}"));
        Assert.Equal(["big=1", "red=1"], facets.For(x => x.Tags).Select(v => $"{v.Label}={v.Count}"));

        // The same when the result uses both sides.
        var rows = (from g in db.Search<Gadget>().Facets(out var both, f => f.Count(x => x.Category))
                    join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldBy equals s.ShopId
                    select new { g.GadgetId, s.Name }).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(["phone=2"], both.For(x => x.Category).Select(v => $"{v.Label}={v.Count}"));
    }
}
