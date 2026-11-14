using BenchmarkDotNet.Attributes;

namespace Lotta.Benchmarks;

public class JoinShop
{
    [Key]
    public string ShopId { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string Country { get; set; } = "";

    [Queryable]
    public string Name { get; set; } = "";
}

public class JoinGadget
{
    [Key]
    public string GadgetId { get; set; } = "";

    /// <summary>Foreign key to <see cref="JoinShop.ShopId"/>, one exact term: joins inside Lucene.</summary>
    [Queryable(QueryableMode.NotAnalyzed)]
    public string SoldBy { get; set; } = "";

    /// <summary>The same key, analyzed: joins in memory.</summary>
    [Queryable]
    public string SoldByText { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string Category { get; set; } = "";

    [Queryable]
    public int Rank { get; set; }
}

/// <summary>
/// LINQ joins between two <c>Search&lt;T&gt;()</c> queries: 50,000 gadgets, 5,000 shops, 2% of
/// the shops in DE. Run with <c>dotnet run -c Release -- --filter "*Join*"</c>.
/// Compare two versions by running this on each and comparing the tables.
/// </summary>
[MemoryDiagnoser]
public class JoinBenchmarks
{
    private const int ShopCount = 5_000;
    private const int GadgetCount = 50_000;

    private LottaCatalog _catalog = null!;
    private LottaDB _db = null!;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _catalog = new LottaCatalog(nameof(JoinBenchmarks), c => c.UseMemory());
        _db = await _catalog.GetDatabaseAsync("joins", config =>
        {
            config.Store<JoinShop>();
            config.Store<JoinGadget>();
        });
        await _db.ResetDatabaseAsync();

        await _db.SaveManyAsync(Enumerable.Range(0, ShopCount)
            .Select(i => new JoinShop { ShopId = "shop" + i, Country = i % 50 == 0 ? "DE" : "US", Name = "Shop " + i }).ToList());

        // Every tenth gadget points at a shop that does not exist.
        await _db.SaveManyAsync(Enumerable.Range(0, GadgetCount)
            .Select(i =>
            {
                var shop = "shop" + (i % 10 == 3 ? -1 : i % ShopCount);
                return new JoinGadget { GadgetId = "gadget" + i, SoldBy = shop, SoldByText = shop, Category = i % 5 == 0 ? "phone" : "other", Rank = i };
            }).ToList());
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await _db.DeleteDatabaseAsync();
        _catalog.Dispose();
    }

    [Benchmark(Description = "Page of 20: gadgets of DE shops")]
    public List<string> Page_FilteredShops() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>().Where(x => x.Country == "DE") on g.SoldBy equals s.ShopId
         orderby g.Rank
         select g.GadgetId).Take(20).ToList();

    [Benchmark(Description = "Page of 20: gadgets of any shop")]
    public List<string> Page_AllShops() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>() on g.SoldBy equals s.ShopId
         orderby g.Rank
         select g.GadgetId).Take(20).ToList();

    [Benchmark(Description = "Count: gadgets of DE shops")]
    public int Count_FilteredShops() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>().Where(x => x.Country == "DE") on g.SoldBy equals s.ShopId
         select g).Count();

    [Benchmark(Description = "All rows: gadgets of DE shops, gadget side")]
    public List<string> All_GadgetSide() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>().Where(x => x.Country == "DE") on g.SoldBy equals s.ShopId
         select g.GadgetId).ToList();

    [Benchmark(Description = "All rows: gadgets of DE shops, both sides")]
    public int All_BothSides() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>().Where(x => x.Country == "DE") on g.SoldBy equals s.ShopId
         select new { g.GadgetId, s.Name }).ToList().Count;

    [Benchmark(Description = "All rows: shops selling a phone, shop side")]
    public List<string> All_ShopSide() =>
        (from s in _db.Search<JoinShop>()
         join g in _db.Search<JoinGadget>().Where(x => x.Category == "phone" && x.Rank < 2000) on s.ShopId equals g.SoldBy
         select s.Name).ToList();

    [Benchmark(Description = "All rows: every gadget with its shop")]
    public int All_Unfiltered() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>() on g.SoldBy equals s.ShopId
         select new { g.GadgetId, s.Name }).ToList().Count;

    [Benchmark(Description = "Page of 20: analyzed foreign key (joined in memory)")]
    public List<string> Page_AnalyzedKey() =>
        (from g in _db.Search<JoinGadget>()
         join s in _db.Search<JoinShop>().Where(x => x.Country == "DE") on g.SoldByText equals s.ShopId
         orderby g.Rank
         select g.GadgetId).Take(20).ToList();

    [Benchmark(Baseline = true, Description = "Page of 20: ToList() both sides, LINQ to Objects")]
    public List<string> Page_ByHand() =>
        (from g in _db.Search<JoinGadget>().ToList()
         join s in _db.Search<JoinShop>().Where(x => x.Country == "DE").ToList() on g.SoldBy equals s.ShopId
         orderby g.Rank
         select g.GadgetId).Take(20).ToList();
}
