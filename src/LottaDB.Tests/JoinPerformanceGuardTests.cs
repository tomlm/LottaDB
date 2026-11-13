using System.Diagnostics;
using Lucene.Net.Linq;

namespace Lotta.Tests;

[CollectionDefinition(nameof(PerformanceGuardCollection), DisableParallelization = true)]
public class PerformanceGuardCollection { }

/// <summary>
/// Fails when a join from a not-analyzed foreign key to a <c>[Key]</c> stops running inside
/// Lucene, or stops being much cheaper than the same join done in memory.
/// </summary>
/// <remarks>
/// A unit test cannot hold a time to within a few percent: the same code varies by more than
/// that from run to run, and far more on a shared build machine. So nothing here compares
/// against a stored time. The checks are an exact count (how many objects the search
/// matched), a ratio between two queries measured side by side in this process (the join
/// inside Lucene is normally 50 to 100 times cheaper; the test asks for 5), and a budget on
/// the bytes the paged join allocates, which is the same on every run.
/// For exact timings use <c>JoinBenchmarks</c> in LottaDB.Benchmarks.
/// </remarks>
[Collection(nameof(PerformanceGuardCollection))]
public class JoinPerformanceGuardTests : LottaTestBase
{
    private const int ShopCount = 400;
    private const int GadgetCount = 8_000;
    private const int MinimumRatio = 5;

    /// <summary>Twice what the paged join allocates today (298 KB, the same on every run).</summary>
    private const long PagedJoinByteBudget = 600 * 1024;

    public JoinPerformanceGuardTests() : base(c => c.UseMemory()) { }

    private async Task<LottaDB> SeedAsync(CancellationToken ct)
    {
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct, testName: nameof(JoinPerformanceGuardTests));

        // 2% of the shops are in DE; a tenth of the gadgets point at a shop that does not exist.
        await db.SaveManyAsync(Enumerable.Range(0, ShopCount)
            .Select(i => new Shop { ShopId = "shop" + i, Country = i % 50 == 0 ? "DE" : "US", Name = "Shop " + i }).ToList(), ct);
        await db.SaveManyAsync(Enumerable.Range(0, GadgetCount)
            .Select(i =>
            {
                var shop = "shop" + (i % 10 == 3 ? -1 : i % ShopCount);
                return new Gadget { GadgetId = "gadget" + i, SoldBy = shop, SoldByText = shop, Rank = i };
            }).ToList(), ct);
        return db;
    }

    [Fact]
    public async Task JoinInsideLucene_LoadsOnlyTheRowsThatJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        var joining = GadgetCount / 50;   // gadgets sold by a DE shop

        var hits = -1;
        var page = (from g in db.Search<Gadget>().CaptureStatistics(stats => hits = stats.TotalHits)
                    join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldBy equals s.ShopId
                    orderby g.Rank
                    select g.GadgetId).Take(20).ToList();

        Assert.Equal(20, page.Count);
        Assert.Equal(joining, hits);

        // The same join on the analyzed copy of the key is done in memory: every gadget is matched.
        hits = -1;
        var inMemory = (from g in db.Search<Gadget>().CaptureStatistics(stats => hits = stats.TotalHits)
                        join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldByText equals s.ShopId
                        orderby g.Rank
                        select g.GadgetId).Take(20).ToList();

        Assert.Equal(page, inMemory);
        Assert.Equal(GadgetCount, hits);
    }

    [Fact]
    public async Task JoinInsideLucene_IsMuchCheaperThanTheSameJoinInMemory()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Func<List<string>> inLucene = () =>
            (from g in db.Search<Gadget>()
             join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldBy equals s.ShopId
             orderby g.Rank
             select g.GadgetId).Take(20).ToList();

        Func<List<string>> inMemory = () =>
            (from g in db.Search<Gadget>()
             join s in db.Search<Shop>(x => x.Country == "DE") on g.SoldByText equals s.ShopId
             orderby g.Rank
             select g.GadgetId).Take(20).ToList();

        Assert.Equal(inMemory(), inLucene());

        var (luceneMs, luceneBytes) = Measure(inLucene);
        var (memoryMs, memoryBytes) = Measure(inMemory);

        var report = $"inside Lucene: {luceneMs:F2} ms, {luceneBytes / 1024} KB; in memory: {memoryMs:F2} ms, {memoryBytes / 1024} KB";
        TestContext.Current.TestOutputHelper?.WriteLine(report);

        // Allocation barely varies between runs, so it is the steadier of the two checks.
        Assert.True(luceneBytes * MinimumRatio < memoryBytes, $"Expected the join inside Lucene to allocate under 1/{MinimumRatio} of the in-memory join. {report}");
        Assert.True(luceneBytes < PagedJoinByteBudget, $"Expected the join inside Lucene to allocate under {PagedJoinByteBudget / 1024} KB. {report}");
        Assert.True(luceneMs * MinimumRatio < memoryMs, $"Expected the join inside Lucene to take under 1/{MinimumRatio} of the in-memory join. {report}");
    }

    /// <summary>Best of several runs, after a warm-up: the least noisy figure a test can take.</summary>
    private static (double Milliseconds, long Bytes) Measure(Func<List<string>> query)
    {
        for (var i = 0; i < 3; i++) query();

        var bestMs = double.MaxValue;
        var bestBytes = long.MaxValue;
        for (var i = 0; i < 7; i++)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            var watch = Stopwatch.StartNew();
            query();
            watch.Stop();
            bestMs = Math.Min(bestMs, watch.Elapsed.TotalMilliseconds);
            bestBytes = Math.Min(bestBytes, GC.GetAllocatedBytesForCurrentThread() - before);
        }
        return (bestMs, bestBytes);
    }
}
