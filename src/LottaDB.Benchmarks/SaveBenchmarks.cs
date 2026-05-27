using BenchmarkDotNet.Attributes;


namespace Lotta.Benchmarks;
[MemoryDiagnoser]
public class SaveBenchmarks
{
    private const int DocumentCount = 1000;
    private static readonly string[] _animals = ["cat", "dog", "fish", "bird", "hamster"];
    private static readonly string[] _adjectives = ["quick", "lazy", "sleepy", "friendly", "grumpy"];
    private LottaCatalog _catalog = null !;
    private LottaDB _db = null !;
    private List<BenchmarkDocument> _docs = null !;
    private string _tempPath = "";
    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _tempPath = Path.Combine(Path.GetTempPath(), $"LottaSaveBench_{Guid.NewGuid():N}");
        _catalog = new LottaCatalog("SaveBenchmarks", c => c.UseSQLite(_tempPath));
        _db = await _catalog.GetDatabaseAsync("bench", config => config.Store<BenchmarkDocument>());
        _docs = Enumerable.Range(0, DocumentCount).Select(i =>
        {
            var animal = _animals[i % _animals.Length];
            var adjective = _adjectives[i % _adjectives.Length];
            return new BenchmarkDocument
            {
                Id = $"doc-{i:D6}",
                Title = $"Document {i}",
                Content = $"The {adjective} {animal} jumps over the lazy fence. Index={i}.",
                Counter = i,
            };
        }).ToList();
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await _db.DeleteDatabaseAsync();
        _catalog.Dispose();
        try
        {
            Directory.Delete(_tempPath, true);
        }
        catch
        {
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _db.ResetDatabaseAsync().GetAwaiter().GetResult();
        // Clear ETags so SaveAsync does an unconditional upsert instead of a conditional Replace
        foreach (var doc in _docs)
            doc.SetETag(null!);
    }
    [Benchmark(Baseline = true)]
    public async Task SaveAsync_Individual()
    {
        foreach (var doc in _docs)
            await _db.SaveAsync(doc!);
    }

    [Benchmark]
    public async Task SaveManyAsync_Batch()
    {
        await _db.SaveManyAsync(_docs);
    }
}