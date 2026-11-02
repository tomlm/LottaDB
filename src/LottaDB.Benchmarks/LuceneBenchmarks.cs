using BenchmarkDotNet.Attributes;

namespace Lotta.Benchmarks;

/// <summary>LottaDB throughput benchmark with the search index as the storage (in-memory directories).</summary>
[MemoryDiagnoser]
public class LuceneBenchmarks : LottaDBBenchmarkBase
{
    protected override void Configure(LottaCatalog catalog) =>
        catalog.UseMemory().UseLucene();
}
