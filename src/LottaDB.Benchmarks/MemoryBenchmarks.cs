using BenchmarkDotNet.Attributes;

namespace Lotta.Benchmarks;

/// <summary>LottaDB throughput benchmark against the in-memory storage provider.</summary>
[MemoryDiagnoser]
public class MemoryBenchmarks : LottaDBBenchmarkBase
{
    protected override void Configure(LottaCatalog catalog) =>
        catalog.UseMemory();
}
