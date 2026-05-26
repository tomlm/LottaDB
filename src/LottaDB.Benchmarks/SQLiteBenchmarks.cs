using BenchmarkDotNet.Attributes;



namespace Lotta.Benchmarks;

/// <summary>LottaDB throughput benchmark against the SQLite storage provider.</summary>
[MemoryDiagnoser]
public class SQLiteBenchmarks : LottaDBBenchmarkBase
{
    private string _tempPath = "";

    protected override void Configure(LottaCatalog catalog)
    {
        if (string.IsNullOrEmpty(_tempPath))
            _tempPath = Path.Combine(Path.GetTempPath(), $"LottaBench_{Guid.NewGuid():N}");
        catalog.UseSQLite(_tempPath);
    }
}
