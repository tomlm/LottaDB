using BenchmarkDotNet.Attributes;



namespace Lotta.Benchmarks;

/// <summary>
/// LottaDB throughput benchmark against the Azure Table Storage provider.
/// Requires Azurite (UseDevelopmentStorage=true) or a real Azure Storage account.
/// Set the <c>LOTTA_AZURE_CONNECTION_STRING</c> environment variable to override
/// the default development-storage connection string.
/// </summary>
[MemoryDiagnoser]
public class AzureBenchmarks : LottaDBBenchmarkBase
{
    private static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("LOTTA_AZURE_CONNECTION_STRING")
        ?? "UseDevelopmentStorage=true";

    protected override void Configure(LottaCatalog catalog) =>
        catalog.UseAzure(ConnectionString);
}
