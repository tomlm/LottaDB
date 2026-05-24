using Azure.Data.Tables;
using Azure.Storage.Blobs;

namespace Lotta;

/// <summary>
/// Extension methods for configuring <see cref="LottaCatalog"/> storage providers.
/// </summary>
public static class LottaCatalogExtensions
{
    /// <summary>
    /// Configure the catalog to use Azure Table Storage and Blob Storage via a connection string.
    /// Lucene index is persisted to blob storage via AzureDirectory with a local FSDirectory cache.
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    /// <param name="connectionString">Azure Storage connection string. Use <c>"UseDevelopmentStorage=true"</c> for Azurite.</param>
    public static LottaCatalog UseAzure(this LottaCatalog catalog, string connectionString)
    {
        catalog.TableServiceClientFactory = () => new TableServiceClient(connectionString);
        catalog.BlobServiceClientFactory = () => new BlobServiceClient(connectionString);
        // LuceneDirectoryFactory = null → AzureDirectory with FSDirectory cache (default)
        catalog.LuceneDirectoryFactory = null;
        return catalog;
    }
}
