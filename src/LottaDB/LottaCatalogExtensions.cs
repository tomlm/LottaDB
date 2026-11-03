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
    /// Lucene index is persisted to blob storage via AzureDirectory with a local FSDirectory cache
    /// (see <see cref="LuceneDirectories.Azure(string)"/>).
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    /// <param name="connectionString">Azure Storage connection string. Use <c>"UseDevelopmentStorage=true"</c> for Azurite.</param>
    public static LottaCatalog UseAzure(this LottaCatalog catalog, string connectionString)
        => catalog.UseStorage("Azure",
            () => new TableServiceClient(connectionString),
            () => new BlobServiceClient(connectionString),
            LuceneDirectories.Azure(catalog.GetBlobServiceClient));
}
