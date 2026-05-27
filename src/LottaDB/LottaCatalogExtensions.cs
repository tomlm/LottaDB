using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;

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
        catalog.LuceneDirectoryFactory = (path) =>
        {
            // Default: AzureDirectory persists index to blob storage with FSDirectory cache in temp.
            // Each catalog+database gets its own cache folder to avoid corruption under parallel use.
            var cachePath = Path.Combine(Path.GetTempPath(), "LottaCatalogCache",  path);
            System.IO.Directory.CreateDirectory(cachePath);
            var cacheDirectory = FSDirectory.Open(cachePath);

            var blobServiceClient = catalog.GetBlobServiceClient();
            var azureDirectory = new AzureDirectory(blobServiceClient, path, cacheDirectory); // Use RAMDirectory to avoid file locks from FSDirectory.

            // delete stale cache files that are not in blob storage to avoid corruption from previous runs. This can happen if a previous run was interrupted before it could clear the cache.
            var files = azureDirectory.ListAll().ToHashSet();
            foreach(var staleFile in cacheDirectory.ListAll().Where(f => !files.Contains(f)))
                cacheDirectory.DeleteFile(staleFile); 

            return azureDirectory;
        };
        return catalog;
    }
}
