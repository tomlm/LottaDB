using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;

namespace Lotta;

/// <summary>
/// Adds <see cref="UseMemory"/> to <see cref="LottaCatalog"/> for in-memory storage.
/// </summary>
public static class LottaMemoryExtensions
{
    /// <summary>
    /// Configure the catalog to use in-memory storage. All data lives in RAM and is lost
    /// when the process exits. Ideal for unit tests -- fast, isolated, no cleanup needed.
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    public static LottaCatalog UseMemory(this LottaCatalog catalog)
    {
        var tableClient = new MemoryTableServiceClient();
        var blobClient = new MemoryBlobServiceClient();
        return catalog.UseStorage("Memory", () => tableClient, () => blobClient, LuceneDirectories.Memory());
    }
}
