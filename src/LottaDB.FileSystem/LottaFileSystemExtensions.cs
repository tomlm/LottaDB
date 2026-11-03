using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;

namespace Lotta;

/// <summary>
/// Adds <see cref="UseFileSystem"/> to <see cref="LottaCatalog"/> for filesystem-backed storage.
/// </summary>
public static class LottaFileSystemExtensions
{
    /// <summary>
    /// Configure the catalog to use filesystem-backed storage. Data is stored as files on disk
    /// under <paramref name="rootPath"/> -- human-inspectable and survives process restarts.
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    /// <param name="rootPath">Root directory for all storage (tables, blobs, and Lucene index).</param>
    public static LottaCatalog UseFileSystem(this LottaCatalog catalog, string rootPath)
        => catalog.UseStorage("FileSystem",
            () => new FileTableServiceClient(rootPath),
            () => new FileBlobServiceClient(rootPath),
            LuceneDirectories.FileSystem(rootPath));
}
