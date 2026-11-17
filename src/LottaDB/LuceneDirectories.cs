using Azure.Storage.Blobs;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// Lucene directory factories for <see cref="LottaCatalog.LuceneDirectoryFactory"/> and
/// <see cref="LottaLuceneExtensions.UseLucene(LottaCatalog, Func{string, LuceneDirectory})"/>.
/// Each returns a factory that maps a path such as <c>{catalog}/{database}/Search</c> to a directory.
/// </summary>
/// <example>
/// <code>catalog.UseLucene(LuceneDirectories.FileSystem(@"C:\data"));</code>
/// </example>
public static class LuceneDirectories
{
    /// <summary>A new in-memory directory for each path. Contents are lost when the directory is disposed.</summary>
    public static Func<string, LuceneDirectory> Memory()
        => _ => new RAMDirectory();

    /// <summary>
    /// A directory on disk at <c>{rootPath}/{path}</c>, created if missing. On Windows it is opened
    /// by its extended path (<c>\\?\C:\…</c>), so <paramref name="rootPath"/> can be as deep as you like.
    /// </summary>
    /// <param name="rootPath">Root folder for all directories.</param>
    public static Func<string, LuceneDirectory> FileSystem(string rootPath)
    {
        var root = ExtendedPath(Path.GetFullPath(rootPath));
        return path =>
        {
            var dir = Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar));
            System.IO.Directory.CreateDirectory(dir);
            return FSDirectory.Open(dir);
        };
    }

    /// <summary>
    /// The extended form of a full path on Windows (<c>\\?\C:\…</c>, or <c>\\?\UNC\server\share\…</c>);
    /// the path as it is anywhere else, or when it is already extended or a device path. Windows
    /// does not normalise an extended path, so anything appended to it must use <c>\</c> and no <c>..</c>.
    /// <para>
    /// .NET reads and writes files past 260 characters without this, but Lucene.Net syncs a file
    /// through its own call to Windows with the path as given. That call fails past 260 characters
    /// unless the program's manifest declares it long-path aware, which an ordinary .NET program
    /// does not; and a queued message's file name alone is over 100 characters.
    /// </para>
    /// </summary>
    internal static string ExtendedPath(string fullPath)
    {
        if (!OperatingSystem.IsWindows() || fullPath.StartsWith(@"\\?\", StringComparison.Ordinal) || fullPath.StartsWith(@"\\.\", StringComparison.Ordinal))
            return fullPath;
        return fullPath.StartsWith(@"\\", StringComparison.Ordinal)
            ? @"\\?\UNC\" + fullPath[2..]
            : @"\\?\" + fullPath;
    }

    /// <summary>
    /// A directory persisted to Azure Blob Storage (<see cref="AzureDirectory"/>), with a local
    /// file cache in the temp folder.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string. Use <c>"UseDevelopmentStorage=true"</c> for Azurite.</param>
    public static Func<string, LuceneDirectory> Azure(string connectionString)
    {
        var blobServiceClient = new Lazy<BlobServiceClient>(() => new BlobServiceClient(connectionString));
        return Azure(() => blobServiceClient.Value);
    }

    /// <summary>
    /// A directory persisted to Azure Blob Storage (<see cref="AzureDirectory"/>), with a local
    /// file cache in the temp folder.
    /// </summary>
    /// <param name="blobServiceClient">The blob account to store the index in.</param>
    public static Func<string, LuceneDirectory> Azure(BlobServiceClient blobServiceClient)
        => Azure(() => blobServiceClient);

    internal static Func<string, LuceneDirectory> Azure(Func<BlobServiceClient> blobServiceClient)
        => path =>
        {
            // The cache path is scoped per catalog+database AND per process. Without the process
            // discriminator, two servers on the same host would share one cache folder — and the
            // stale-file sweep below would delete segments the other process is actively reading.
            // Segment files are immutable, so a per-process cache costs only a re-download after
            // a restart. Process ids are recycled by the OS, which bounds the folder count.
            // An extended path is passed to Windows as it is, so its separators must all be '\'.
            var cachePath = Path.Combine(ExtendedPath(Path.GetFullPath(Path.GetTempPath())), "LottaCatalogCache",
                $"p{Environment.ProcessId}", path.Replace('/', Path.DirectorySeparatorChar));
            System.IO.Directory.CreateDirectory(cachePath);
            var cacheDirectory = FSDirectory.Open(cachePath);

            var azureDirectory = new AzureDirectory(blobServiceClient(), path, cacheDirectory);

            // delete stale cache files that are not in blob storage to avoid corruption from previous runs. This can happen if a previous run was interrupted before it could clear the cache.
            var files = azureDirectory.ListAll().ToHashSet();
            foreach (var staleFile in cacheDirectory.ListAll().Where(f => !files.Contains(f)))
                cacheDirectory.DeleteFile(staleFile);

            return azureDirectory;
        };
}
