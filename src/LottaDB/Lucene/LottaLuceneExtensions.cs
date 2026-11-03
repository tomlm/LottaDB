using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// Adds <see cref="UseLucene(LottaCatalog)"/> to <see cref="LottaCatalog"/>: pure-Lucene storage.
/// </summary>
public static class LottaLuceneExtensions
{
    /// <summary>
    /// Store everything in in-memory Lucene directories: objects, search index and blobs.
    /// All data lives in RAM and is lost when the process exits.
    /// See <see cref="UseLucene(LottaCatalog, Func{string, LuceneDirectory})"/>.
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    public static LottaCatalog UseLucene(this LottaCatalog catalog)
        => catalog.UseLucene(LuceneDirectories.Memory());

    /// <summary>
    /// Store everything in Lucene directories from <paramref name="directoryFactory"/>, e.g.
    /// <c>catalog.UseLucene(LuceneDirectories.FileSystem(path))</c>.
    /// Each object is written once — into its database's index — and point reads, queries and
    /// optimistic concurrency are served from there. Blobs are files in the <c>{catalog}/blobs</c>
    /// directory.
    /// <para>
    /// Index writes become durable when committed (see <see cref="ILottaConfiguration.AutoCommitDelay"/>),
    /// so a crash can lose writes made since the last commit. Reads in the writing process always
    /// see its own writes; other processes see committed state. Blob writes are durable when the
    /// upload completes.
    /// </para>
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    /// <param name="directoryFactory">
    /// Returns the Lucene directory for a path such as <c>{catalog}/{database}/Search</c>; see
    /// <see cref="LuceneDirectories"/>.
    /// </param>
    public static LottaCatalog UseLucene(this LottaCatalog catalog, Func<string, LuceneDirectory> directoryFactory)
        => catalog.UseStorage("Lucene",
            () => new LuceneTableServiceClient(catalog),
            () => new LuceneBlobServiceClient(catalog),
            directoryFactory);
}
