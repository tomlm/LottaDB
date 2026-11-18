using Iciclecreek.Azure.Storage.Lucene.Blobs;
using Iciclecreek.Azure.Storage.Lucene.Queues;
using Iciclecreek.Azure.Storage.Lucene.Tables;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// Adds <see cref="UseLucene(LottaCatalog)"/> to <see cref="LottaCatalog"/>: storage that needs
/// nothing but Lucene <see cref="LuceneDirectory"/> objects.
/// </summary>
public static class LottaLuceneExtensions
{
    /// <summary>
    /// Store everything in in-memory Lucene <see cref="LuceneDirectory"/> objects: tables, blobs,
    /// queues and the search index. All data lives in RAM and is lost when the process exits.
    /// See <see cref="UseLucene(LottaCatalog, Func{string, LuceneDirectory})"/>.
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    public static LottaCatalog UseLucene(this LottaCatalog catalog)
        => catalog.UseLucene(LuceneDirectories.Memory());

    /// <summary>
    /// Store everything in Lucene <see cref="LuceneDirectory"/> objects from
    /// <paramref name="directoryFactory"/>, e.g.
    /// <c>catalog.UseLucene(LuceneDirectories.FileSystem(path))</c>.
    /// <para>
    /// Tables, blobs and queues are the clients of <c>Iciclecreek.Azure.Storage.Lucene</c>: each
    /// table, blob container and queue is a Lucene <see cref="LuceneDirectory"/> of its own
    /// (<c>tables/{table}</c>, <c>blobs/{container}</c>, <c>queues/{queue}</c>). Each database's
    /// search index is one more (<c>{catalog}/{database}/Search</c>). As with every provider, the
    /// tables and blobs are the record and the search index is built from them, so it can be
    /// rebuilt with <see cref="LottaDB.RebuildSearchIndex"/>.
    /// </para>
    /// <para>
    /// A table, blob or queue write is durable when it returns. Search index writes become durable
    /// when committed (see <see cref="ILottaConfiguration.AutoCommitDelay"/>).
    /// </para>
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    /// <param name="directoryFactory">
    /// Returns the Lucene <see cref="LuceneDirectory"/> for a path such as <c>tables/{table}</c> or
    /// <c>{catalog}/{database}/Search</c>; see <see cref="LuceneDirectories"/>.
    /// </param>
    public static LottaCatalog UseLucene(this LottaCatalog catalog, Func<string, LuceneDirectory> directoryFactory)
        => catalog.UseStorage("Lucene",
            () => new LuceneTableServiceClient(directoryFactory),
            () => new LuceneBlobServiceClient(directoryFactory),
            () => new LuceneQueueServiceClient(directoryFactory),
            directoryFactory);
}
