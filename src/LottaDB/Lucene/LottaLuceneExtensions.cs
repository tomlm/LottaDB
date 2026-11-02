namespace Lotta;

/// <summary>
/// Adds <see cref="UseLucene"/> to <see cref="LottaCatalog"/>: pure-Lucene storage.
/// </summary>
public static class LottaLuceneExtensions
{
    /// <summary>
    /// Store objects in the Lucene search index itself instead of a separate table store.
    /// Each object is written once — into its database's index — and point reads, queries and
    /// optimistic concurrency are served from there. Call after a provider that supplies the
    /// Lucene directory and blob storage, e.g. <c>catalog.UseFileSystem(path).UseLucene()</c>.
    /// <para>
    /// Index writes become durable when committed (see <see cref="ILottaConfiguration.AutoCommitDelay"/>),
    /// so a crash can lose writes made since the last commit. Reads in the writing process always
    /// see its own writes; other processes see committed state.
    /// </para>
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    public static LottaCatalog UseLucene(this LottaCatalog catalog)
    {
        catalog.TableServiceClientFactory = () => new LuceneTableServiceClient(catalog);
        return catalog;
    }
}
