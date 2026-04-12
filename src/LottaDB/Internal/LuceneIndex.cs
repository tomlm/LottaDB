using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Lucene.Net.Util;

namespace LottaDB.Internal;

/// <summary>
/// Manages a single Lucene index via LuceneDataProvider.
/// Writes via sessions (auto-commit on dispose + context.Reload refreshes searcher).
/// Reads via AsQueryable (always sees committed data).
/// </summary>
internal class LuceneIndex : IDisposable
{
    private const LuceneVersion Version = LuceneVersion.LUCENE_48;

    private readonly LuceneDirectory _directory;
    private readonly Lucene.Net.Linq.LuceneDataProvider _provider;

    public LuceneIndex(LuceneDirectory directory)
    {
        _directory = directory;

        // Initialize the index so it exists
        var config = new IndexWriterConfig(Version, new StandardAnalyzer(Version))
        {
            OpenMode = OpenMode.CREATE_OR_APPEND
        };
        using (var writer = new IndexWriter(directory, config))
        {
            writer.Commit();
        }

        _provider = new Lucene.Net.Linq.LuceneDataProvider(directory, Version);
    }

    /// <summary>
    /// Write objects via a session. Session auto-commits on dispose,
    /// and context.Reload() refreshes the IndexSearcher.
    /// </summary>
    public void WriteWithSession<T>(Action<Lucene.Net.Linq.ISession<T>> action) where T : class, new()
    {
        using var session = _provider.OpenSession<T>();
        action(session);
    }

    /// <summary>
    /// Get a queryable over the index. Always reflects committed writes
    /// because session commit triggers context.Reload().
    /// </summary>
    public IQueryable<T> Search<T>() where T : class, new()
    {
        return _provider.AsQueryable<T>();
    }

    /// <summary>
    /// Delete all documents via a typed session.
    /// </summary>
    public void DeleteAll<T>() where T : class, new()
    {
        using var session = _provider.OpenSession<T>();
        session.DeleteAll();
    }

    public void Dispose()
    {
        _provider?.Dispose();
    }
}
