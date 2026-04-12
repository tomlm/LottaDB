using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Lucene.Net.Util;

namespace LottaDB.Internal;

/// <summary>
/// Manages a single Lucene index: one IndexWriter, lazy flush, IndexSearcher snapshot.
///
/// Writes (Add/Delete) go to the IndexWriter and set a dirty flag.
/// No flush happens until Search() is called.
/// Search() flushes if dirty, creates a fresh IndexSearcher snapshot, and returns it.
/// </summary>
internal class LuceneIndex : IDisposable
{
    private const LuceneVersion Version = LuceneVersion.LUCENE_48;

    private readonly LuceneDirectory _directory;
    private readonly IndexWriter _writer;
    private readonly Lucene.Net.Linq.LuceneDataProvider _provider;
    private bool _dirty;
    private readonly object _lock = new();

    public LuceneIndex(LuceneDirectory directory)
    {
        _directory = directory;

        var config = new IndexWriterConfig(Version, new StandardAnalyzer(Version))
        {
            OpenMode = OpenMode.CREATE_OR_APPEND
        };
        _writer = new IndexWriter(directory, config);
        _writer.Commit();

        // Create the LuceneDataProvider with our managed writer
        _provider = new Lucene.Net.Linq.LuceneDataProvider(directory, Version);
    }

    /// <summary>
    /// Get the LuceneDataProvider for writing (OpenSession) and reading (AsQueryable).
    /// </summary>
    public Lucene.Net.Linq.LuceneDataProvider Provider => _provider;

    /// <summary>
    /// Add/update objects via a session. Marks the index as dirty.
    /// The session commits on dispose but the searcher is NOT refreshed until Search is called.
    /// </summary>
    public void WriteWithSession<T>(Action<Lucene.Net.Linq.ISession<T>> action) where T : class, new()
    {
        using var session = _provider.OpenSession<T>();
        action(session);
        // Session commits on dispose
        lock (_lock) { _dirty = true; }
    }

    /// <summary>
    /// Get a queryable that reflects all committed writes.
    /// If dirty, forces a flush and creates a fresh IndexSearcher.
    /// </summary>
    public IQueryable<T> Search<T>() where T : class, new()
    {
        lock (_lock)
        {
            if (_dirty)
            {
                // Flush writes and commit
                _writer.Flush(triggerMerge: false, applyAllDeletes: true);
                _writer.Commit();
                _dirty = false;
            }
        }

        // Create a fresh read-only provider to get a new IndexSearcher
        // that sees all committed data. This provider is read-only
        // (AsQueryable doesn't create a writer).
        var readProvider = new Lucene.Net.Linq.LuceneDataProvider(_directory, Version);
        return readProvider.AsQueryable<T>();
    }

    /// <summary>
    /// Delete all documents. Marks dirty.
    /// </summary>
    public void DeleteAll()
    {
        _writer.DeleteAll();
        _writer.Commit();
        lock (_lock) { _dirty = true; }
    }

    public void Dispose()
    {
        _provider?.Dispose();
        _writer?.Dispose();
    }
}
