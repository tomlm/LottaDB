using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Lucene.Net.Util;

namespace LottaDB.Internal;

/// <summary>
/// Wraps a single LuceneDataProvider for one database.
/// Write sessions commit to the provider's IndexWriter.
/// On session commit, context.Reload() refreshes the IndexSearcher.
/// AsQueryable always returns fresh results after commit.
/// </summary>
internal class LuceneIndex : IDisposable
{
    private const LuceneVersion Version = LuceneVersion.LUCENE_48;
    private readonly Lucene.Net.Linq.LuceneDataProvider _provider;

    public LuceneIndex(LuceneDirectory directory)
    {
        // Ensure the index exists before opening the provider
        using (var writer = new IndexWriter(directory,
            new IndexWriterConfig(Version, new StandardAnalyzer(Version))
            { OpenMode = OpenMode.CREATE_OR_APPEND }))
        {
            writer.Commit();
        }

        _provider = new Lucene.Net.Linq.LuceneDataProvider(directory, Version);
    }

    /// <summary>Open a Lucene write session for type T.</summary>
    public Lucene.Net.Linq.ISession<T> OpenSession<T>() where T : class, new()
        => _provider.OpenSession<T>();

    /// <summary>Query the index. Returns fresh results after any session commit.</summary>
    public IQueryable<T> AsQueryable<T>() where T : class, new()
        => _provider.AsQueryable<T>();

    public void Dispose() => _provider?.Dispose();
}
