using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using System.Runtime.CompilerServices;

namespace Lotta;

/// <summary>
/// Primary-key lookup against one index reader. A <c>TermQuery</c> search builds its term
/// statistics by seeking the term in every segment with a freshly allocated terms enum, plus a
/// scorer and a top-docs collector — tens of KB per lookup on a near-real-time index with many
/// segments. This keeps one terms enum and docs enum per segment and reuses them, checking the
/// largest segments first and stopping at the first live hit.
/// </summary>
internal sealed class IndexKeyLookup
{
    private static readonly ConditionalWeakTable<IndexReader, IndexKeyLookup> s_lookups = new();

    private readonly string _field;
    private readonly AtomicReaderContext[] _segments;
    private readonly TermsEnum?[] _terms;
    private readonly DocsEnum?[] _docs;
    private readonly BytesRef _key = new();
    private readonly object _lock = new();

    private IndexKeyLookup(IndexReader reader, string field)
    {
        _field = field;
        // Largest segments first: a key is most likely in one of them.
        _segments = reader.Leaves.OrderByDescending(l => l.AtomicReader.MaxDoc).ToArray();
        _terms = new TermsEnum?[_segments.Length];
        _docs = new DocsEnum?[_segments.Length];
        for (var i = 0; i < _segments.Length; i++)
            _terms[i] = _segments[i].AtomicReader.GetTerms(field)?.GetEnumerator();
    }

    /// <summary>The lookup for a reader, created on first use and dropped with the reader.</summary>
    public static IndexKeyLookup For(IndexReader reader, string field)
    {
        var lookup = s_lookups.GetValue(reader, r => new IndexKeyLookup(r, field));
        return lookup._field == field ? lookup : new IndexKeyLookup(reader, field);
    }

    /// <summary>The top-level doc id of the live document with this key, or -1.</summary>
    public int Find(string key)
    {
        // The enums are stateful, so lookups on one reader take turns. Each is a few seeks.
        lock (_lock)
        {
            _key.CopyChars(key);
            for (var i = 0; i < _segments.Length; i++)
            {
                var terms = _terms[i];
                if (terms == null || !terms.SeekExact(_key)) continue;

                var segment = _segments[i].AtomicReader;
                _docs[i] = terms.Docs(segment.LiveDocs, _docs[i], DocsFlags.NONE);
                var doc = _docs[i]!.NextDoc();
                if (doc != DocIdSetIterator.NO_MORE_DOCS)
                    return _segments[i].DocBase + doc;
            }
            return -1;
        }
    }
}
