using System.Text.Json;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Search;

namespace Lotta.Internal;

/// <summary>
/// Wraps a ClassMap-built mapper to:
/// - ToDocument: indexes [Field] properties for search + stores _json for full POCO fidelity
/// - ToObject: deserializes from _json (full roundtrip) instead of field-by-field
/// - PrepareSearchSettings: filters by _type DocumentKey
/// </summary>
internal class LottaDocumentMapper<T> : IDocumentMapper<T>, IDocumentModificationDetector<T> where T : class, new()
{
    private readonly IDocumentMapper<T> _inner;

    public LottaDocumentMapper(IDocumentMapper<T> inner)
    {
        _inner = inner;
    }

    public Lucene.Net.Linq.Analysis.PerFieldAnalyzer Analyzer => _inner.Analyzer;

    public void ToDocument(T source, Document target)
    {
        // Index all [Field] properties via the ClassMap mapper (for search/filter/sort)
        _inner.ToDocument(source, target);

        // Store _json for full POCO deserialization on read
        target.RemoveField("_json");
        target.Add(new StringField("_json",
            JsonSerializer.Serialize(source, source.GetType()),
            Field.Store.YES));
    }

    public void ToObject(Document source, IQueryExecutionContext context, T target)
    {
        // Deserialize from _json for full POCO fidelity
        var json = source.Get("_json");
        if (json != null)
        {
            var deserialized = JsonSerializer.Deserialize<T>(json);
            if (deserialized != null)
            {
                ObjectCopier<T>.Copy(deserialized, target);
                return;
            }
        }

        // Fallback to field-by-field hydration
        _inner.ToObject(source, context, target);
    }

    public IDocumentKey ToKey(T source) => _inner.ToKey(source);

    public void PrepareSearchSettings(IQueryExecutionContext context)
        => _inner.PrepareSearchSettings(context);

    public bool Equals(T item1, T item2) => _inner.Equals(item1, item2);

    public IFieldMappingInfo GetMappingInfo(string propertyName) => _inner.GetMappingInfo(propertyName);
    public Query CreateMultiFieldQuery(string query) => _inner.CreateMultiFieldQuery(query);
    public IEnumerable<string> AllProperties => _inner.AllProperties;
    public IEnumerable<string> KeyProperties => _inner.KeyProperties;
    public IEnumerable<string> IndexedProperties => _inner.IndexedProperties;

    public bool IsModified(T item, Document document)
    {
        var json1 = document.Get("_json");
        if (String.IsNullOrEmpty(json1))
            return true;
        var json2 = JsonSerializer.Serialize(item, item.GetType());
        return json1 == json2;
    }
}
