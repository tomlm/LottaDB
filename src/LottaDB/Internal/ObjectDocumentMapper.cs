using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using System.Text.Json;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// Universal document mapper that deserializes any Lucene document as the correct type.
/// Dispatches based on the _type_ field: CLR types are deserialized to their concrete type,
/// dynamic schema types are returned as JsonDocument.
/// Used by Search&lt;object&gt;() for untyped cross-type searches.
/// </summary>
internal class ObjectDocumentMapper : DocumentMapperBase<object>
{
    private readonly LottaDB _db;

    public ObjectDocumentMapper(Version version, Analyzer analyzer, LottaDB db)
        : base(version, analyzer)
    {
        _db = db;
        DefaultSearchProperty = StorageFields.Content;
        AddField(new JsonFieldMapper<object>(version, analyzer));
        AddField(new ContentFieldMapper<object>(version, analyzer, []));
    }

    public override object CreateFromDocument(Document source, IQueryExecutionContext context,
        Type actualType, ObjectLookup<object> factory)
    {
        return EntityMapper.FromLuceneDocument(source, _db);
    }

    public override bool IsModified(object item, Document document) => true;
}
