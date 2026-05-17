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
        DefaultSearchProperty = LottaDB.CONTENT_FIELD;
        AddField(new JsonFieldMapper<object>(version, analyzer));
        AddField(new ContentFieldMapper<object>(version, analyzer, []));
    }

    public override object CreateFromDocument(Document source, IQueryExecutionContext context,
        Type actualType, ObjectLookup<object> factory)
    {
        var json = source.Get(LottaDB.OBJECT_FIELD);
        if (json == null)
            throw new InvalidOperationException(
                $"Lucene document missing '{LottaDB.OBJECT_FIELD}' field. Key: {source.Get(LottaDB.KEY_FIELD) ?? "unknown"}. Index may be corrupted — consider calling RebuildSearchIndex().");

        var typeName = source.Get("_type_");
        var etag = source.Get(LottaDB.ETAG_FIELD);
        var keyValue = source.Get(LottaDB.KEY_FIELD);

        object result;
        if (typeName != null && JsonMetadata.IsJsonTypeName(typeName))
        {
            var jsonDoc = JsonDocument.Parse(json);
            if (etag != null) jsonDoc.SetETag(etag);
            if (keyValue != null) jsonDoc.SetKey(keyValue);
            result = jsonDoc;
        }
        else
        {
            var obj = TypeUtils.DeserializeFromTypeName(json, typeName!)
                ?? throw new InvalidOperationException(
                    $"Failed to deserialize type '{typeName}' from Lucene document. Key: {source.Get(LottaDB.KEY_FIELD) ?? "unknown"}. Index may be corrupted — consider calling RebuildSearchIndex().");
            if (etag != null) obj.SetETag(etag);
            if (keyValue != null) obj.SetKey(keyValue);
            if (obj is BlobFile bf && _db != null) bf.Database = _db;
            result = obj;
        }

        return result;
    }

    public override bool IsModified(object item, Document document) => true;
}
