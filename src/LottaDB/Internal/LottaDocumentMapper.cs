using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Fluent;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// LottaDB's document mapper. Built on ClassMap from Lucene.Net.Linq's fluent API.
/// Only maps properties explicitly marked with [Key], [Queryable], [Field], or
/// configured via fluent AddQueryable/AddField. Stores _json for full POCO roundtrip.
/// </summary>
internal class LottaDocumentMapper<T> : DocumentMapperBase<T>
    where T : class, new()
{
    private const string JSON = "_json_";

    public LottaDocumentMapper(Version version, TypeMetadata? meta = null) : base(version)
    {
        if (meta == null) return;
        var classMap = new ClassMap<T>(version);

        // Key property → Lucene document key for upsert/delete
        if (meta.KeyProperty != null)
            classMap.Key(PropExpr(meta.KeyProperty)).NotAnalyzed();

        // Indexed properties from [Queryable], [Field], or fluent config
        foreach (var indexed in meta.IndexedProperties)
        {
            if (meta.KeyProperty != null && indexed.Property == meta.KeyProperty)
                continue;

            var propMap = classMap.Property(PropExpr(indexed.Property));
            if (indexed.IsNotAnalyzed)
                propMap.NotAnalyzed();
        }

        // Build the mapper via ClassMap, then extract its fields into ourselves
        var source = classMap.ToDocumentMapper();
        foreach (var propName in source.KeyProperties)
        {
            var fieldMapper = (IFieldMapper<T>)source.GetMappingInfo(propName);
            AddKeyField(fieldMapper);
        }
        foreach (var propName in source.AllProperties)
        {
            if (fieldMap.ContainsKey(propName))
                continue; // already added as key field
            var fieldMapper = (IFieldMapper<T>)source.GetMappingInfo(propName);
            AddField(fieldMapper);
        }
    }

    private static Expression<Func<T, object>> PropExpr(PropertyInfo prop)
    {
        var param = Expression.Parameter(typeof(T), "x");
        var access = Expression.Property(param, prop);
        var boxed = Expression.Convert(access, typeof(object));
        return Expression.Lambda<Func<T, object>>(boxed, param);
    }

    public override void MapFieldsToDocument(T source, Document target)
    {
        base.MapFieldsToDocument(source, target);
        target.Add(new StoredField(JSON, JsonSerializer.Serialize(source, source.GetType())));
    }

    public override T CreateFromDocument(Document source, IQueryExecutionContext context,
          Type actualType, ObjectLookup<T> factory)
    {
        var json = source.Get(JSON);
        if (json != null)
        {
            return (T)JsonSerializer.Deserialize(json, actualType ?? typeof(T))!;
        }

        return base.CreateFromDocument(source, context, actualType, factory);
    }

    public override bool IsModified(T item, Document document)
    {
        var json1 = document.Get(JSON);
        if (String.IsNullOrEmpty(json1))
            return true;
        var json2 = JsonSerializer.Serialize(item, item.GetType());
        return json1 != json2;
    }
}
