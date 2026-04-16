using System.ComponentModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Fluent;
using Lucene.Net.Linq.Mapping;
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
    private static UtcDateTimeConverter _dtConverter = new UtcDateTimeConverter("yyyyMMddTHHmmssfffZ");
    private static UtcDateTimeOffsetConverter _dtoConverter = new UtcDateTimeOffsetConverter("yyyyMMddTHHmmssfffZ");

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

            if (indexed.Property.PropertyType == typeof(DateTime) || indexed.Property.PropertyType == typeof(DateTime?))
                propMap.ConvertWith(_dtConverter);
            else if (indexed.Property.PropertyType == typeof(DateTimeOffset) || indexed.Property.PropertyType == typeof(DateTimeOffset?))
                propMap.ConvertWith(_dtoConverter);
            else if (IsNumericType(indexed.Property.PropertyType))
                propMap.AsNumericField();

            if (indexed.IsNotAnalyzed)
                propMap.NotAnalyzed();

            propMap.NotStored(); // we don't store any of the propertiies as we have the _json_
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
        AddField(new JsonFieldMapper<T>(version));
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
    }

    public override T CreateFromDocument(Document source, IQueryExecutionContext context,
          Type actualType, ObjectLookup<T> factory)
    {
        var json = source.Get(LottaDB.JSON_FIELD);
        if (json != null)
        {
            return (T)JsonSerializer.Deserialize(json, actualType ?? typeof(T))!;
        }

        return base.CreateFromDocument(source, context, actualType, factory);
    }

    public override bool IsModified(T item, Document document)
    {
        var json1 = document.Get(LottaDB.JSON_FIELD);
        if (String.IsNullOrEmpty(json1))
            return true;
        var json2 = JsonSerializer.Serialize(item, item.GetType());
        return json1 != json2;
    }

    static bool IsNumericType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        return Type.GetTypeCode(type) switch
        {
            TypeCode.Int32 => true,
            TypeCode.Int64 => true,
            TypeCode.Double => true,
            TypeCode.Single => true,
            _ => false
        };
    }
}
