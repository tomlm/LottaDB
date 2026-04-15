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
/// Extends ReflectionDocumentMapper to:
/// - Store _json for full POCO roundtrip fidelity
/// - Deserialize from _json instead of field-by-field
/// - Only index properties with explicit attributes (IndexAllProperties = false)
/// - Apply [Queryable] and fluent-configured fields via PropertyMap
/// </summary>
internal class LottaDocumentMapper<T> : ReflectionDocumentMapper<T>
    where T : class, new()
{
    private const string JSON = "_json_";

    protected override bool IndexAllProperties => false;

    public LottaDocumentMapper() : base(Version.LUCENE_48)
    {
    }

    public LottaDocumentMapper(Version version) : base(version)
    {
    }

    protected LottaDocumentMapper(Version version, Analyzer externalAnalyzer)
        : base(version, externalAnalyzer)
    {
    }

    /// <summary>
    /// Applies configuration from TypeMetadata: adds indexed properties (from [Queryable]
    /// or fluent AddQueryable/AddField) and ensures a key field exists for delete support.
    /// Uses PropertyMap from Lucene.Net.Linq for correct type-aware field mapping.
    /// </summary>
    internal void ApplyFluentConfig(TypeMetadata meta)
    {
        var classMap = new ClassMap<T>(Version.LUCENE_48);

        // Add queryable/field-indexed properties
        foreach (var indexed in meta.IndexedProperties)
        {
            if (fieldMap.ContainsKey(indexed.Property.Name))
                continue; // already mapped via attribute

            var propMap = classMap.Property(PropExpr(indexed.Property));
            if (indexed.IsNotAnalyzed)
                propMap.NotAnalyzed();
            AddField(propMap.ToFieldMapper());
        }

        // Ensure key field exists for delete support
        if (keyFields.Count == 0 && meta.KeyProperty != null)
        {
            if (!fieldMap.TryGetValue(meta.KeyProperty.Name, out var keyMapper))
            {
                var keyPropMap = classMap.Key(PropExpr(meta.KeyProperty)).NotAnalyzed();
                keyMapper = keyPropMap.ToFieldMapper();
                AddKeyField(keyMapper);
            }
            else
            {
                keyFields.Add(keyMapper);
            }
        }
    }

    /// <summary>
    /// Builds a trivial expression x => x.Property for use with ClassMap.Property().
    /// </summary>
    private static System.Linq.Expressions.Expression<Func<T, object>> PropExpr(PropertyInfo prop)
    {
        var param = System.Linq.Expressions.Expression.Parameter(typeof(T), "x");
        var access = System.Linq.Expressions.Expression.Property(param, prop);
        var boxed = System.Linq.Expressions.Expression.Convert(access, typeof(object));
        return System.Linq.Expressions.Expression.Lambda<Func<T, object>>(boxed, param);
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

        // Fall back to field-by-field for documents without _json_
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
