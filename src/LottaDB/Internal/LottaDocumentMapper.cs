using System.Reflection;
using System.Text.Json;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// Wraps a ClassMap-built mapper to:
/// - ToDocument: indexes [Field] properties for search + stores _json for full POCO fidelity
/// - ToObject: deserializes from _json (full roundtrip) instead of field-by-field
/// - PrepareSearchSettings: filters by _type DocumentKey
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
    /// Applies fluent configuration from TypeMetadata: adds indexed fields and
    /// ensures a key field exists for Lucene session.Delete().
    /// </summary>
    internal void ApplyFluentConfig(TypeMetadata meta)
    {
        // Add fluent-indexed properties to the Lucene field map
        foreach (var indexed in meta.IndexedProperties)
        {
            if (fieldMap.ContainsKey(indexed.Property.Name))
                continue; // already mapped via attribute

            var mapper = FieldMappingInfoBuilder.Build<T>(indexed.Property, Version.LUCENE_48, null);
            AddField(mapper);

            if (indexed.IsKey && !keyFields.Contains(mapper))
                keyFields.Add(mapper);
        }

        // Ensure key field exists for delete support
        if (keyFields.Count == 0 && meta.KeyProperty != null)
        {
            if (!fieldMap.TryGetValue(meta.KeyProperty.Name, out var keyMapper))
            {
                keyMapper = FieldMappingInfoBuilder.Build<T>(meta.KeyProperty, Version.LUCENE_48, null);
                AddKeyField(keyMapper);
            }
            else
            {
                keyFields.Add(keyMapper);
            }
        }
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
