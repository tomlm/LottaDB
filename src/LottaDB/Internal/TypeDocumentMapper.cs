using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Fluent;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Linq.Search;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Microsoft.Extensions.AI;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Lotta;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// LottaDB's document mapper. Built on ClassMap from Lucene.Net.Linq's fluent API.
/// Only maps properties explicitly marked with [Key], [Queryable], [Field], or
/// configured via fluent AddQueryable/AddField. Stores _json for full POCO roundtrip.
/// </summary>
internal class TypeDocumentMapper<T> : DocumentMapperBase<T>
    where T : class, new()
{
    private static UtcDateTimeConverter _dtConverter = new UtcDateTimeConverter("yyyyMMddTHHmmssfffZ");
    private static UtcDateTimeOffsetConverter _dtoConverter = new UtcDateTimeOffsetConverter("yyyyMMddTHHmmssfffZ");
    private static readonly Analyzer _propertyAnalyzer = new StandardAnalyzer(Version.LUCENE_48);
    private readonly LottaDB _db;

    public TypeDocumentMapper(Version version, Analyzer analyzer, TypeMetadata? meta, IEmbeddingGenerator<string, Embedding<float>>? embeddingGenerator, LottaDB db)
        : base(version, analyzer)
    {
        _db = db;
        if (meta == null) return;
        var classMap = new ClassMap<T>(version);

        // Key property → Lucene document key for upsert/delete
        if (meta.KeyProperty != null)
            classMap.Key(PropExpr(meta.KeyProperty)).NotAnalyzed();

        // Indexed properties from [Queryable], [Field], or fluent config
        var stringArrayProperties = new List<PropertyInfo>();
        foreach (var indexed in meta.IndexedProperties)
        {
            if (meta.KeyProperty != null && indexed.Property == meta.KeyProperty)
                continue;

            // string[] can't go through ClassMap — handle separately
            if (indexed.Property.PropertyType == typeof(string[]))
            {
                stringArrayProperties.Add(indexed.Property);
                continue;
            }

            var propMap = classMap.Property(PropExpr(indexed.Property));

            if (indexed.Property.PropertyType == typeof(DateTime) || indexed.Property.PropertyType == typeof(DateTime?))
                propMap.ConvertWith(_dtConverter);
            else if (indexed.Property.PropertyType == typeof(DateTimeOffset) || indexed.Property.PropertyType == typeof(DateTimeOffset?))
                propMap.ConvertWith(_dtoConverter);
            else if (IsNumericType(indexed.Property.PropertyType))
                propMap.AsNumericField();
            else
            {
                if (indexed.IsNotAnalyzed)
                    propMap.NotAnalyzed();
                else
                    // Per-property fields always use StandardAnalyzer so LINQ .Contains/.StartsWith/.EndsWith
                    // produce wildcard terms that match the indexed tokens verbatim. The configured Analyzer
                    // (which may stem) applies only to the _content_ free-text field.
                    propMap.AnalyzeWith(_propertyAnalyzer).Analyzed();
            }

            propMap.NotStored(); // we don't store any of the propertiies as we have the _json_

            if (indexed.IsVectorField)
            {
                // Vector field: analyzed for full-text + vector embeddings for similarity search.
                // AsVectorField() creates a new VectorPropertyMap replacing this one in the ClassMap,
                // so we chain all config on the returned instance.
                propMap.AsVectorField()
                    .WithEmbeddingGenerator(embeddingGenerator);
            }
        }

        // Build the mapper via ClassMap, then extract its fields into ourselves
        var source = classMap.ToDocumentMapper();
        foreach (var propName in source.KeyProperties)
        {
            var fieldMapper = (IFieldMapper<T>)source.GetMappingInfo(propName);
            AddKeyField(new LottaDocumentKeyFieldMapper<T>(fieldMapper));
        }

        foreach (var propName in source.AllProperties)
        {
            if (fieldMap.ContainsKey(propName))
                continue; // already added as key field
            var fieldMapper = (IFieldMapper<T>)source.GetMappingInfo(propName);
            AddField(fieldMapper);
        }
        AddField(new JsonFieldMapper<T>(version, analyzer));

        // Add string[] field mappers (multi-valued Lucene fields)
        foreach (var prop in stringArrayProperties)
            AddField(new StringArrayFieldMapper<T>(prop, _propertyAnalyzer));

        if (meta.DefaultSearchProperty != null)
        {
            // User-defined default search property — skip _content_ composite field
            DefaultSearchProperty = meta.DefaultSearchProperty.Name;
        }
        else
        {
            // No user default — create _content_ composite field (include string[] properties)
            var contentProps = meta.IndexedProperties
                .Where(p => !p.IsNotAnalyzed && (p.Property.PropertyType == typeof(string) || p.Property.PropertyType == typeof(string[])))
                .Select(p => p.Property);

            IFieldMapper<T> contentMapper = new ContentFieldMapper<T>(version, analyzer, contentProps);
            if (embeddingGenerator != null)
            {
                contentMapper = new VectorFieldMapper<T>(contentMapper, embeddingGenerator);
            }
            AddField(contentMapper);

            DefaultSearchProperty = StorageFields.Content;
        }
    }

    private static Expression<Func<T, object>> PropExpr(PropertyInfo prop)
    {
        var param = Expression.Parameter(typeof(T), "x");
        var access = Expression.Property(param, prop);
        var boxed = Expression.Convert(access, typeof(object));
        return Expression.Lambda<Func<T, object>>(boxed, param);
    }

    public override T CreateFromDocument(Document source, IQueryExecutionContext context,
          Type actualType, ObjectLookup<T> factory)
    {
        return EntityMapper.FromLuceneDocument<T>(source, _db);
    }

    public override bool IsModified(T item, Document document)
    {
        var json1 = document.Get(StorageFields.ObjectPrefix);
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

/// <summary>
/// Custom field mapper for string[] properties. Adds each array element as a separate
/// TextField with the same field name, enabling multi-valued search.
/// </summary>
internal class StringArrayFieldMapper<T> : IFieldMapper<T> where T : class, new()
{
    private readonly PropertyInfo _property;
    private readonly Analyzer _analyzer;
    private readonly Version _version;

    public StringArrayFieldMapper(PropertyInfo property, Analyzer analyzer)
    {
        _property = property;
        _analyzer = analyzer;
        _version = Version.LUCENE_48;
    }

    public string FieldName => _property.Name;
    public string PropertyName => _property.Name;
    public Analyzer Analyzer => _analyzer;
    public IndexMode IndexMode => Lucene.Net.Linq.Mapping.IndexMode.Analyzed;

    public void CopyToDocument(T source, Document target)
    {
        var value = _property.GetValue(source) as string[];
        if (value == null) return;
        foreach (var element in value)
        {
            if (!string.IsNullOrEmpty(element))
                target.Add(new TextField(_property.Name, element, Field.Store.NO));
        }
    }

    public void CopyFromDocument(Document source, IQueryExecutionContext context, T target)
    {
        // Not needed — we reconstruct from _object_ JSON
    }

    public object GetPropertyValue(T source) => _property.GetValue(source) ?? Array.Empty<string>();

    public string ConvertToQueryExpression(object value) => value?.ToString() ?? string.Empty;

    public string EscapeSpecialCharacters(string str) => QueryParserBase.Escape(str ?? string.Empty);

    public Query CreateQuery(string pattern)
    {
        var parser = new QueryParser(_version, FieldName, _analyzer)
        {
            AllowLeadingWildcard = true,
            LowercaseExpandedTerms = true,
        };
        return parser.Parse(pattern);
    }

    public Query CreateRangeQuery(object lowerBound, object upperBound, RangeType lowerRange, RangeType upperRange)
        => throw new NotSupportedException("Range queries not supported on string[] fields");

    public SortField CreateSortField(bool reverse)
        => throw new NotSupportedException("Sorting not supported on string[] fields");
}

