using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Core;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
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
        var stringCollectionProperties = new List<IndexedTypeProperty>();
        foreach (var indexed in meta.IndexedProperties)
        {
            if (meta.KeyProperty != null && indexed.Property == meta.KeyProperty)
                continue;

            // String collections get their own multi-valued mapper (one Lucene field per element)
            if (TypeUtils.IsStringCollection(indexed.Property.PropertyType))
            {
                stringCollectionProperties.Add(indexed);
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

        // Add string collection field mappers (multi-valued Lucene fields)
        foreach (var indexed in stringCollectionProperties)
            AddField(new StringCollectionFieldMapper<T>(indexed.Property, indexed.IsNotAnalyzed, _propertyAnalyzer));

        if (meta.DefaultSearchProperty != null)
        {
            // User-defined default search property — skip _content_ composite field
            DefaultSearchProperty = meta.DefaultSearchProperty.Name;
        }
        else
        {
            // No user default — create _content_ composite field (include string collection properties)
            var contentProps = meta.IndexedProperties
                .Where(p => !p.IsNotAnalyzed && (p.Property.PropertyType == typeof(string) || TypeUtils.IsStringCollection(p.Property.PropertyType)))
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

    // For simplicity, we always treat documents as modified. Lucene.Net.Linq's upsert logic will compare the new document
    // to the existing one and skip reindexing if they're identical, so this won't cause unnecessary updates.
    public override bool IsModified(T item, Document document) => true;

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
/// Field mapper for string collection properties (string[], List&lt;string&gt;, IEnumerable&lt;string&gt;…).
/// Adds each element as a separate field with the same name, enabling multi-valued search:
/// <c>x.Tags.Contains("a")</c> matches documents where any element matches.
/// Analyzed collections tokenize each element; NotAnalyzed collections match whole elements verbatim.
/// </summary>
internal class StringCollectionFieldMapper<T> : IFieldMapper<T> where T : class, new()
{
    private readonly PropertyInfo _property;
    private readonly bool _isNotAnalyzed;
    private readonly Analyzer _analyzer;
    private readonly Version _version;

    public StringCollectionFieldMapper(PropertyInfo property, bool isNotAnalyzed, Analyzer analyzer)
    {
        _property = property;
        _isNotAnalyzed = isNotAnalyzed;
        _analyzer = isNotAnalyzed ? new KeywordAnalyzer() : analyzer;
        _version = Version.LUCENE_48;
    }

    public string FieldName => _property.Name;
    public string PropertyName => _property.Name;
    public Analyzer Analyzer => _analyzer;
    public IndexMode IndexMode => _isNotAnalyzed ? IndexMode.NotAnalyzed : IndexMode.Analyzed;

    public void CopyToDocument(T source, Document target)
    {
        target.RemoveFields(_property.Name);
        if (_property.GetValue(source) is not IEnumerable<string> values) return;
        foreach (var element in values)
        {
            if (string.IsNullOrEmpty(element))
                continue;
            target.Add(_isNotAnalyzed
                ? new StringField(_property.Name, element, Field.Store.NO)
                : new TextField(_property.Name, element, Field.Store.NO));
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
        // QueryParser splits on whitespace before analysis, which would break a verbatim
        // element like "Iowa Property" into two terms. Build the single term directly instead.
        if (_isNotAnalyzed && pattern.Any(char.IsWhiteSpace))
            return CreateVerbatimQuery(pattern);

        var parser = new QueryParser(_version, FieldName, _analyzer)
        {
            AllowLeadingWildcard = true,
            LowercaseExpandedTerms = !_isNotAnalyzed,
        };
        return parser.Parse(pattern);
    }

    /// <summary>
    /// Term query for an escaped pattern, or a wildcard query when it has unescaped * or ? characters.
    /// </summary>
    private Query CreateVerbatimQuery(string pattern)
    {
        // literal: the element text with escapes removed. wildcard: the same text for WildcardQuery,
        // where unescaped * and ? stay wildcards and escaped ones stay escaped (literal).
        var literal = new System.Text.StringBuilder(pattern.Length);
        var wildcard = new System.Text.StringBuilder(pattern.Length);
        var isWildcard = false;
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == '\\' && i + 1 < pattern.Length)
            {
                c = pattern[++i];
                if (c is '*' or '?' or '\\')
                    wildcard.Append('\\');
            }
            else if (c is '*' or '?')
            {
                isWildcard = true;
            }
            literal.Append(c);
            wildcard.Append(c);
        }

        return isWildcard
            ? new WildcardQuery(new Term(FieldName, wildcard.ToString()))
            : new TermQuery(new Term(FieldName, literal.ToString()));
    }

    public Query CreateRangeQuery(object lowerBound, object upperBound, RangeType lowerRange, RangeType upperRange)
        => throw new NotSupportedException("Range queries not supported on string collection fields");

    public SortField CreateSortField(bool reverse)
        => throw new NotSupportedException("Sorting not supported on string collection fields");
}
