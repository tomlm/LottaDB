using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Analysis;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using System.Text;
using System.Text.Json;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// Document mapper for dynamic (JSON Schema-defined) documents.
/// Implements IDocumentMapper and IFieldMappingInfoProvider to work with JsonElement directly.
/// </summary>
internal class DynamicDocumentMapper : IDocumentMapper, IFieldMappingInfoProvider
{
    private readonly DynamicSchema _schema;
    private readonly Version _version;
    private readonly Dictionary<string, DynamicFieldMappingInfo> _fieldMappings = new(StringComparer.Ordinal);
    private readonly Analyzer _contentAnalyzer;

    public DynamicDocumentMapper(DynamicSchema schema, Version version, Analyzer contentAnalyzer)
    {
        _schema = schema;
        _version = version;
        _contentAnalyzer = contentAnalyzer;

        // Build per-field analyzer
        Analyzer = new PerFieldAnalyzer(new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        Analyzer.AddAnalyzer(LottaDB.KEY_FIELD, new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        Analyzer.AddAnalyzer(LottaDB.CONTENT_FIELD, contentAnalyzer);

        // Build field mappings for each schema property
        var propertyAnalyzer = new StandardAnalyzer(version);
        foreach (var prop in schema.Properties)
        {
            var mapping = new DynamicFieldMappingInfo(
                prop.Name, prop.ClrType, prop.IsAnalyzed, version, propertyAnalyzer);
            _fieldMappings[prop.Name] = mapping;

            Analyzer.AddAnalyzer(prop.Name,
                prop.IsAnalyzed ? propertyAnalyzer : new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        }
    }

    public Type MappedType => typeof(JsonElement);

    public PerFieldAnalyzer Analyzer { get; }

    // === IFieldMappingInfoProvider ===

    public IEnumerable<string> AllProperties =>
        new[] { LottaDB.KEY_FIELD, LottaDB.CONTENT_FIELD }
        .Concat(_fieldMappings.Keys);

    public IEnumerable<string> KeyProperties => new[] { LottaDB.KEY_FIELD };

    public IEnumerable<string> IndexedProperties =>
        _fieldMappings.Keys;

    public string DefaultSearchProperty => LottaDB.CONTENT_FIELD;

    public IFieldMappingInfo GetMappingInfo(string propertyName)
    {
        if (propertyName == LottaDB.CONTENT_FIELD)
            return new DynamicContentFieldMappingInfo(_version, _contentAnalyzer);
        if (_fieldMappings.TryGetValue(propertyName, out var mapping))
            return mapping;
        throw new KeyNotFoundException($"Unrecognized field: '{propertyName}'");
    }

    public Query CreateMultiFieldQuery(string pattern)
    {
        var fields = _fieldMappings.Keys.ToArray();
        var parser = new MultiFieldQueryParser(_version, fields, _contentAnalyzer);
        return parser.Parse(pattern);
    }

    // === IDocumentMapper ===

    public void ToDocument(object source, Document target)
    {
        var json = source is JsonDocument jd ? jd.RootElement : (JsonElement)source;
        var key = _schema.ExtractKey(json);

        // _key_ field
        target.Add(new StringField(LottaDB.KEY_FIELD, key, Field.Store.YES));

        // _type_ field — prefixed schema name as type discriminator
        target.Add(new StringField("_type_", _schema.StorageTypeName, Field.Store.YES));

        // _object_ field — full JSON stored (not indexed)
        target.Add(new StoredField(LottaDB.OBJECT_FIELD, json.GetRawText()));

        // Per-property indexed fields
        var contentBuilder = new StringBuilder();
        foreach (var prop in _schema.Properties)
        {
            if (!json.TryGetProperty(prop.Name, out var val) || val.ValueKind == JsonValueKind.Null)
                continue;

            AddFieldToDocument(target, prop, val);

            // Accumulate analyzed string fields into _content_
            if (prop.IsAnalyzed && prop.ClrType == typeof(string))
            {
                var s = val.GetString();
                if (!string.IsNullOrEmpty(s))
                {
                    if (contentBuilder.Length > 0) contentBuilder.Append(' ');
                    contentBuilder.Append(s);
                }
            }
        }

        // _content_ composite field
        if (contentBuilder.Length > 0)
            target.Add(new TextField(LottaDB.CONTENT_FIELD, contentBuilder.ToString(), Field.Store.NO));
    }

    public void ToObject(Document source, object target)
    {
        // Not needed — we read _object_ directly
    }

    public void PrepareSearchSettings(IQueryExecutionContext context)
    {
        // No special settings needed
    }

    private static void AddFieldToDocument(Document target, SchemaPropertyDef prop, JsonElement val)
    {
        if (prop.ClrType == typeof(int))
        {
            if (val.TryGetInt32(out var i))
                target.Add(new Int32Field(prop.Name, i, Field.Store.NO));
        }
        else if (prop.ClrType == typeof(double))
        {
            if (val.TryGetDouble(out var d))
                target.Add(new DoubleField(prop.Name, d, Field.Store.NO));
        }
        else if (prop.ClrType == typeof(bool))
        {
            // Index booleans as "true"/"false" strings (not analyzed)
            target.Add(new StringField(prop.Name, val.GetBoolean().ToString().ToLowerInvariant(), Field.Store.NO));
        }
        else // string (and fallback)
        {
            var s = val.GetString() ?? val.GetRawText();
            if (prop.IsAnalyzed)
                target.Add(new TextField(prop.Name, s, Field.Store.NO));
            else
                target.Add(new StringField(prop.Name, s, Field.Store.NO));
        }
    }
}

/// <summary>
/// Minimal IFieldMappingInfo for the _content_ composite field.
/// Used by query parser when no specific field is targeted.
/// </summary>
internal class DynamicContentFieldMappingInfo : IFieldMappingInfo
{
    private readonly Version _version;
    private readonly Analyzer _analyzer;

    public DynamicContentFieldMappingInfo(Version version, Analyzer analyzer)
    {
        _version = version;
        _analyzer = analyzer;
    }

    public string FieldName => LottaDB.CONTENT_FIELD;
    public string PropertyName => LottaDB.CONTENT_FIELD;

    public string ConvertToQueryExpression(object value) => value?.ToString() ?? string.Empty;

    public string EscapeSpecialCharacters(string str)
        => QueryParserBase.Escape(str ?? string.Empty);

    public Query CreateQuery(string pattern)
    {
        var parser = new QueryParser(_version, FieldName, _analyzer)
        {
            AllowLeadingWildcard = true,
            LowercaseExpandedTerms = true,
        };
        return parser.Parse(pattern);
    }

    public Query CreateRangeQuery(object lowerBound, object upperBound,
        Lucene.Net.Linq.Search.RangeType lowerRange, Lucene.Net.Linq.Search.RangeType upperRange)
        => throw new NotSupportedException("Range queries not supported on _content_ field");

    public SortField CreateSortField(bool reverse)
        => throw new NotSupportedException("Sorting not supported on _content_ field");
}
