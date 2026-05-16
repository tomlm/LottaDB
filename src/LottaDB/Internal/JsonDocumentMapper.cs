using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Analysis;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Microsoft.Extensions.AI;
using System.Text;
using System.Text.Json;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// Document mapper for dynamic (JSON Schema-defined) documents.
/// Implements IDocumentMapper and IFieldMappingInfoProvider to work with JsonElement directly.
/// </summary>
internal class JsonDocumentMapper : IDocumentMapper, IFieldMappingInfoProvider
{
    private readonly JsonMetadata _schema;
    private readonly Version _version;
    private readonly Dictionary<string, JsonFieldMappingInfo> _fieldMappings = new(StringComparer.Ordinal);
    private readonly Analyzer _contentAnalyzer;
    private readonly IEmbeddingGenerator<string, Embedding<float>>? _embeddingGenerator;

    public JsonDocumentMapper(JsonMetadata schema, Version version, Analyzer contentAnalyzer,
        IEmbeddingGenerator<string, Embedding<float>>? embeddingGenerator = null)
    {
        _schema = schema;
        _version = version;
        _contentAnalyzer = contentAnalyzer;
        _embeddingGenerator = embeddingGenerator;

        // Build per-field analyzer
        Analyzer = new PerFieldAnalyzer(new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        Analyzer.AddAnalyzer(LottaDB.KEY_FIELD, new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        Analyzer.AddAnalyzer(LottaDB.CONTENT_FIELD, contentAnalyzer);

        // Build field mappings for each schema property
        foreach (var prop in schema.Properties)
        {
            var mapping = new JsonFieldMappingInfo(
                prop.Name, prop.ClrType, prop.IsAnalyzed, version);
            _fieldMappings[prop.Name] = mapping;

            Analyzer.AddAnalyzer(prop.Name,
                prop.IsAnalyzed ? new StandardAnalyzer(version) : new Lucene.Net.Analysis.Core.KeywordAnalyzer());
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

    public string DefaultSearchProperty => _schema.DefaultSearchProperty ?? LottaDB.CONTENT_FIELD;

    public IFieldMappingInfo GetMappingInfo(string propertyName)
    {
        if (propertyName == LottaDB.CONTENT_FIELD)
            return new JsonContentFieldMapper(_version, _contentAnalyzer);
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
        var jsonDoc = source as JsonDocument;
        var json = jsonDoc != null ? jsonDoc.RootElement : (JsonElement)source;
        // Use the authoritative key from metadata if available (set by SaveAsync),
        // otherwise extract from JSON. This prevents auto-key from generating a new ULID.
        var key = jsonDoc?.GetKey() ?? _schema.GetKey(json);

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
            var val = JsonMetadata.GetValue(json, prop);
            if (val == null || val.Value.ValueKind == JsonValueKind.Null)
                continue;

            AddFieldToDocument(target, prop, val.Value, _embeddingGenerator);

            // Accumulate analyzed string fields into _content_
            if (prop.IsAnalyzed && prop.ClrType == typeof(string))
            {
                var s = val.Value.GetString();
                if (!string.IsNullOrEmpty(s))
                {
                    if (contentBuilder.Length > 0) contentBuilder.Append(' ');
                    contentBuilder.Append(s);
                }
            }
        }

        // _content_ composite field
        if (contentBuilder.Length > 0)
        {
            var contentText = contentBuilder.ToString();
            target.Add(new TextField(LottaDB.CONTENT_FIELD, contentText, Field.Store.NO));

            // Generate vector embedding for _content_ if embedding generator is available
            if (_embeddingGenerator != null)
            {
                var result = _embeddingGenerator.GenerateAsync(new[] { contentText })
                    .ConfigureAwait(false).GetAwaiter().GetResult();
                if (result != null && result.Count > 0)
                {
                    var vectorFieldName = LottaDB.CONTENT_FIELD + "_vector";
                    target.RemoveFields(vectorFieldName);
                    var floats = result[0].Vector.Span;
                    var bytes = new byte[floats.Length * sizeof(float)];
                    System.Buffer.BlockCopy(floats.ToArray(), 0, bytes, 0, bytes.Length);
                    target.Add(new BinaryDocValuesField(vectorFieldName, new Lucene.Net.Util.BytesRef(bytes)));
                }
            }
        }
    }

    public void ToObject(Document source, object target)
    {
        // Not needed — we read _object_ directly
    }

    public void PrepareSearchSettings(IQueryExecutionContext context)
    {
        // No special settings needed
    }

    private static void AddFieldToDocument(Document target, IndexedJsonProperty prop, JsonElement val,
        IEmbeddingGenerator<string, Embedding<float>>? embeddingGenerator)
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
            target.Add(new StringField(prop.Name, val.GetBoolean().ToString().ToLowerInvariant(), Field.Store.NO));
        }
        else // string (and fallback)
        {
            var s = val.GetString() ?? val.GetRawText();
            if (prop.IsAnalyzed)
                target.Add(new TextField(prop.Name, s, Field.Store.NO));
            else
                target.Add(new StringField(prop.Name, s, Field.Store.NO));

            // Generate vector embedding for vector-enabled string fields
            if (prop.IsVectorField && embeddingGenerator != null && s != null)
            {
                var result = embeddingGenerator.GenerateAsync(new[] { s })
                    .ConfigureAwait(false).GetAwaiter().GetResult();
                if (result != null && result.Count > 0)
                {
                    var vectorFieldName = prop.Name + "_vector";
                    target.RemoveFields(vectorFieldName);
                    var floats = result[0].Vector.Span;
                    var bytes = new byte[floats.Length * sizeof(float)];
                    System.Buffer.BlockCopy(floats.ToArray(), 0, bytes, 0, bytes.Length);
                    target.Add(new BinaryDocValuesField(vectorFieldName, new Lucene.Net.Util.BytesRef(bytes)));
                }
            }
        }
    }
}

/// <summary>
/// Minimal IFieldMappingInfo for the _content_ composite field.
/// Used by query parser when no specific field is targeted.
/// </summary>
internal class JsonContentFieldMapper : IFieldMappingInfo
{
    private readonly Version _version;
    private readonly Analyzer _analyzer;

    public JsonContentFieldMapper(Version version, Analyzer analyzer)
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
