using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Linq.Search;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// IFieldMappingInfo implementation for a schema-defined property.
/// Used by FieldMappingQueryParser to resolve field names in Lucene queries.
/// </summary>
internal class DynamicFieldMappingInfo : IFieldMappingInfo
{
    private readonly Version _version;
    private readonly Analyzer _analyzer;
    private readonly bool _isNumeric;
    private readonly Type _clrType;

    public DynamicFieldMappingInfo(string fieldName, Type clrType, bool isAnalyzed, Version version, Analyzer? analyzer = null)
    {
        FieldName = fieldName;
        PropertyName = fieldName;
        _version = version;
        _clrType = clrType;
        _isNumeric = clrType == typeof(int) || clrType == typeof(long)
                  || clrType == typeof(double) || clrType == typeof(float);

        _analyzer = isAnalyzed
            ? analyzer ?? new StandardAnalyzer(version)
            : new Lucene.Net.Analysis.Core.KeywordAnalyzer();
    }

    public string FieldName { get; }
    public string PropertyName { get; }
    public Analyzer Analyzer => _analyzer;

    public string ConvertToQueryExpression(object value)
    {
        return value?.ToString() ?? string.Empty;
    }

    public string EscapeSpecialCharacters(string str)
        => QueryParserBase.Escape(str ?? string.Empty);

    public Query CreateQuery(string pattern)
    {
        if (_isNumeric)
        {
            // For numeric fields queried with exact value, create a NumericRange with equal bounds
            if (int.TryParse(pattern, out var i))
                return NumericRangeQuery.NewInt32Range(FieldName, i, i, true, true);
            if (double.TryParse(pattern, out var d))
                return NumericRangeQuery.NewDoubleRange(FieldName, d, d, true, true);
        }

        var parser = new QueryParser(_version, FieldName, _analyzer)
        {
            AllowLeadingWildcard = true,
            LowercaseExpandedTerms = true,
        };
        return parser.Parse(pattern);
    }

    public Query CreateRangeQuery(object lowerBound, object upperBound, RangeType lowerRange, RangeType upperRange)
    {
        if (_isNumeric)
        {
            var minInclusive = lowerRange == RangeType.Inclusive;
            var maxInclusive = upperRange == RangeType.Inclusive;

            if (_clrType == typeof(int))
            {
                var lower = lowerBound != null ? ParseInt(lowerBound) : int.MinValue;
                var upper = upperBound != null ? ParseInt(upperBound) : int.MaxValue;
                return NumericRangeQuery.NewInt32Range(FieldName, lower, upper, minInclusive, maxInclusive);
            }
            else
            {
                var lower = lowerBound != null ? ParseDouble(lowerBound) : double.MinValue;
                var upper = upperBound != null ? ParseDouble(upperBound) : double.MaxValue;
                return NumericRangeQuery.NewDoubleRange(FieldName, lower, upper, minInclusive, maxInclusive);
            }
        }

        return TermRangeQuery.NewStringRange(FieldName,
            lowerBound?.ToString(), upperBound?.ToString(),
            lowerRange == RangeType.Inclusive, upperRange == RangeType.Inclusive);
    }

    public SortField CreateSortField(bool reverse)
    {
        if (_clrType == typeof(int)) return new SortField(FieldName, SortFieldType.INT32, reverse);
        if (_clrType == typeof(double)) return new SortField(FieldName, SortFieldType.DOUBLE, reverse);
        return new SortField(FieldName, SortFieldType.STRING, reverse);
    }

    private static int ParseInt(object value)
    {
        if (value is int i) return i;
        return int.TryParse(value?.ToString(), out var parsed) ? parsed : 0;
    }

    private static double ParseDouble(object value)
    {
        if (value is double d) return d;
        return double.TryParse(value?.ToString(), out var parsed) ? parsed : 0;
    }
}
