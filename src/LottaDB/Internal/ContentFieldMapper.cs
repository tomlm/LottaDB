using System.Collections;
using System.Reflection;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Linq.Search;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

/// <summary>
/// Combines all analyzed string queryable properties into the <c>_content_</c> field
/// used as the default field for free-text <see cref="LottaDB.Search{T}(string?)"/> queries.
/// Not stored — this field exists only for search.
/// </summary>
internal class ContentFieldMapper<T> : IFieldMapper<T>
{
    private readonly Version _version;
    private readonly Analyzer _analyzer;
    private readonly PropertyInfo[] _properties;

    public ContentFieldMapper(Version version, Analyzer analyzer, IEnumerable<PropertyInfo> properties)
    {
        _version = version;
        _analyzer = analyzer;
        _properties = properties.ToArray();
    }

    public string FieldName => LottaDB.CONTENT_FIELD;

    public string PropertyName => FieldName;

    public Analyzer Analyzer => _analyzer;

    public IndexMode IndexMode => IndexMode.Analyzed;

    public void CopyFromDocument(Document source, IQueryExecutionContext context, T target)
    {
    }

    public void CopyToDocument(T source, Document target)
    {
        target.RemoveFields(FieldName);
        var content = BuildContent(source);
        if (content.Length > 0)
            target.Add(new TextField(FieldName, content, Field.Store.NO));
    }

    public object GetPropertyValue(T source) => BuildContent(source);

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
        => throw new NotSupportedException();

    public SortField CreateSortField(bool reverse) => throw new NotSupportedException();

    private string BuildContent(T source)
    {
        if (source == null || _properties.Length == 0) return string.Empty;
        var sb = new StringBuilder();
        foreach (var prop in _properties)
        {
            var val = prop.GetValue(source);
            if (val == null) continue;
            if (val is string s)
            {
                if (s.Length == 0) continue;
                if (sb.Length > 0) sb.Append(' ');
                sb.Append(s);
            }
            else if (val is IEnumerable items)
            {
                foreach (var item in items)
                {
                    if (item == null) continue;
                    if (sb.Length > 0) sb.Append(' ');
                    sb.Append(item);
                }
            }
        }
        return sb.ToString();
    }
}
