using System.Text.Json;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Linq.Search;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta.Internal;

internal class JsonFieldMapper<T> : IFieldMapper<T>
{
    private readonly Version _version;
    private readonly Analyzer _analyzer;

    public JsonFieldMapper(Version version)
    {
        _version = version;
        _analyzer = new StandardAnalyzer(version);
    }

    public string FieldName => LottaDB.JSON_FIELD;

    public string PropertyName => FieldName;

    public Analyzer Analyzer => _analyzer;

    public IndexMode IndexMode => IndexMode.Analyzed;

    public void CopyFromDocument(Document source, IQueryExecutionContext context, T target)
    {
    }

    public void CopyToDocument(T source, Document target)
    {
        target.RemoveFields(FieldName);
        target.Add(new TextField(FieldName, Serialize(source), Field.Store.YES));
    }

    public object GetPropertyValue(T source) => Serialize(source);

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

    public SortField CreateSortField(bool reverse)
        => throw new NotSupportedException();

    private static string Serialize(T source) => JsonSerializer.Serialize(source, source?.GetType() ?? typeof(T));
}
