using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Linq.Search;
using Lucene.Net.Search;

namespace Lotta.Internal;

internal sealed class LottaDocumentKeyFieldMapper<T> : IFieldMapper<T>, IDocumentFieldConverter
{
    private readonly IFieldMapper<T> _inner;
    private readonly IDocumentFieldConverter _converter;

    public LottaDocumentKeyFieldMapper(IFieldMapper<T> inner)
    {
        _inner = inner;
        _converter = inner as IDocumentFieldConverter
            ?? throw new InvalidOperationException($"Key field mapper {inner.GetType().Name} must implement {nameof(IDocumentFieldConverter)}.");
    }

    public void CopyFromDocument(Document source, IQueryExecutionContext context, T target)
        => _inner.CopyFromDocument(source, context, target);

    public void CopyToDocument(T source, Document target)
    {
        _inner.CopyToDocument(source, target);

        var value = ConvertToQueryExpression(GetPropertyValue(source));
        target.Add(new StringField(LottaDB.KEY_FIELD, value, Field.Store.YES));
    }

    public object GetPropertyValue(T source) => _inner.GetPropertyValue(source);

    public object GetFieldValue(Document document) => _converter.GetFieldValue(document);

    public string FieldName => _inner.FieldName;

    public string PropertyName => _inner.PropertyName;

    public string ConvertToQueryExpression(object value) => _inner.ConvertToQueryExpression(value);

    public string EscapeSpecialCharacters(string str) => _inner.EscapeSpecialCharacters(str);

    public Query CreateQuery(string pattern)
    {
        var queryParser = new Lucene.Net.QueryParsers.Classic.QueryParser(
            Lucene.Net.Util.LuceneVersion.LUCENE_48,
            LottaDB.KEY_FIELD,
            new Lucene.Net.Analysis.Core.KeywordAnalyzer())
        {
            AllowLeadingWildcard = true,
            LowercaseExpandedTerms = false,
        };
        return queryParser.Parse(pattern);
    }

    public Query CreateRangeQuery(object lowerBound, object upperBound, RangeType lowerRange, RangeType upperRange)
        => _inner.CreateRangeQuery(lowerBound, upperBound, lowerRange, upperRange);

    public SortField CreateSortField(bool reverse) => _inner.CreateSortField(reverse);

    public Analyzer Analyzer => _inner.Analyzer;

    public IndexMode IndexMode => _inner.IndexMode;
}
