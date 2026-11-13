using Lucene.Net.Analysis.Core;
using Lucene.Net.Documents;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.QueryParsers.Classic;
using Lucene.Net.Search;
using Lucene.Net.Util;

namespace Lotta.Internal;

/// <summary>
/// Key field mapper for a plain string key. Behaves like
/// <see cref="LottaDocumentKeyFieldMapper{T}"/> (also writes the shared
/// <see cref="StorageFields.Key"/> field and queries the key through it), but stays a
/// <see cref="ReflectionFieldMapper{T}"/>. Lucene.Net.Linq only runs a join inside
/// Lucene when both key mappers are that type, so this is what lets
/// <c>join b in db.Search&lt;B&gt;() on a.BId equals b.Key</c> avoid loading both sides.
/// </summary>
internal sealed class LottaKeyFieldMapper<T> : ReflectionFieldMapper<T>
{
    public LottaKeyFieldMapper(ReflectionFieldMapper<T> inner)
        : base(inner.PropertyInfo, inner.Store, inner.IndexMode, inner.TermVector, inner.Converter, inner.FieldName,
               inner.DefaultParseOperator, inner.CaseSensitive, inner.Analyzer, inner.Boost, inner.NativeSort,
               inner.DocValues, inner.Facet)
    {
    }

    public override void CopyToDocument(T source, Document target)
    {
        base.CopyToDocument(source, target);

        var value = ConvertToQueryExpression(GetPropertyValue(source));
        target.Add(new StringField(StorageFields.Key, value, Field.Store.YES));
    }

    public override Query CreateQuery(string pattern)
    {
        var queryParser = new QueryParser(LuceneVersion.LUCENE_48, StorageFields.Key, new KeywordAnalyzer())
        {
            AllowLeadingWildcard = true,
            LowercaseExpandedTerms = false,
        };
        return queryParser.Parse(pattern);
    }
}
