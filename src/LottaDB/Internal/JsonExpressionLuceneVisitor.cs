using System.Linq.Expressions;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;

namespace Lotta.Internal;

/// <summary>
/// Translates <c>Expression&lt;Func&lt;JsonExpression, bool&gt;&gt;</c> into a Lucene <see cref="Query"/>.
/// Handles indexer access (<c>j["field"]</c>), metadata methods (<c>j.GetSchema()</c>),
/// and logical operators (&amp;&amp;, ||, !).
/// </summary>
internal static class JsonExpressionLuceneVisitor
{
    public static Query Translate(Expression<Func<JsonExpression, bool>> expression, Analyzer analyzer)
    {
        return Visit(expression.Body, analyzer);
    }

    private static Query Visit(Expression node, Analyzer analyzer)
    {
        return node switch
        {
            BinaryExpression binary => VisitBinary(binary, analyzer),
            UnaryExpression { NodeType: ExpressionType.Not } unary => VisitNot(unary, analyzer),
            MethodCallExpression call => VisitMethodCall(call, analyzer),
            _ => throw new NotSupportedException($"Unsupported expression type: {node.NodeType}")
        };
    }

    private static Query VisitBinary(BinaryExpression node, Analyzer analyzer)
    {
        // Logical AND / OR
        if (node.NodeType == ExpressionType.AndAlso)
        {
            var bq = new BooleanQuery();
            bq.Add(Visit(node.Left, analyzer), Occur.MUST);
            bq.Add(Visit(node.Right, analyzer), Occur.MUST);
            return bq;
        }

        if (node.NodeType == ExpressionType.OrElse)
        {
            var bq = new BooleanQuery();
            bq.Add(Visit(node.Left, analyzer), Occur.SHOULD);
            bq.Add(Visit(node.Right, analyzer), Occur.SHOULD);
            bq.MinimumNumberShouldMatch = 1;
            return bq;
        }

        // Comparison: field op value
        (string fieldName, object? value) extracted;
        try
        {
            extracted = ExtractFieldAndValue(node);
        }
        catch (NotSupportedException)
        {
            throw new NotSupportedException(
                $"Could not extract field/value from {node.NodeType}: Left={node.Left.NodeType}({node.Left.GetType().Name}), Right={node.Right.NodeType}({node.Right.GetType().Name}), Method={node.Method}");
        }
        var (fieldName, value) = extracted;

        return node.NodeType switch
        {
            ExpressionType.Equal => CreateTermQuery(fieldName, value, analyzer),
            ExpressionType.NotEqual => CreateNotQuery(CreateTermQuery(fieldName, value, analyzer)),
            ExpressionType.GreaterThan => CreateRangeQuery(fieldName, value, lowerInclusive: false, upper: null),
            ExpressionType.GreaterThanOrEqual => CreateRangeQuery(fieldName, value, lowerInclusive: true, upper: null),
            ExpressionType.LessThan => CreateRangeQuery(fieldName, null, lowerInclusive: false, upper: value, upperInclusive: false),
            ExpressionType.LessThanOrEqual => CreateRangeQuery(fieldName, null, lowerInclusive: false, upper: value, upperInclusive: true),
            _ => throw new NotSupportedException($"Unsupported comparison: {node.NodeType}")
        };
    }

    private static Query VisitNot(UnaryExpression node, Analyzer analyzer)
    {
        return CreateNotQuery(Visit(node.Operand, analyzer));
    }

    private static Query VisitMethodCall(MethodCallExpression node, Analyzer analyzer)
    {
        // Handle string methods: j["field"].Contains("x"), .StartsWith("x"), .EndsWith("x")
        if (node.Object != null && node.Arguments.Count == 1)
        {
            var fieldName = ExtractFieldName(node.Object);
            if (fieldName != null)
            {
                var value = GetConstantValue(node.Arguments[0])?.ToString() ?? "";
                return node.Method.Name switch
                {
                    "Contains" => CreateWildcardQuery(fieldName, $"*{value}*"),
                    "StartsWith" => CreateWildcardQuery(fieldName, $"{value}*"),
                    "EndsWith" => CreateWildcardQuery(fieldName, $"*{value}"),
                    _ => throw new NotSupportedException($"Unsupported method: {node.Method.Name}")
                };
            }
        }

        throw new NotSupportedException($"Unsupported method call: {node.Method.Name}");
    }

    private static (string fieldName, object? value) ExtractFieldAndValue(BinaryExpression node)
    {
        // Try left = field, right = value
        var leftField = ExtractFieldName(node.Left);
        if (leftField != null)
            return (leftField, GetConstantValue(node.Right));

        // Try right = field, left = value (reversed comparison)
        var rightField = ExtractFieldName(node.Right);
        if (rightField != null)
            return (rightField, GetConstantValue(node.Left));

        throw new NotSupportedException("Could not extract field name and value from expression.");
    }

    private static string? ExtractFieldName(Expression node)
    {
        // j["fieldName"] → indexer call → get_Item("fieldName")
        if (node is MethodCallExpression { Method.Name: "get_Item" } indexer
            && indexer.Arguments.Count == 1)
        {
            return GetConstantValue(indexer.Arguments[0])?.ToString();
        }

        // j.GetSchema() → "Schema"
        if (node is MethodCallExpression { Method.Name: "GetSchema" })
            return StorageFields.Schema;

        // j.GetKey() → "_key_"
        if (node is MethodCallExpression { Method.Name: "GetKey" })
            return StorageFields.Key;

        // j.GetETag() → "_etag_"
        if (node is MethodCallExpression { Method.Name: "GetETag" })
            return StorageFields.ETag;

        // Unwrap Convert nodes (boxing)
        if (node is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return ExtractFieldName(convert.Operand);

        return null;
    }

    private static object? GetConstantValue(Expression node)
    {
        if (node is ConstantExpression constant)
            return constant.Value;

        // Handle captured variables (closures): MemberAccess on a ConstantExpression
        if (node is MemberExpression member && member.Expression is ConstantExpression capture)
        {
            return member.Member switch
            {
                System.Reflection.FieldInfo fi => fi.GetValue(capture.Value),
                System.Reflection.PropertyInfo pi => pi.GetValue(capture.Value),
                _ => null
            };
        }

        // Unwrap Convert
        if (node is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return GetConstantValue(convert.Operand);

        // Try to compile and evaluate
        try
        {
            var lambda = Expression.Lambda(node);
            return lambda.Compile().DynamicInvoke();
        }
        catch
        {
            throw new NotSupportedException($"Cannot evaluate expression: {node}");
        }
    }

    private static Query CreateTermQuery(string fieldName, object? value, Analyzer analyzer)
    {
        if (value == null)
            return new BooleanQuery(); // empty — no match

        if (value is int i)
            return NumericRangeQuery.NewInt64Range(fieldName, (long)i, (long)i, true, true);
        if (value is long l)
            return NumericRangeQuery.NewInt64Range(fieldName, l, l, true, true);
        if (value is double d)
            return NumericRangeQuery.NewDoubleRange(fieldName, d, d, true, true);
        if (value is bool b)
            return new TermQuery(new Term(fieldName, b.ToString().ToLowerInvariant()));

        // String — TermQuery with lowercased value for all fields.
        // AutoQueryable indexes text via TextField (which lowercases with default analyzer),
        // so we match with lowercased terms.
        var str = value.ToString()!;
        if (fieldName == StorageFields.Schema || fieldName == StorageFields.Key || fieldName == StorageFields.ETag)
            return new TermQuery(new Term(fieldName, str));

        // AutoQueryable fields are indexed with KeywordAnalyzer (verbatim), so match exactly
        return new TermQuery(new Term(fieldName, str));
    }

    private static Query CreateNotQuery(Query inner)
    {
        var bq = new BooleanQuery();
        bq.Add(new MatchAllDocsQuery(), Occur.MUST);
        bq.Add(inner, Occur.MUST_NOT);
        return bq;
    }

    private static Query CreateWildcardQuery(string fieldName, string pattern)
    {
        return new WildcardQuery(new Term(fieldName, pattern.ToLowerInvariant()));
    }

    private static Query CreateRangeQuery(string fieldName, object? lower, bool lowerInclusive,
        object? upper, bool upperInclusive = false)
    {
        // Numeric ranges — AutoQueryable stores all integers as Int64 in Lucene
        if (lower is int li || upper is int ui)
            return NumericRangeQuery.NewInt64Range(fieldName,
                lower != null ? Convert.ToInt64(lower) : null,
                upper != null ? Convert.ToInt64(upper) : null,
                lowerInclusive, upperInclusive);
        if (lower is long || upper is long)
            return NumericRangeQuery.NewInt64Range(fieldName,
                lower as long?, upper as long?, lowerInclusive, upperInclusive);
        if (lower is double || upper is double)
            return NumericRangeQuery.NewDoubleRange(fieldName,
                lower as double?, upper as double?, lowerInclusive, upperInclusive);

        // String range (lexicographic)
        return TermRangeQuery.NewStringRange(fieldName,
            lower?.ToString(), upper?.ToString(), lowerInclusive, upperInclusive);
    }
}
