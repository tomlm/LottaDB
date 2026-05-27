using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Lotta.Internal;

/// <summary>
/// Translates <c>Expression&lt;Func&lt;JsonExpression, bool&gt;&gt;</c> into an OData filter string
/// for Azure Table Storage queries.
/// </summary>
internal static class JsonExpressionODataVisitor
{
    public static string Translate(Expression<Func<JsonExpression, bool>> expression)
    {
        return Visit(expression.Body);
    }

    private static string Visit(Expression node)
    {
        return node switch
        {
            BinaryExpression binary => VisitBinary(binary),
            UnaryExpression { NodeType: ExpressionType.Not } unary => $"not ({Visit(unary.Operand)})",
            MethodCallExpression call => VisitMethodCall(call),
            _ => throw new NotSupportedException($"Unsupported expression type: {node.NodeType}")
        };
    }

    private static string VisitBinary(BinaryExpression node)
    {
        if (node.NodeType == ExpressionType.AndAlso)
            return $"({Visit(node.Left)}) and ({Visit(node.Right)})";

        if (node.NodeType == ExpressionType.OrElse)
            return $"({Visit(node.Left)}) or ({Visit(node.Right)})";

        var (fieldName, value) = ExtractFieldAndValue(node);
        var op = node.NodeType switch
        {
            ExpressionType.Equal => "eq",
            ExpressionType.NotEqual => "ne",
            ExpressionType.GreaterThan => "gt",
            ExpressionType.GreaterThanOrEqual => "ge",
            ExpressionType.LessThan => "lt",
            ExpressionType.LessThanOrEqual => "le",
            _ => throw new NotSupportedException($"Unsupported comparison: {node.NodeType}")
        };

        return $"{fieldName} {op} {FormatODataValue(value)}";
    }

    private static string VisitMethodCall(MethodCallExpression node)
    {
        // String methods not directly supported in OData Table Storage filtering
        throw new NotSupportedException(
            $"Method '{node.Method.Name}' is not supported in OData Table Storage filters. " +
            "Use Lucene Search for full-text queries.");
    }

    private static (string fieldName, object? value) ExtractFieldAndValue(BinaryExpression node)
    {
        var leftField = ExtractFieldName(node.Left);
        if (leftField != null)
            return (leftField, GetConstantValue(node.Right));

        var rightField = ExtractFieldName(node.Right);
        if (rightField != null)
            return (rightField, GetConstantValue(node.Left));

        throw new NotSupportedException("Could not extract field name and value from expression.");
    }

    private static string? ExtractFieldName(Expression node)
    {
        // j["fieldName"] → get_Item("fieldName")
        if (node is MethodCallExpression { Method.Name: "get_Item" } indexer
            && indexer.Arguments.Count == 1)
        {
            var name = GetConstantValue(indexer.Arguments[0])?.ToString();
            if (name != null && !name.All(c => char.IsLetterOrDigit(c) || c == '_'))
                throw new ArgumentException($"Invalid field name '{name}'. Field names must be alphanumeric or underscore.");
            return name;
        }

        // j.GetSchema() → "Schema"
        if (node is MethodCallExpression { Method.Name: "GetSchema" })
            return StorageFields.Schema;

        // j.GetKey() → "RowKey" (OData uses RowKey for Table Storage)
        if (node is MethodCallExpression { Method.Name: "GetKey" })
            return "RowKey";

        // Unwrap Convert
        if (node is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return ExtractFieldName(convert.Operand);

        return null;
    }

    private static object? GetConstantValue(Expression node)
    {
        if (node is ConstantExpression constant)
            return constant.Value;

        if (node is MemberExpression member && member.Expression is ConstantExpression capture)
        {
            return member.Member switch
            {
                FieldInfo fi => fi.GetValue(capture.Value),
                PropertyInfo pi => pi.GetValue(capture.Value),
                _ => null
            };
        }

        if (node is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return GetConstantValue(convert.Operand);

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

    private static string FormatODataValue(object? value)
    {
        return value switch
        {
            null => "null",
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b.ToString().ToLowerInvariant(),
            int or long or double or float => value.ToString()!,
            DateTime dt => $"datetime'{dt:O}'",
            DateTimeOffset dto => $"datetime'{dto:O}'",
            Guid g => $"guid'{g}'",
            _ => $"'{value}'"
        };
    }
}
