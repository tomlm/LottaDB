using System.Linq.Expressions;
using System.Reflection;

namespace LottaDB.Internal;

/// <summary>
/// Parses a CreateView LINQ expression to extract dependencies, join keys, and the compiled projection.
/// </summary>
internal class ViewExpressionParser
{
    public ViewDefinition? Parse(object expressionObj, Type viewType)
    {
        if (expressionObj is not LambdaExpression lambda)
            return null;

        var visitor = new ViewExpressionVisitor();
        visitor.Visit(lambda.Body);

        if (visitor.SourceTypes.Count == 0)
            return null;

        return new ViewDefinition
        {
            ViewType = viewType,
            DependsOn = visitor.SourceTypes.ToHashSet(),
            JoinKeys = visitor.JoinKeys,
            CompiledQuery = CompileQuery(lambda),
        };
    }

    private Func<ILottaDB, IEnumerable<object>> CompileQuery(LambdaExpression lambda)
    {
        // Compile the expression: Func<ILottaDB, IQueryable<TView>> → execute and return as IEnumerable<object>
        var compiled = lambda.Compile();
        return db =>
        {
            var queryable = compiled.DynamicInvoke(db);
            if (queryable is IEnumerable<object> enumerable)
                return enumerable;
            if (queryable is IQueryable queryableObj)
                return queryableObj.Cast<object>().ToList();
            return Enumerable.Empty<object>();
        };
    }
}

internal class ViewExpressionVisitor : ExpressionVisitor
{
    public List<Type> SourceTypes { get; } = new();
    public List<JoinKeyInfo> JoinKeys { get; } = new();

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Detect db.Search<T>() calls to extract source types
        if (node.Method.Name == "Search" && node.Method.IsGenericMethod)
        {
            var sourceType = node.Method.GetGenericArguments()[0];
            if (!SourceTypes.Contains(sourceType))
                SourceTypes.Add(sourceType);
        }

        // Detect Join calls to extract join keys
        if (node.Method.Name == "Join" && node.Arguments.Count >= 5)
        {
            // Standard LINQ Join(outer, inner, outerKeySelector, innerKeySelector, resultSelector)
            ExtractJoinKeys(node);
        }

        return base.VisitMethodCall(node);
    }

    private void ExtractJoinKeys(MethodCallExpression joinCall)
    {
        // Arguments: [0]=outer, [1]=inner, [2]=outerKeySelector, [3]=innerKeySelector, [4]=resultSelector
        if (joinCall.Arguments.Count < 5) return;

        var outerKeyLambda = UnwrapLambda(joinCall.Arguments[2]);
        var innerKeyLambda = UnwrapLambda(joinCall.Arguments[3]);

        if (outerKeyLambda != null && innerKeyLambda != null)
        {
            // Extract property names from key selectors
            var outerProps = ExtractPropertyNames(outerKeyLambda.Body);
            var innerProps = ExtractPropertyNames(innerKeyLambda.Body);

            // Determine which source types are involved
            var outerType = GetSourceTypeFromExpression(joinCall.Arguments[0]);
            var innerType = GetSourceTypeFromExpression(joinCall.Arguments[1]);

            if (outerType != null && innerType != null)
            {
                JoinKeys.Add(new JoinKeyInfo
                {
                    OuterType = outerType,
                    InnerType = innerType,
                    OuterKeyProperties = outerProps,
                    InnerKeyProperties = innerProps,
                });
            }
        }
    }

    private static LambdaExpression? UnwrapLambda(Expression expr)
    {
        if (expr is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
            return lambda;
        if (expr is LambdaExpression directLambda)
            return directLambda;
        return null;
    }

    private static List<string> ExtractPropertyNames(Expression expr)
    {
        var names = new List<string>();

        if (expr is NewExpression newExpr)
        {
            // Anonymous type: new { a.Prop1, b.Prop2 }
            foreach (var arg in newExpr.Arguments)
            {
                if (arg is MemberExpression member)
                    names.Add(member.Member.Name);
            }
        }
        else if (expr is MemberExpression member)
        {
            names.Add(member.Member.Name);
        }

        return names;
    }

    private static Type? GetSourceTypeFromExpression(Expression expr)
    {
        // Walk through to find the Search<T>() call
        if (expr is MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "Search" && methodCall.Method.IsGenericMethod)
                return methodCall.Method.GetGenericArguments()[0];
            // Could be chained: Search<T>().Where(...)
            foreach (var arg in methodCall.Arguments)
            {
                var result = GetSourceTypeFromExpression(arg);
                if (result != null) return result;
            }
        }
        return null;
    }
}

internal class JoinKeyInfo
{
    public required Type OuterType { get; init; }
    public required Type InnerType { get; init; }
    public required List<string> OuterKeyProperties { get; init; }
    public required List<string> InnerKeyProperties { get; init; }
}

internal class ViewDefinition
{
    public required Type ViewType { get; init; }
    public required HashSet<Type> DependsOn { get; init; }
    public required List<JoinKeyInfo> JoinKeys { get; init; }
    public required Func<ILottaDB, IEnumerable<object>> CompiledQuery { get; init; }

    public IEnumerable<object> Execute(ILottaDB db)
    {
        return CompiledQuery(db);
    }

    public List<(string pk, string rk)> FindAffectedViewKeys(object entity, Type triggerType, LottaDBInstance db)
    {
        // Execute the full query and find views that match
        // This is a brute-force approach — execute the query, collect all results,
        // then return their keys. For the per-tenant scaling model this is fine.
        var results = new List<(string pk, string rk)>();

        try
        {
            var allViews = Execute(db);
            foreach (var view in allViews)
            {
                // Extract PK/RK from the view object
                if (db._metadata.TryGetValue(ViewType, out var meta))
                {
                    var pk = meta.GetPartitionKey(view);
                    var rk = meta.GetRowKey(view);
                    results.Add((pk, rk));
                }
            }
        }
        catch
        {
            // If execution fails (e.g., missing data), return empty
        }

        return results;
    }
}
