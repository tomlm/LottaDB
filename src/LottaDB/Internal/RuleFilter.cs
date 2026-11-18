using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// A rule's filter, taken apart once when the rule is made. A filter may use
/// <see cref="Rule.Changed"/>, which only a save can answer (it has the old and the new version in
/// hand), so the filter is kept in two forms: the whole of it for the save, and the rest of it,
/// with <c>Changed</c> taken to be true, for every later check.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
internal sealed class RuleFilter<T> where T : class
{
    /// <summary>The whole filter, <c>Changed</c> included, for the save itself; null when every save matches.</summary>
    public Func<T, bool>? OnSave { get; init; }

    /// <summary>
    /// The filter with <c>Changed</c> taken to be true; null when nothing is left to check. This
    /// is what a queued run checks on the object as it then is, and what a backfill asks the
    /// search index.
    /// </summary>
    public Expression<Func<T, bool>>? RestExpression { get; init; }

    /// <summary><see cref="RestExpression"/>, compiled.</summary>
    public Func<T, bool>? Rest { get; init; }

    /// <summary>The values named in <c>Changed(...)</c>: what an <c>Update</c> rule reads.</summary>
    public Func<T, object?>[] Inputs { get; init; } = [];

    /// <summary>Whether the filter uses <c>Changed(...)</c>.</summary>
    public bool UsesChanged { get; init; }
}

internal static class RuleFilter
{
    private static readonly MethodInfo HasChangedMethod =
        typeof(RuleFilter).GetMethod(nameof(HasChanged), BindingFlags.Static | BindingFlags.Public)!;

    /// <summary>Takes a filter apart; see <see cref="RuleFilter{T}"/>.</summary>
    /// <exception cref="ArgumentException"><c>Changed(...)</c> is used in a way that cannot be evaluated.</exception>
    public static RuleFilter<T> Parse<T>(Expression<Func<T, bool>>? filter) where T : class
    {
        if (filter == null)
            return new RuleFilter<T>();

        var rewriter = new ChangedRewriter<T>(filter.Parameters[0]);
        var body = rewriter.Visit(filter.Body);
        if (rewriter.Inputs.Count == 0)
        {
            // Compiled once, here: every check, on save, before a run and in .Do, uses the delegate.
            var compiled = filter.Compile();
            return new RuleFilter<T> { OnSave = compiled, RestExpression = filter, Rest = compiled };
        }

        var rest = new RestBuilder().Build(filter.Body);
        var restExpression = rest == null ? null : Expression.Lambda<Func<T, bool>>(rest, filter.Parameters);
        return new RuleFilter<T>
        {
            OnSave = Expression.Lambda<Func<T, bool>>(body, filter.Parameters).Compile(),
            RestExpression = restExpression,
            Rest = restExpression?.Compile(),
            Inputs = [.. rewriter.Inputs],
            UsesChanged = true,
        };
    }

    /// <summary>
    /// What <c>Changed(...)</c> becomes in a filter: whether any of the values differs between the
    /// object being saved and the version this save replaces. An object with no earlier version
    /// (new, or looked at outside a save) counts as changed.
    /// </summary>
    public static bool HasChanged<T>(T entity, Func<T, object?>[] values) where T : class
    {
        if (entity.GetPrevious() is not T before)
            return true;
        foreach (var value in values)
        {
            var now = Json(value(entity));
            string was;
            try
            {
                was = Json(value(before));
            }
            catch (Exception)
            {
                return true;   // the value could not even be read from the old version
            }
            if (now != was)
                return true;
        }
        return false;
    }

    /// <summary>Whether every one of the values is the same on both objects.</summary>
    public static bool SameValues(object a, object b, Func<object, object?>[] values)
    {
        foreach (var value in values)
        {
            try
            {
                if (Json(value(a)) != Json(value(b)))
                    return false;
            }
            catch (Exception)
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>A value as JSON, the form objects are stored and compared in.</summary>
    private static string Json(object? value) =>
        value == null ? "null" : JsonSerializer.Serialize(value, value.GetType());

    private static bool IsChanged(MethodCallExpression call) =>
        call.Method.DeclaringType == typeof(Rule) && call.Method.Name == nameof(Rule.Changed);

    /// <summary>Replaces each <c>Changed(a, b)</c> with a call that compares a and b across the save.</summary>
    private sealed class ChangedRewriter<T>(ParameterExpression item) : ExpressionVisitor where T : class
    {
        public List<Func<T, object?>> Inputs { get; } = [];

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (!IsChanged(node))
                return base.VisitMethodCall(node);

            // Changed(a, b) arrives as one argument: new object[] { a, b }.
            var values = node.Arguments[0] is NewArrayExpression { NodeType: ExpressionType.NewArrayInit } array
                ? array.Expressions
                : node.Arguments[0] is MethodCallExpression { Method.Name: nameof(Array.Empty) } ? [] : (IReadOnlyList<Expression>)[node.Arguments[0]];
            if (values.Count == 0)
                throw new ArgumentException("Changed() needs at least one value of the object: Changed(n.Content).");

            var getters = new Func<T, object?>[values.Count];
            for (var n = 0; n < values.Count; n++)
            {
                try
                {
                    getters[n] = Expression.Lambda<Func<T, object?>>(Expression.Convert(values[n], typeof(object)), item).Compile();
                }
                catch (InvalidOperationException e)
                {
                    throw new ArgumentException(
                        $"Changed({values[n]}) cannot be evaluated: give it values of the object the rule is for, such as Changed({item.Name}.Content).", e);
                }
            }
            Inputs.AddRange(getters);
            return Expression.Call(HasChangedMethod.MakeGenericMethod(typeof(T)), item, Expression.Constant(getters));
        }
    }

    /// <summary>
    /// Builds the filter that is left when <c>Changed</c> cannot be asked: each <c>Changed</c> is
    /// replaced by whatever lets the rule through (true, or false under a "not"), and the
    /// constants are folded away. Gives null when nothing is left to check.
    /// </summary>
    private sealed class RestBuilder
    {
        private bool _unknown;

        public Expression? Build(Expression body)
        {
            var rest = Rest(body, negated: false);
            // Changed used as a value (compared, passed on): nothing can be said without it.
            if (_unknown)
                return null;
            return rest is ConstantExpression { Value: true } ? null : rest;
        }

        private Expression Rest(Expression node, bool negated)
        {
            switch (node)
            {
                case MethodCallExpression call when IsChanged(call):
                    return Expression.Constant(!negated);

                case UnaryExpression { NodeType: ExpressionType.Not } not when not.Type == typeof(bool):
                    {
                        var operand = Rest(not.Operand, !negated);
                        return operand is ConstantExpression { Value: bool value } ? Expression.Constant(!value) : Expression.Not(operand);
                    }

                case BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.And } both when both.Type == typeof(bool):
                    {
                        var left = Rest(both.Left, negated);
                        var right = Rest(both.Right, negated);
                        if (left is ConstantExpression { Value: bool l })
                            return l ? right : left;
                        if (right is ConstantExpression { Value: bool r })
                            return r ? left : right;
                        return Expression.AndAlso(left, right);
                    }

                case BinaryExpression { NodeType: ExpressionType.OrElse or ExpressionType.Or } either when either.Type == typeof(bool):
                    {
                        var left = Rest(either.Left, negated);
                        var right = Rest(either.Right, negated);
                        if (left is ConstantExpression { Value: bool l })
                            return l ? left : right;
                        if (right is ConstantExpression { Value: bool r })
                            return r ? right : left;
                        return Expression.OrElse(left, right);
                    }

                default:
                    if (new ChangedFinder().Uses(node))
                        _unknown = true;
                    return node;
            }
        }
    }

    private sealed class ChangedFinder : ExpressionVisitor
    {
        private bool _found;

        public bool Uses(Expression node)
        {
            Visit(node);
            return _found;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (IsChanged(node))
                _found = true;
            return base.VisitMethodCall(node);
        }
    }
}
