using System.Linq.Expressions;

namespace Lotta;

public class StorageConfiguration<T> : IStorageConfiguration<T> where T : class, new()
{
    internal LambdaExpression? KeyExpression { get; private set; }
    internal KeyMode? KeyModeValue { get; private set; }
    internal List<QueryablePropertyConfig> QueryableProperties { get; } = new();
    internal List<LambdaExpression> TagProperties { get; } = new();
    internal List<FieldPropertyConfig> FieldProperties { get; } = new();
    internal List<LambdaExpression> IgnoredProperties { get; } = new();
    internal LambdaExpression? DefaultSearchExpression { get; private set; }

    public IStorageConfiguration<T> SetKey(Expression<Func<T, string>> resolver)
    {
        KeyExpression = resolver;
        return this;
    }

    public IStorageConfiguration<T> SetKey(KeyMode strategy)
    {
        KeyModeValue = strategy;
        return this;
    }

    public IQueryableConfiguration AddQueryable<TProp>(Expression<Func<T, TProp>> property)
    {
        var config = new QueryablePropertyConfig(property);
        QueryableProperties.Add(config);
        return config;
    }

    public IStorageConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property)
    {
        TagProperties.Add(property);
        return this;
    }

    public IFieldConfiguration AddField<TProp>(Expression<Func<T, TProp>> property)
    {
        var config = new FieldPropertyConfig(property);
        FieldProperties.Add(config);
        return config;
    }

    public IStorageConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property)
    {
        IgnoredProperties.Add(property);
        return this;
    }

    public IStorageConfiguration<T> DefaultSearch<TProp>(Expression<Func<T, TProp>> property)
    {
        DefaultSearchExpression = property;
        return this;
    }
}

internal class QueryablePropertyConfig : IQueryableConfiguration
{
    internal LambdaExpression Expression { get; }
    internal QueryableMode Mode { get; private set; } = QueryableMode.Auto;
    internal bool IsVectorField { get; private set; }

    public QueryablePropertyConfig(LambdaExpression expression)
    {
        Expression = expression;
    }

    public IQueryableConfiguration Analyzed() { Mode = QueryableMode.Analyzed; return this; }
    public IQueryableConfiguration NotAnalyzed() { Mode = QueryableMode.NotAnalyzed; return this; }
    public IQueryableConfiguration Vector() { IsVectorField = true; return this; }
}

internal class FieldPropertyConfig : IFieldConfiguration
{
    internal LambdaExpression Expression { get; }
    internal bool IsNotAnalyzed { get; private set; }
    internal bool IsVectorField { get; private set; }

    public FieldPropertyConfig(LambdaExpression expression)
    {
        Expression = expression;
    }

    public IFieldConfiguration Analyzed() { IsNotAnalyzed = false; return this; }
    public IFieldConfiguration NotAnalyzed() { IsNotAnalyzed = true; return this; }
    public IFieldConfiguration Vector() { IsVectorField = true; return this; }
}
