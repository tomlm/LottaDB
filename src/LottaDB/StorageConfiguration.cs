using System.Linq.Expressions;

namespace Lotta;

public class StorageConfiguration<T> : IStorageConfiguration<T> where T : class, new()
{
    internal LambdaExpression? KeyExpression { get; private set; }
    internal KeyMode? KeyModeValue { get; private set; }
    internal List<LambdaExpression> Tags { get; } = new();
    internal List<IndexedPropertyConfig> IndexedProperties { get; } = new();
    internal List<LambdaExpression> IgnoredProperties { get; } = new();

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

    public IStorageConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property)
    {
        Tags.Add(property);
        return this;
    }

    public IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property)
    {
        var config = new IndexedPropertyConfig(property);
        IndexedProperties.Add(config);
        return config;
    }

    public IStorageConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property)
    {
        IgnoredProperties.Add(property);
        return this;
    }
}

internal class IndexedPropertyConfig : IIndexPropertyConfiguration
{
    internal LambdaExpression Expression { get; }
    internal bool IsKey { get; private set; }
    internal bool IsNotAnalyzed { get; private set; }
    internal bool IsNumeric { get; private set; }
    internal bool HasDocValues { get; private set; }
    internal Type? AnalyzerType { get; private set; }

    public IndexedPropertyConfig(LambdaExpression expression)
    {
        Expression = expression;
    }

    public IIndexPropertyConfiguration AsKey() { IsKey = true; return this; }
    public IIndexPropertyConfiguration NotAnalyzed() { IsNotAnalyzed = true; return this; }
    public IIndexPropertyConfiguration AnalyzedWith<TAnalyzer>() where TAnalyzer : class { AnalyzerType = typeof(TAnalyzer); return this; }
    public IIndexPropertyConfiguration AsNumeric() { IsNumeric = true; return this; }
    public IIndexPropertyConfiguration WithDocValues() { HasDocValues = true; return this; }
}
