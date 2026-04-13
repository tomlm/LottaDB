using System.Linq.Expressions;

namespace Lotta;

public class StoreConfiguration<T> : IStoreConfiguration<T> where T : class, new()
{
    internal LambdaExpression? KeyExpression { get; private set; }
    internal KeyStrategy? KeyStrategyValue { get; private set; }
    internal List<LambdaExpression> Tags { get; } = new();
    internal List<LambdaExpression> IgnoredProperties { get; } = new();

    public IStoreConfiguration<T> SetKey(Expression<Func<T, string>> resolver)
    {
        KeyExpression = resolver;
        return this;
    }

    public IStoreConfiguration<T> SetKey(KeyStrategy strategy)
    {
        KeyStrategyValue = strategy;
        return this;
    }

    public IStoreConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property)
    {
        Tags.Add(property);
        return this;
    }

    public IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property)
    {
        return new IndexPropertyConfiguration();
    }

    public IStoreConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property)
    {
        IgnoredProperties.Add(property);
        return this;
    }
}

internal class IndexPropertyConfiguration : IIndexPropertyConfiguration
{
    public IIndexPropertyConfiguration AsKey() => this;
    public IIndexPropertyConfiguration NotAnalyzed() => this;
    public IIndexPropertyConfiguration AnalyzedWith<TAnalyzer>() where TAnalyzer : class => this;
    public IIndexPropertyConfiguration AsNumeric() => this;
    public IIndexPropertyConfiguration WithDocValues() => this;
}
