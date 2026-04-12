using System.Linq.Expressions;

namespace LottaDB;

public class StoreConfiguration<T> : IStoreConfiguration<T> where T : class
{
    internal string? TableName { get; private set; }
    internal LambdaExpression? PartitionKeyExpression { get; private set; }
    internal LambdaExpression? RowKeyExpression { get; private set; }
    internal RowKeyStrategy? RowKeyStrategyValue { get; private set; }
    internal List<LambdaExpression> Tags { get; } = new();
    internal List<LambdaExpression> IgnoredProperties { get; } = new();

    public IStoreConfiguration<T> SetTableName(string tableName)
    {
        TableName = tableName;
        return this;
    }

    public IStoreConfiguration<T> SetPartitionKey(Expression<Func<T, string>> resolver)
    {
        PartitionKeyExpression = resolver;
        return this;
    }

    public IStoreConfiguration<T> SetRowKey(Expression<Func<T, string>> resolver)
    {
        RowKeyExpression = resolver;
        return this;
    }

    public IStoreConfiguration<T> SetRowKey(RowKeyStrategy strategy)
    {
        RowKeyStrategyValue = strategy;
        return this;
    }

    public IStoreConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property)
    {
        Tags.Add(property);
        return this;
    }

    public IStoreConfiguration<T> AddComputedTag<TProp>(string name, Func<T, TProp> compute)
    {
        // Store for later use
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
