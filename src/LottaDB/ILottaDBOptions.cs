using System.Linq.Expressions;

namespace LottaDB;

public interface ILottaDBOptions
{
    ILottaDBOptions UseAzureTables(string connectionString);
    ILottaDBOptions UseInMemoryTables();
    ILottaDBOptions UseLuceneDirectory(IDirectoryProvider provider);

    ILottaDBOptions Store<T>(Action<IStoreConfiguration<T>>? configure = null) where T : class;

    ILottaDBOptions CreateView<TView>(Expression<Func<ILottaDB, IQueryable<TView>>> viewExpression) where TView : class;

    ILottaDBOptions AddBuilder<TTrigger, TDerived, TBuilder>()
        where TTrigger : class
        where TDerived : class
        where TBuilder : class, IBuilder<TTrigger, TDerived>;

    ILottaDBOptions Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class;
}

public interface IStoreConfiguration<T> where T : class
{
    IStoreConfiguration<T> SetTableName(string tableName);
    IStoreConfiguration<T> SetPartitionKey(Expression<Func<T, string>> resolver);
    IStoreConfiguration<T> SetRowKey(Expression<Func<T, string>> resolver);
    IStoreConfiguration<T> SetRowKey(RowKeyStrategy strategy);
    IStoreConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);
    IStoreConfiguration<T> AddComputedTag<TProp>(string name, Func<T, TProp> compute);

    // Lucene index configuration
    IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property);
    IStoreConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);
}

public interface IIndexPropertyConfiguration
{
    IIndexPropertyConfiguration AsKey();
    IIndexPropertyConfiguration NotAnalyzed();
    IIndexPropertyConfiguration AnalyzedWith<TAnalyzer>() where TAnalyzer : class;
    IIndexPropertyConfiguration AsNumeric();
    IIndexPropertyConfiguration WithDocValues();
}
