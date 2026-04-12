using System.Linq.Expressions;

namespace LottaDB;

/// <summary>
/// Configuration options for a LottaDB database instance.
/// </summary>
public interface ILottaDBOptions
{
    /// <summary>Register an object type. Config from [Key]/[Tag] attributes, or fluent override.</summary>
    ILottaDBOptions Store<T>(Action<IStoreConfiguration<T>>? configure = null) where T : class, new();

    /// <summary>Declare a materialized view as a LINQ join. Use db.Query&lt;T&gt;() in the expression.</summary>
    ILottaDBOptions CreateView<TView>(Expression<Func<ILottaDB, IQueryable<TView>>> viewExpression) where TView : class, new();

    /// <summary>Register an explicit builder for custom derivation logic.</summary>
    ILottaDBOptions AddBuilder<TTrigger, TDerived, TBuilder>()
        where TTrigger : class, new()
        where TDerived : class, new()
        where TBuilder : class, IBuilder<TTrigger, TDerived>, new();

    /// <summary>Register an observer for object changes.</summary>
    ILottaDBOptions Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();
}

/// <summary>
/// Fluent configuration for how an object type is stored and indexed.
/// </summary>
public interface IStoreConfiguration<T> where T : class, new()
{
    /// <summary>Set the key using a custom expression (e.g. composite keys).</summary>
    IStoreConfiguration<T> SetKey(Expression<Func<T, string>> resolver);

    /// <summary>Set the key strategy (Natural, DescendingTime, AscendingTime).</summary>
    IStoreConfiguration<T> SetKey(KeyStrategy strategy);

    /// <summary>Promote a property to a native Azure Table Storage column for server-side filtering.</summary>
    IStoreConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Configure a Lucene index field.</summary>
    IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Exclude a property from indexing.</summary>
    IStoreConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);
}

/// <summary>
/// Fluent configuration for a single Lucene index field.
/// </summary>
public interface IIndexPropertyConfiguration
{
    IIndexPropertyConfiguration AsKey();
    IIndexPropertyConfiguration NotAnalyzed();
    IIndexPropertyConfiguration AnalyzedWith<TAnalyzer>() where TAnalyzer : class;
    IIndexPropertyConfiguration AsNumeric();
    IIndexPropertyConfiguration WithDocValues();
}
