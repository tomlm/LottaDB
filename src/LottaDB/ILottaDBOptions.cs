using System.Linq.Expressions;

namespace LottaDB;

/// <summary>
/// Configuration options for a LottaDB database instance.
/// </summary>
public interface ILottaDBOptions
{
    /// <summary>Register an object type. Config from [Key]/[Tag] attributes, or fluent override.</summary>
    /// <typeparam name="T">The object type to register.</typeparam>
    /// <param name="configure">Optional fluent configuration for key strategy, tags, and index fields.</param>
    ILottaDBOptions Store<T>(Action<IStoreConfiguration<T>>? configure = null) where T : class, new();

    /// <summary>
    /// Declare a materialized view as a LINQ join expression.
    /// Use <c>db.Query&lt;T&gt;()</c> in the expression for table storage sources.
    /// LottaDB parses the expression tree to extract dependencies and join keys.
    /// </summary>
    /// <typeparam name="TView">The materialized view type. Must also be registered via <c>Store&lt;TView&gt;()</c>.</typeparam>
    /// <param name="viewExpression">A LINQ expression that produces view objects from source objects via joins.</param>
    ILottaDBOptions CreateView<TView>(Expression<Func<LottaDB, IQueryable<TView>>> viewExpression) where TView : class, new();

    /// <summary>Register an explicit builder for custom derivation logic that can't be expressed as a LINQ join.</summary>
    /// <typeparam name="TTrigger">The type that triggers the builder when saved or deleted.</typeparam>
    /// <typeparam name="TDerived">The type of derived object the builder produces.</typeparam>
    /// <typeparam name="TBuilder">The builder implementation class.</typeparam>
    ILottaDBOptions AddBuilder<TTrigger, TDerived, TBuilder>()
        where TTrigger : class, new()
        where TDerived : class, new()
        where TBuilder : class, IBuilder<TTrigger, TDerived>, new();

    /// <summary>Register an observer that fires for the lifetime of the database instance.</summary>
    /// <typeparam name="T">The object type to observe.</typeparam>
    /// <param name="handler">Async callback receiving an <see cref="ObjectChange{T}"/> for each change.</param>
    ILottaDBOptions Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();
}

/// <summary>
/// Fluent configuration for how an object type is stored in Azure Table Storage and indexed in Lucene.
/// Used inside <c>opts.Store&lt;T&gt;(s =&gt; ...)</c>.
/// </summary>
/// <typeparam name="T">The object type being configured.</typeparam>
public interface IStoreConfiguration<T> where T : class, new()
{
    /// <summary>Set the key using a custom expression. For composite keys (e.g. <c>s.SetKey(x =&gt; $"{x.Domain}/{x.Id}")</c>).</summary>
    /// <param name="resolver">Expression that computes the key string from the object.</param>
    IStoreConfiguration<T> SetKey(Expression<Func<T, string>> resolver);

    /// <summary>Set the key strategy for time-ordered objects.</summary>
    /// <param name="strategy">The key generation strategy (Natural, DescendingTime, AscendingTime).</param>
    IStoreConfiguration<T> SetKey(KeyStrategy strategy);

    /// <summary>Promote a property to a native Azure Table Storage column (tag) for server-side filtering.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to promote.</param>
    IStoreConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Configure how a property is indexed in Lucene. Returns a builder for field options.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to configure.</param>
    IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Exclude a property from both table storage tags and Lucene indexing.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to exclude.</param>
    IStoreConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);
}

/// <summary>
/// Fluent configuration for a single Lucene index field.
/// </summary>
public interface IIndexPropertyConfiguration
{
    /// <summary>Mark this field as the Lucene document key (for upsert behavior).</summary>
    IIndexPropertyConfiguration AsKey();
    /// <summary>Index as a non-analyzed (exact match) field.</summary>
    IIndexPropertyConfiguration NotAnalyzed();
    /// <summary>Index with a custom analyzer for full-text search.</summary>
    /// <typeparam name="TAnalyzer">The Lucene analyzer type.</typeparam>
    IIndexPropertyConfiguration AnalyzedWith<TAnalyzer>() where TAnalyzer : class;
    /// <summary>Index as a trie-encoded numeric field (supports range queries).</summary>
    IIndexPropertyConfiguration AsNumeric();
    /// <summary>Enable DocValues for fast sorting on this field.</summary>
    IIndexPropertyConfiguration WithDocValues();
}
