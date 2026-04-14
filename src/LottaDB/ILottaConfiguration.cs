using System.Linq.Expressions;

namespace Lotta;

/// <summary>
/// Configuration options for a LottaDB database instance.
/// </summary>
public interface ILottaConfiguration
{
    /// <summary>Register an object type. Config from [Key]/[Tag] attributes, or fluent override.</summary>
    /// <typeparam name="T">The object type to register.</typeparam>
    /// <param name="configure">Optional fluent configuration for key strategy, tags, and index fields.</param>
    ILottaConfiguration Store<T>(Action<IStorageConfiguration<T>>? configure = null) where T : class, new();

    /// <summary>
    /// Register a handler that runs inline after every save or delete of type <typeparamref name="T"/>.
    /// The handler has full DB access — it can save, delete, query, or search any type.
    /// Multiple handlers per type are allowed and run in registration order.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="handler">Async handler receiving the object, trigger kind, and DB instance.</param>
    ILottaConfiguration On<T>(Func<T, TriggerKind, LottaDB, Task> handler) where T : class, new();
}

/// <summary>
/// Fluent configuration for how an object type is stored in Azure Table Storage and indexed in Lucene.
/// Used inside <c>opts.Store&lt;T&gt;(s =&gt; ...)</c>.
/// </summary>
/// <typeparam name="T">The object type being configured.</typeparam>
public interface IStorageConfiguration<T> where T : class, new()
{
    /// <summary>Set the key using a custom expression. For composite keys (e.g. <c>s.SetKey(x =&gt; $"{x.Domain}/{x.Id}")</c>).</summary>
    /// <param name="resolver">Expression that computes the key string from the object.</param>
    IStorageConfiguration<T> SetKey(Expression<Func<T, string>> resolver);

    /// <summary>Set the key strategy for time-ordered objects.</summary>
    /// <param name="strategy">The key generation mode (Manual or Auto).</param>
    IStorageConfiguration<T> SetKey(KeyMode strategy);

    /// <summary>Promote a property to a native Azure Table Storage column (tag) for server-side filtering.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to promote.</param>
    IStorageConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Configure how a property is indexed in Lucene. Returns a builder for field options.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to configure.</param>
    IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Exclude a property from both table storage tags and Lucene indexing.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to exclude.</param>
    IStorageConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);
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
