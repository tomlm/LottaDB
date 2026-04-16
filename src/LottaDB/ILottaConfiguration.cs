using Azure.Data.Tables;
using LuceneDirectory = Lucene.Net.Store.Directory;
using System.Linq.Expressions;

namespace Lotta;

/// <summary>
/// Configuration options for a LottaDB database instance.
/// </summary>
public interface ILottaConfiguration
{
    Func<string, TableServiceClient> CreateTableServiceClient { get; set; }
    Func<string, LuceneDirectory> CreateLuceneDirectory { get; set; }

    /// <summary>Register an object type. Config from [Key]/[Queryable] attributes, or fluent override.</summary>
    /// <typeparam name="T">The object type to register.</typeparam>
    /// <param name="configure">Optional fluent configuration for key strategy, queryable properties, etc.</param>
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

    /// <summary>
    /// Make a property queryable: promotes it to a Table Storage column for server-side
    /// filtering AND indexes it in Lucene for search. Smart defaults by type —
    /// strings are analyzed (full-text), value types are not analyzed (exact match).
    /// </summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to make queryable.</param>
    IQueryableConfiguration AddQueryable<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Promote a property to a Table Storage column only (not indexed in Lucene).</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to promote.</param>
    IStorageConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Index a property in Lucene only (not promoted to a Table Storage column).</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to index.</param>
    IFieldConfiguration AddField<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Exclude a property from both table storage and Lucene indexing.</summary>
    /// <typeparam name="TProp">The property type.</typeparam>
    /// <param name="property">Expression selecting the property to exclude.</param>
    IStorageConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);
}

/// <summary>
/// Fluent options for a queryable property.
/// </summary>
public interface IQueryableConfiguration
{
    /// <summary>Index with full-text analysis (tokenized, searchable by terms).</summary>
    IQueryableConfiguration Analyzed();
    /// <summary>Index as-is for exact match filtering only.</summary>
    IQueryableConfiguration NotAnalyzed();
}

/// <summary>
/// Fluent options for a Lucene-only indexed field.
/// </summary>
public interface IFieldConfiguration
{
    /// <summary>Index with full-text analysis (tokenized, searchable by terms).</summary>
    IFieldConfiguration Analyzed();
    /// <summary>Index as-is for exact match filtering only.</summary>
    IFieldConfiguration NotAnalyzed();
}
