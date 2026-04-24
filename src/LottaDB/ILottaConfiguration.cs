using Azure.Data.Tables;
using Microsoft.Extensions.AI;
using System.Linq.Expressions;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// Configuration options for a LottaDB database instance.
/// </summary>
public interface ILottaConfiguration
{
    /// <summary>
    /// Gets or sets the interval, in milliseconds, at which automatic search commits are performed.
    /// </summary>
    /// <remarks>
    /// This setting controls how frequently the search index is updated with recent changes. 
    /// A shorter interval means more up-to-date search results but may impact performance, 
    /// while a longer interval can improve performance at the cost of search freshness. 
    /// The default value is 1000 milliseconds (1 second), which is a good starting point
    /// for most applications. Adjust this value based on your application's needs and workload characteristics.
    /// </remarks>
    public int AutoCommitDelay { get; set; }

    /// <summary>
    /// Factory for instantiating a TableServiceClient with a given connection string. The default factory creates a new client per database instance, 
    /// which is suitable for most scenarios. Override this to implement custom client caching or dependency injection.
    /// </summary>
    Func<string, TableServiceClient> TableServiceClientFactory { get; set; }

    /// <summary>
    /// Factory for instantiating a Lucene Directory with a given name. The default factory creates a new AzureDirectory per database instance
    /// </summary>
    Func<string, LuceneDirectory> LuceneDirectoryFactory { get; set; }

    /// <summary>
    /// Embedding generator for vector similarity search. Used to convert text into
    /// vector embeddings for properties marked with <see cref="QueryableMode.Vector"/>.
    /// Defaults to ElBruno.LocalEmbeddings with SmartComponents/bge-micro-v2 model.
    /// Set to <c>null</c> to disable vector support.
    /// </summary>
    IEmbeddingGenerator<string, Embedding<float>>? EmbeddingGenerator { get; set; }

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

    /// <summary>
    /// Set the default search property for free-text queries and object-level
    /// <c>.Query()</c> / <c>.Similar()</c>. When set, the automatic <c>_content_</c>
    /// composite field is not created. The property must be indexed via
    /// <see cref="AddQueryable{TProp}"/> or <see cref="AddField{TProp}"/>.
    /// </summary>
    IStorageConfiguration<T> DefaultSearch<TProp>(Expression<Func<T, TProp>> property);
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
    /// <summary>Enable vector embeddings for similarity search. Composable with any analysis mode.</summary>
    IQueryableConfiguration Vector();
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
    /// <summary>Enable vector embeddings for similarity search. Composable with any analysis mode.</summary>
    IFieldConfiguration Vector();
}
