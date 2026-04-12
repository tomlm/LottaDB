using System.Linq.Expressions;

namespace LottaDB;

/// <summary>
/// Configuration options for LottaDB. Used in <c>services.AddLottaDB(opts => ...)</c>.
/// </summary>
public interface ILottaDBOptions
{
    /// <summary>Set the Lucene Directory provider (e.g. <see cref="RAMDirectoryProvider"/> or <see cref="FSDirectoryProvider"/>).</summary>
    ILottaDBOptions UseLuceneDirectory(IDirectoryProvider provider);

    /// <summary>
    /// Register an object type for storage and indexing.
    /// Storage config comes from [PartitionKey]/[RowKey]/[Tag] attributes by default.
    /// Pass a lambda to override or extend with fluent configuration.
    /// </summary>
    ILottaDBOptions Store<T>(Action<IStoreConfiguration<T>>? configure = null) where T : class, new();

    /// <summary>
    /// Declare a materialized view as a LINQ join expression. LottaDB parses the expression tree
    /// to extract dependencies and join keys, and incrementally maintains the view as source objects change.
    /// Use <see cref="ILottaDB.Search{T}"/> in the expression for LINQ query syntax support.
    /// </summary>
    ILottaDBOptions CreateView<TView>(Expression<Func<ILottaDB, IQueryable<TView>>> viewExpression) where TView : class, new();

    /// <summary>
    /// Register an explicit builder for custom derivation logic that can't be expressed as a LINQ join.
    /// </summary>
    ILottaDBOptions AddBuilder<TTrigger, TDerived, TBuilder>()
        where TTrigger : class, new()
        where TDerived : class, new()
        where TBuilder : class, IBuilder<TTrigger, TDerived>, new();

    /// <summary>
    /// Register an observer that is called whenever an object of type <typeparamref name="T"/> changes.
    /// Observers registered here fire for the lifetime of the application.
    /// </summary>
    ILottaDBOptions Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();
}

/// <summary>
/// Fluent configuration for how an object type is stored in Azure Table Storage and indexed in Lucene.
/// Used inside <c>opts.Store&lt;T&gt;(s => ...)</c>.
/// </summary>
public interface IStoreConfiguration<T> where T : class, new()
{
    /// <summary>Override the default table name (which is the lowercased CLR type name).</summary>
    IStoreConfiguration<T> SetTableName(string tableName);

    /// <summary>Set the partition key resolver. Overrides [PartitionKey] attribute.</summary>
    IStoreConfiguration<T> SetPartitionKey(Expression<Func<T, string>> resolver);

    /// <summary>Set the row key resolver with a custom expression. Overrides [RowKey] attribute.</summary>
    IStoreConfiguration<T> SetRowKey(Expression<Func<T, string>> resolver);

    /// <summary>Set the row key strategy. Overrides [RowKey] attribute.</summary>
    IStoreConfiguration<T> SetRowKey(RowKeyStrategy strategy);

    /// <summary>Promote a property to a native Azure Table Storage column (tag) for server-side filtering.</summary>
    IStoreConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Add a computed tag column that is derived from the object but not stored on the POCO.</summary>
    IStoreConfiguration<T> AddComputedTag<TProp>(string name, Func<T, TProp> compute);

    /// <summary>Configure a Lucene index field for a property. Returns a builder for field options.</summary>
    IIndexPropertyConfiguration Index<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Exclude a property from both table storage tags and Lucene indexing.</summary>
    IStoreConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);
}

/// <summary>
/// Fluent configuration for a single Lucene index field.
/// </summary>
public interface IIndexPropertyConfiguration
{
    /// <summary>Mark this field as the document key.</summary>
    IIndexPropertyConfiguration AsKey();
    /// <summary>Index as a non-analyzed (exact match) field.</summary>
    IIndexPropertyConfiguration NotAnalyzed();
    /// <summary>Index with a custom analyzer for full-text search.</summary>
    IIndexPropertyConfiguration AnalyzedWith<TAnalyzer>() where TAnalyzer : class;
    /// <summary>Index as a trie-encoded numeric field (supports range queries).</summary>
    IIndexPropertyConfiguration AsNumeric();
    /// <summary>Enable DocValues for fast sorting.</summary>
    IIndexPropertyConfiguration WithDocValues();
}
