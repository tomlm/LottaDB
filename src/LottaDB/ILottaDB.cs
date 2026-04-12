namespace LottaDB;

/// <summary>
/// The main LottaDB facade. Provides CRUD operations against Azure Table Storage,
/// full-text search via Lucene, observer notifications, and index maintenance.
/// All write operations automatically index into Lucene and run registered builders.
/// </summary>
public interface ILottaDB
{
    // === Write ===

    /// <summary>Save (upsert) an object. Keys are extracted from [PartitionKey]/[RowKey] attributes.</summary>
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>Save (upsert) an object with explicit partition key and row key.</summary>
    Task<ObjectResult> SaveAsync<T>(string partitionKey, string rowKey, T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Optimistic read-modify-write. Fetches the object, applies the mutation, saves with ETag check.
    /// Retries automatically on ETag conflict. The mutation function must be pure (may be called multiple times).
    /// </summary>
    Task<ObjectResult> ChangeAsync<T>(string partitionKey, string rowKey, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Optimistic read-modify-write using keys extracted from the object's attributes.
    /// </summary>
    Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object by partition key and row key. Triggers builders for cleanup of derived objects.</summary>
    Task<ObjectResult> DeleteAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object. Keys are extracted from [PartitionKey]/[RowKey] attributes.</summary>
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    // === Read (table storage) ===

    /// <summary>Point-read an object from Azure Table Storage by partition key and row key.</summary>
    Task<T?> GetAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Conditional get using the internally-tracked ETag. Returns null if the object hasn't changed.
    /// Pass <paramref name="force"/> = true to always fetch regardless of ETag.
    /// </summary>
    Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Query Azure Table Storage. Returns an async enumerable that supports LINQ operators.
    /// Predicates on tagged properties are pushed down as server-side OData filters.
    /// </summary>
    IAsyncEnumerable<T> QueryAsync<T>() where T : class, new();

    // === Read (Lucene) ===

    /// <summary>
    /// Search the Lucene index. Returns an async enumerable that supports LINQ operators.
    /// Optionally pass a Lucene query string to pre-filter results (e.g. "content:lucene* AND authorId:alice").
    /// </summary>
    IAsyncEnumerable<T> SearchAsync<T>(string? query = null) where T : class, new();

    /// <summary>
    /// Search the Lucene index, returning an IQueryable for LINQ query syntax.
    /// Used in <c>CreateView</c> expressions and for ad-hoc joins across types.
    /// </summary>
    IQueryable<T> Search<T>() where T : class, new();

    // === Observe ===

    /// <summary>
    /// Subscribe to changes for objects of type <typeparamref name="T"/>.
    /// The handler is called for every save or delete, including derived objects produced by builders.
    /// Dispose the returned handle to unsubscribe.
    /// </summary>
    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();

    // === Maintain ===

    /// <summary>
    /// Rebuild the Lucene index for type <typeparamref name="T"/> from Azure Table Storage.
    /// Drops and recreates the index, then re-indexes all objects of this type.
    /// Does not re-run builders.
    /// </summary>
    Task RebuildIndex<T>(CancellationToken ct = default) where T : class, new();
}
