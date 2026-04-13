namespace LottaDB;

/// <summary>
/// A LottaDB database instance: one Azure table, one Lucene index.
/// Each write opens a Lucene session, commits on completion, and the IndexSearcher refreshes.
/// Reads via <see cref="Search{T}()"/> always reflect the last committed state.
/// </summary>
public interface ILottaDB
{
    // === Write ===

    /// <summary>Save (upsert) an object. Key extracted from <see cref="KeyAttribute"/>. Opens a Lucene session, commits, refreshes searcher.</summary>
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object by key. Removes from table storage and Lucene index.</summary>
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object. Key extracted from <see cref="KeyAttribute"/>.</summary>
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Read-modify-write. Fetches the object by key, applies the mutation function, and saves.
    /// Throws if the object does not exist.
    /// </summary>
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    // === Read ===

    /// <summary>Point-read an object by key from Azure Table Storage.</summary>
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Query Azure Table Storage. Returns an <see cref="IQueryable{T}"/> filtered by type.
    /// Supports LINQ joins (used by <c>CreateView</c> expressions).
    /// </summary>
    IQueryable<T> Query<T>() where T : class, new();

    /// <summary>
    /// Search the Lucene index. Returns an <see cref="IQueryable{T}"/> backed by
    /// <c>Iciclecreek.Lucene.Net.Linq</c>. Always reflects the last committed state.
    /// </summary>
    /// <param name="query">Optional Lucene query string to pre-filter results. Example: "foo*^2"</param>
    IQueryable<T> Search<T>(string? query=null) where T : class, new();

    // === Observe ===

    /// <summary>
    /// Subscribe to changes for objects of type <typeparamref name="T"/>.
    /// The handler fires after each commit for saved or deleted objects, including derived objects from builders.
    /// Dispose the returned handle to unsubscribe.
    /// </summary>
    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();

    // === Maintain ===

    /// <summary>
    /// Rebuild the entire Lucene index from Azure Table Storage.
    /// Re-indexes all registered types. Does not re-run builders.
    /// </summary>
    Task RebuildIndex(CancellationToken ct = default);
}
