namespace LottaDB;

/// <summary>
/// A LottaDB database instance: one Azure table, one Lucene index.
/// Each write opens a Lucene session, commits on completion, and the IndexSearcher refreshes.
/// Reads via <see cref="Search{T}(string?)"/> always reflect the last committed state.
/// </summary>
public interface ILottaDB
{
    // === Write ===

    /// <summary>Save (upsert) an object. Key extracted from <see cref="KeyAttribute"/>. Opens a Lucene session, commits, refreshes searcher.</summary>
    /// <typeparam name="T">The object type. Must be registered via <c>Store&lt;T&gt;()</c>.</typeparam>
    /// <param name="entity">The object to save.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes (including derived objects from builders) and any builder errors.</returns>
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object by key. Removes from table storage and Lucene index.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="key">The unique key of the object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any derived changes from builders.</returns>
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object. Key extracted from <see cref="KeyAttribute"/>.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="entity">The object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any derived changes from builders.</returns>
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Read-modify-write. Fetches the object by key, applies the mutation function, and saves.
    /// Throws <see cref="InvalidOperationException"/> if the object does not exist.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="key">The unique key of the object to modify.</param>
    /// <param name="mutate">A function that receives the current object and returns the modified version.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> from the save operation.</returns>
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    // === Read ===

    /// <summary>Point-read an object by key from Azure Table Storage.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="key">The unique key of the object.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The object, or null if not found.</returns>
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Query Azure Table Storage. Returns an <see cref="IQueryable{T}"/> filtered by type.
    /// Supports LINQ joins (used by <c>CreateView</c> expressions).
    /// </summary>
    /// <typeparam name="T">The object type. Returns only objects of this type (PartitionKey filter).</typeparam>
    IQueryable<T> Query<T>() where T : class, new();

    /// <summary>
    /// Search the Lucene index. Returns an <see cref="IQueryable{T}"/> backed by
    /// <c>Iciclecreek.Lucene.Net.Linq</c>. Always reflects the last committed state.
    /// </summary>
    /// <typeparam name="T">The object type to search for.</typeparam>
    /// <param name="query">Optional Lucene query string to pre-filter results. Example: <c>"content:lucene* AND authorId:alice"</c>.</param>
    IQueryable<T> Search<T>(string? query = null) where T : class, new();

    // === Observe ===

    /// <summary>
    /// Subscribe to changes for objects of type <typeparamref name="T"/>.
    /// The handler fires after each commit for saved or deleted objects, including derived objects from builders.
    /// Dispose the returned handle to unsubscribe.
    /// </summary>
    /// <typeparam name="T">The object type to observe.</typeparam>
    /// <param name="handler">Async callback receiving an <see cref="ObjectChange{T}"/> for each change.</param>
    /// <returns>A disposable handle. Dispose to stop receiving notifications.</returns>
    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();

    // === Maintain ===

    /// <summary>
    /// Rebuild the entire Lucene index from Azure Table Storage.
    /// Re-indexes all registered types. Does not re-run builders.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task RebuildIndex(CancellationToken ct = default);
}
