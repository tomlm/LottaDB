namespace LottaDB;

/// <summary>
/// The main LottaDB facade. A single database: one Azure table, one Lucene index.
/// All write operations automatically index into Lucene and run registered builders.
/// </summary>
public interface ILottaDB
{
    // === Write ===

    /// <summary>Save (upsert) an object. Key extracted from [Key] attribute.</summary>
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>Save (upsert) an object with an explicit key.</summary>
    Task<ObjectResult> SaveAsync<T>(string key, T entity, CancellationToken ct = default) where T : class, new();

    /// <summary>
    /// Optimistic read-modify-write. Fetches, applies mutation, saves with ETag check.
    /// Retries on conflict. Mutation function must be pure (may be called multiple times).
    /// </summary>
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    /// <summary>Optimistic read-modify-write. Key extracted from [Key] attribute.</summary>
    Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object by key.</summary>
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new();

    /// <summary>Delete an object. Key extracted from [Key] attribute.</summary>
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new();

    // === Read ===

    /// <summary>Point-read an object by key.</summary>
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new();

    /// <summary>Conditional get using internally-tracked ETag. Returns null if unchanged. force=true always fetches.</summary>
    Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class, new();

    /// <summary>Query Azure Table Storage. Auto-filtered by type. Supports polymorphic queries.</summary>
    IQueryable<T> Query<T>() where T : class, new();

    /// <summary>Search the Lucene index. Auto-filtered by type. Supports polymorphic queries.</summary>
    IQueryable<T> Search<T>() where T : class, new();

    /// <summary>Search the Lucene index with a Lucene query string pre-filter.</summary>
    IQueryable<T> Search<T>(string query) where T : class, new();

    // === Observe ===

    /// <summary>Subscribe to changes for objects of type T (including derived types).</summary>
    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();

    // === Maintain ===

    /// <summary>Rebuild the entire Lucene index from table storage.</summary>
    Task RebuildIndex(CancellationToken ct = default);
}
