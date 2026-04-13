namespace LottaDB;

/// <summary>
/// A LottaDB database: one Azure table, one Lucene index.
/// Each write opens a Lucene session, commits on completion, searcher refreshes.
/// </summary>
public interface ILottaDB
{
    // === Write ===

    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new();
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new();
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new();
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new();

    // === Read ===

    Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new();
    IQueryable<T> Query<T>() where T : class, new();
    IQueryable<T> Search<T>() where T : class, new();
    IQueryable<T> Search<T>(string query) where T : class, new();

    // === Observe ===

    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new();

    // === Maintain ===

    Task RebuildIndex(CancellationToken ct = default);
}
