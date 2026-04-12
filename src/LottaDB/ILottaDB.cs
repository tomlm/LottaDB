using System.Linq.Expressions;

namespace LottaDB;

public interface ILottaDB
{
    // === Write ===

    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class;
    Task<ObjectResult> SaveAsync<T>(string partitionKey, string rowKey, T entity, CancellationToken ct = default) where T : class;

    Task<ObjectResult> ChangeAsync<T>(string partitionKey, string rowKey, Func<T, T> mutate, CancellationToken ct = default) where T : class;
    Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default) where T : class;

    Task<ObjectResult> DeleteAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class;
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class;

    // === Read (table storage) ===

    Task<T?> GetAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class;
    Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class;

    IAsyncEnumerable<T> QueryAsync<T>() where T : class;

    // === Read (Lucene) ===

    IAsyncEnumerable<T> SearchAsync<T>() where T : class;

    // IQueryable variant for use in CreateView expressions (supports LINQ query syntax)
    IQueryable<T> Search<T>() where T : class;

    // === Observe ===

    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class;

    // === Maintain ===

    Task RebuildIndex<T>(CancellationToken ct = default) where T : class;
}
