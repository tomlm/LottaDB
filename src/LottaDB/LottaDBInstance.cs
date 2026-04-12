namespace LottaDB;

internal class LottaDBInstance : ILottaDB
{
    private readonly LottaDBOptions _options;

    public LottaDBInstance(LottaDBOptions options)
    {
        _options = options;
    }

    public Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<ObjectResult> SaveAsync<T>(string partitionKey, string rowKey, T entity, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<ObjectResult> ChangeAsync<T>(string partitionKey, string rowKey, Func<T, T> mutate, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<ObjectResult> DeleteAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<T?> GetAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class
        => throw new NotImplementedException();

    public IAsyncEnumerable<T> QueryAsync<T>() where T : class
        => throw new NotImplementedException();

    public IAsyncEnumerable<T> SearchAsync<T>() where T : class
        => throw new NotImplementedException();

    public IQueryable<T> Search<T>() where T : class
        => throw new NotImplementedException();

    public IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class
        => throw new NotImplementedException();

    public Task RebuildIndex<T>(CancellationToken ct = default) where T : class
        => throw new NotImplementedException();
}
