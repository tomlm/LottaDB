using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Azure.Data.Tables;
using LottaDB.Internal;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace LottaDB;

/// <summary>
/// Core LottaDB implementation. One table, one Lucene index, one database.
/// </summary>
public class LottaDBInstance : ILottaDB
{
    private readonly string _name;
    private readonly LottaDBOptions _options;
    private readonly IBuilderFailureSink? _failureSink;
    private readonly TableStorageAdapter _tableStore;
    private readonly LuceneIndex _luceneIndex;
    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<string, (string etag, string key)> _etagTracker = new();
    private readonly ConcurrentDictionary<string, string> _keyTracker = new();
    private readonly ConcurrentDictionary<Type, List<object>> _observers = new();

    public LottaDBInstance(string name, TableServiceClient tableServiceClient, LuceneDirectory directory,
        LottaDBOptions options, IBuilderFailureSink? failureSink = null)
    {
        _name = name;
        _options = options;
        _failureSink = failureSink;
        _tableStore = new TableStorageAdapter(tableServiceClient);
        _luceneIndex = new LuceneIndex(directory);
        InitializeMetadata();
        InitializeObservers();
    }

    private void InitializeMetadata()
    {
        foreach (var (type, configObj) in _options.StoreConfigurations)
        {
            var buildMethod = typeof(TypeMetadata).GetMethod(nameof(TypeMetadata.Build))!
                .MakeGenericMethod(type);
            var meta = (TypeMetadata)buildMethod.Invoke(null, new[] { configObj })!;
            _metadata[type] = meta;
        }
    }

    private void InitializeObservers()
    {
        foreach (var reg in _options.ObserverRegistrations)
        {
            var list = _observers.GetOrAdd(reg.ObjectType, _ => new List<object>());
            list.Add(reg.Handler);
        }
    }

    private TypeMetadata GetMetadata<T>() where T : class, new()
    {
        if (_metadata.TryGetValue(typeof(T), out var meta))
            return meta;
        throw new InvalidOperationException($"Type {typeof(T).Name} is not registered. Call opts.Store<{typeof(T).Name}>() during configuration.");
    }

    private string GetPartitionKey<T>() => typeof(T).Name;

    // === Write ===

    public async Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var key = meta.GetKey(entity);
        return await SaveInternalAsync(entity, key, meta, ct);
    }

    public async Task<ObjectResult> SaveAsync<T>(string key, T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        return await SaveInternalAsync(entity, key, meta, ct);
    }

    private async Task<ObjectResult> SaveInternalAsync<T>(T entity, string key, TypeMetadata meta, CancellationToken ct,
        HashSet<string>? cycleGuard = null) where T : class, new()
    {
        var pk = GetPartitionKey<T>();

        await _tableStore.UpsertAsync(_name, pk, key, entity, meta);

        var etag = _tableStore.GetETag(_name, pk, key) ?? "";
        TrackETag(typeof(T), key, etag);
        TrackKey(typeof(T), entity, key);

        IndexObject(entity);

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = key, Kind = ChangeKind.Saved, Object = entity }
        };
        var errors = new List<BuilderError>();

        await NotifyObserversAsync(entity, key, ChangeKind.Saved);
        await RunBuildersAsync(entity, typeof(T), key, TriggerKind.Saved, changes, errors, cycleGuard, ct);

        return new ObjectResult { Changes = changes, Errors = errors };
    }

    public async Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = GetPartitionKey<T>();
        const int maxRetries = 10;

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            var (current, currentETag) = await _tableStore.GetAsync<T>(_name, pk, key);
            if (current == null)
                throw new InvalidOperationException($"Object {typeof(T).Name} with key '{key}' not found.");

            var mutated = mutate(current);

            if (await _tableStore.UpsertWithETagAsync(_name, pk, key, mutated, meta, currentETag!))
            {
                var newETag = _tableStore.GetETag(_name, pk, key) ?? "";
                TrackETag(typeof(T), key, newETag);
                IndexObject(mutated);

                var changes = new List<ObjectChange>
                {
                    new() { TypeName = typeof(T).Name, Key = key, Kind = ChangeKind.Saved, Object = mutated }
                };
                var errors = new List<BuilderError>();

                await NotifyObserversAsync(mutated, key, ChangeKind.Saved);
                await RunBuildersAsync(mutated, typeof(T), key, TriggerKind.Saved, changes, errors, null, ct);

                return new ObjectResult { Changes = changes, Errors = errors };
            }
        }

        throw new InvalidOperationException($"ChangeAsync exceeded max retries ({maxRetries}) for {typeof(T).Name} '{key}'.");
    }

    public async Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var key = meta.GetKey(entity);
        return await ChangeAsync<T>(key, mutate, ct);
    }

    public async Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = GetPartitionKey<T>();

        var (existing, _) = await _tableStore.GetAsync<T>(_name, pk, key);
        await _tableStore.DeleteAsync(_name, pk, key);

        if (existing != null)
            RemoveFromIndex(existing);

        RemoveETag(typeof(T), key);

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = key, Kind = ChangeKind.Deleted, Object = existing }
        };
        var errors = new List<BuilderError>();

        await NotifyObserversAsync(existing, key, ChangeKind.Deleted);

        if (existing != null)
            await RunBuildersAsync(existing, typeof(T), key, TriggerKind.Deleted, changes, errors, null, ct);

        return new ObjectResult { Changes = changes, Errors = errors };
    }

    public async Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var key = GetTrackedKey(typeof(T), entity) ?? meta.GetKey(entity);
        return await DeleteAsync<T>(key, ct);
    }

    // === Read ===

    public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = GetPartitionKey<T>();
        var (result, etag) = await _tableStore.GetAsync<T>(_name, pk, key);
        if (result != null && etag != null)
            TrackETag(typeof(T), key, etag);
        return result;
    }

    public async Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var key = GetTrackedKey(typeof(T), entity) ?? meta.GetKey(entity);

        if (!force)
        {
            var trackedETag = GetTrackedETag(typeof(T), key);
            if (trackedETag != null)
            {
                var pk = GetPartitionKey<T>();
                var currentETag = _tableStore.GetETag(_name, pk, key);
                if (currentETag == trackedETag)
                    return null;
            }
        }

        return await GetAsync<T>(key, ct);
    }

    public IQueryable<T> Query<T>() where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = GetPartitionKey<T>();
        // Query table storage filtered by type (PartitionKey)
        return _tableStore.QueryAll<T>(_name, pk).AsQueryable();
    }

    public IQueryable<T> Search<T>() where T : class, new()
    {
        return _luceneIndex.Search<T>();
    }

    public IQueryable<T> Search<T>(string query) where T : class, new()
    {
        // TODO: Apply Lucene query string pre-filter
        return Search<T>();
    }

    // === Observe ===

    public IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new()
    {
        var list = _observers.GetOrAdd(typeof(T), _ => new List<object>());
        lock (list) { list.Add(handler); }
        return new ObserverDisposable<T>(list, handler);
    }

    // === Maintain ===

    public Task RebuildIndex(CancellationToken ct = default)
    {
        // Re-add all objects from all registered types
        foreach (var (type, meta) in _metadata)
        {
            try
            {
                var rebuildMethod = typeof(LottaDBInstance)
                    .GetMethod(nameof(RebuildTypeIndex), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(type);
                rebuildMethod.Invoke(this, new object[] { meta });
            }
            catch { }
        }

        return Task.CompletedTask;
    }

    private void RebuildTypeIndex<T>(TypeMetadata meta) where T : class, new()
    {
        try
        {
            var pk = GetPartitionKey<T>();
            _luceneIndex.WriteWithSession<T>(session =>
            {
                foreach (var item in _tableStore.QueryAll<T>(_name, pk))
                {
                    session.Add(item);
                }
            });
        }
        catch { }
    }

    // === Lucene indexing ===

    private void IndexObject<T>(T entity) where T : class, new()
    {
        try
        {
            _luceneIndex.WriteWithSession<T>(session =>
            {
                // Add with UpdateOrAdd constraint — replaces doc with same key
                session.Add(Lucene.Net.Linq.KeyConstraint.UpdateOrAdd, entity);
            });
        }
        catch { }
    }

    private void RemoveFromIndex<T>(T entity) where T : class, new()
    {
        try
        {
            _luceneIndex.WriteWithSession<T>(session =>
            {
                session.Delete(entity);
            });
        }
        catch { }
    }

    // === ETag tracking ===

    private void TrackETag(Type type, string key, string etag)
    {
        _etagTracker[$"{type.FullName}||{key}"] = (etag, key);
    }

    private string? GetTrackedETag(Type type, string key)
    {
        return _etagTracker.TryGetValue($"{type.FullName}||{key}", out var entry) ? entry.etag : null;
    }

    private void RemoveETag(Type type, string key)
    {
        _etagTracker.TryRemove($"{type.FullName}||{key}", out _);
    }

    // === Key tracking (for time-keyed objects) ===

    private void TrackKey(Type type, object entity, string key)
    {
        var trackKey = $"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}";
        _keyTracker[trackKey] = key;
    }

    private string? GetTrackedKey(Type type, object entity)
    {
        var trackKey = $"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}";
        return _keyTracker.TryGetValue(trackKey, out var key) ? key : null;
    }

    // === Observer notifications ===

    private async Task NotifyObserversAsync<T>(T? obj, string key, ChangeKind kind) where T : class, new()
    {
        if (!_observers.TryGetValue(typeof(T), out var list)) return;
        List<object> snapshot;
        lock (list) { snapshot = list.ToList(); }

        var change = new ObjectChange<T> { Key = key, Object = obj, Kind = kind };
        foreach (var handler in snapshot)
        {
            if (handler is Func<ObjectChange<T>, Task> typed)
            {
                try { await typed(change); }
                catch { }
            }
        }
    }

    // === Builder engine ===

    private async Task RunBuildersAsync(object entity, Type triggerType, string key,
        TriggerKind trigger, List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string>? cycleGuard, CancellationToken ct)
    {
        cycleGuard ??= new HashSet<string>();
        var cycleKey = $"{triggerType.FullName}||{key}";
        if (!cycleGuard.Add(cycleKey))
            return;

        foreach (var reg in _options.BuilderRegistrations.Where(b => b.TriggerType == triggerType))
        {
            try
            {
                var method = typeof(LottaDBInstance)
                    .GetMethod(nameof(RunTypedBuilderAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(reg.TriggerType, reg.DerivedType);
                var builder = Activator.CreateInstance(reg.BuilderType)!;
                await (Task)method.Invoke(this, new object[] { builder, entity, trigger, changes, errors, cycleGuard, ct })!;
            }
            catch (Exception ex)
            {
                ReportBuilderError(errors, new BuilderError
                {
                    BuilderName = reg.BuilderType.Name,
                    TriggerTypeName = triggerType.Name,
                    TriggerKey = key,
                    Exception = ex
                });
            }
        }

        foreach (var viewReg in _options.ViewRegistrations)
        {
            try
            {
                await RunViewBuilderAsync(entity, triggerType, key, trigger, viewReg, changes, errors, cycleGuard, ct);
            }
            catch (Exception ex)
            {
                ReportBuilderError(errors, new BuilderError
                {
                    BuilderName = $"CreateView<{viewReg.ViewType.Name}>",
                    TriggerTypeName = triggerType.Name,
                    TriggerKey = key,
                    Exception = ex
                });
            }
        }
    }

    private async Task RunTypedBuilderAsync<TTrigger, TDerived>(
        IBuilder<TTrigger, TDerived> builder, object entity, TriggerKind trigger,
        List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string> cycleGuard, CancellationToken ct)
        where TTrigger : class, new()
        where TDerived : class, new()
    {
        bool hasResults = false;

        await foreach (var result in builder.BuildAsync((TTrigger)entity, trigger, this, ct))
        {
            hasResults = true;
            if (result.Object != null)
            {
                var saveResult = await SaveDerivedAsync(result.Object, cycleGuard, ct);
                changes.AddRange(saveResult.Changes);
                errors.AddRange(saveResult.Errors);
            }
            else if (result.Key != null)
            {
                var deleteResult = await DeleteAsync<TDerived>(result.Key, ct);
                changes.AddRange(deleteResult.Changes);
                errors.AddRange(deleteResult.Errors);
            }
        }

        if (trigger == TriggerKind.Deleted && !hasResults)
        {
            var derivedMeta = _metadata[typeof(TDerived)];
            var keysToDelete = new List<string>();

            try
            {
                await foreach (var result in builder.BuildAsync((TTrigger)entity, TriggerKind.Saved, this, ct))
                {
                    if (result.Object != null)
                        keysToDelete.Add(derivedMeta.GetKey(result.Object));
                }
            }
            catch { }

            foreach (var derivedKey in keysToDelete)
            {
                var deleteResult = await DeleteAsync<TDerived>(derivedKey, ct);
                changes.AddRange(deleteResult.Changes);
                errors.AddRange(deleteResult.Errors);
            }
        }
    }

    private async Task<ObjectResult> SaveDerivedAsync<T>(T entity, HashSet<string> cycleGuard, CancellationToken ct) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var key = meta.GetKey(entity);
        return await SaveInternalAsync(entity, key, meta, ct, cycleGuard);
    }

    private async Task RunViewBuilderAsync(object entity, Type triggerType, string key,
        TriggerKind trigger, ViewRegistration viewReg, List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string> cycleGuard, CancellationToken ct)
    {
        if (!_metadata.ContainsKey(viewReg.ViewType))
            return;

        var parser = new ViewExpressionParser();
        var viewDef = parser.Parse(viewReg.Expression, viewReg.ViewType);
        if (viewDef == null || !viewDef.DependsOn.Contains(triggerType))
            return;

        var viewMeta = _metadata[viewReg.ViewType];

        List<object> newViews;
        try
        {
            newViews = viewDef.Execute(this).ToList();
        }
        catch (Exception ex)
        {
            ReportBuilderError(errors, new BuilderError
            {
                BuilderName = $"CreateView<{viewReg.ViewType.Name}>",
                TriggerTypeName = triggerType.Name,
                TriggerKey = key,
                Exception = ex
            });
            return;
        }

        // Clear existing views of this type and replace
        var viewPk = viewReg.ViewType.Name;
        _tableStore.ClearTable(_name, viewPk);

        var saveMethod = typeof(LottaDBInstance)
            .GetMethod(nameof(SaveDerivedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(viewReg.ViewType);

        foreach (var viewObj in newViews)
        {
            var result = await (Task<ObjectResult>)saveMethod.Invoke(this, new object[] { viewObj, cycleGuard, ct })!;
            changes.AddRange(result.Changes);
            errors.AddRange(result.Errors);
        }
    }

    private void ReportBuilderError(List<BuilderError> errors, BuilderError error)
    {
        errors.Add(error);
        _failureSink?.ReportAsync(error).ConfigureAwait(false).GetAwaiter().GetResult();
    }
}

internal class ObserverDisposable<T> : IDisposable where T : class
{
    private readonly List<object> _list;
    private readonly Func<ObjectChange<T>, Task> _handler;

    public ObserverDisposable(List<object> list, Func<ObjectChange<T>, Task> handler)
    {
        _list = list;
        _handler = handler;
    }

    public void Dispose()
    {
        lock (_list) { _list.Remove(_handler); }
    }
}
