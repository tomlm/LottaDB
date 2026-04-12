using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using LottaDB.Internal;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;

namespace LottaDB;

internal class LottaDBInstance : ILottaDB
{
    private readonly LottaDBOptions _options;
    private readonly IBuilderFailureSink? _failureSink;
    private readonly Internal.TableStorageAdapter _tableStore;
    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, LuceneIndex> _luceneIndexes = new();
    private readonly ConcurrentDictionary<string, (string pk, string rk, string etag)> _etagTracker = new();
    // Track generated RowKeys for objects so DeleteAsync(entity) works for time-keyed types
    private readonly ConcurrentDictionary<string, string> _rowKeyTracker = new();
    private readonly ConcurrentDictionary<Type, List<object>> _observers = new();


    public LottaDBInstance(LottaDBOptions options, Azure.Data.Tables.TableServiceClient tableServiceClient, IBuilderFailureSink? failureSink = null)
    {
        _options = options;
        _failureSink = failureSink;
        _tableStore = new Internal.TableStorageAdapter(tableServiceClient);
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

    // === Write ===

    public async Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = meta.GetPartitionKey(entity);
        var rk = meta.GetRowKey(entity);
        return await SaveInternalAsync(entity, pk, rk, meta, ct);
    }

    public async Task<ObjectResult> SaveAsync<T>(string partitionKey, string rowKey, T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        return await SaveInternalAsync(entity, partitionKey, rowKey, meta, ct);
    }

    private async Task<ObjectResult> SaveInternalAsync<T>(T entity, string pk, string rk, TypeMetadata meta, CancellationToken ct,
        HashSet<string>? cycleGuard = null) where T : class, new()
    {
        // Write to table storage
        await _tableStore.UpsertAsync(meta.TableName, pk, rk, entity, meta);

        // Track ETag and RowKey (for time-keyed objects, RowKey is generated and must be tracked)
        var etag = _tableStore.GetETag(meta.TableName, pk, rk) ?? "";
        TrackETag(typeof(T), pk, rk, etag);
        TrackRowKey(typeof(T), entity, pk, rk);

        // Index into Lucene
        IndexObject(entity, meta, pk, rk);

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = $"{pk}|{rk}", Kind = ChangeKind.Saved, Object = entity }
        };
        var errors = new List<BuilderError>();

        // Notify observers
        await NotifyObserversAsync(entity, pk, rk, ChangeKind.Saved);

        // Run builders
        await RunBuildersAsync(entity, typeof(T), pk, rk, TriggerKind.Saved, changes, errors, cycleGuard, ct);

        return new ObjectResult { Changes = changes, Errors = errors };
    }

    public async Task<ObjectResult> ChangeAsync<T>(string partitionKey, string rowKey, Func<T, T> mutate, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        const int maxRetries = 10;

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            var (current, currentETag) = await _tableStore.GetAsync<T>(meta.TableName, partitionKey, rowKey);
            if (current == null)
                throw new InvalidOperationException($"Object {typeof(T).Name} with key ({partitionKey}, {rowKey}) not found.");

            var mutated = mutate(current);
            var pk = meta.GetPartitionKey(mutated);
            var rk = meta.GetRowKey(mutated);

            if (await _tableStore.UpsertWithETagAsync(meta.TableName, pk, rk, mutated, meta, currentETag!))
            {
                var newETag = _tableStore.GetETag(meta.TableName, pk, rk) ?? "";
                TrackETag(typeof(T), pk, rk, newETag);
                IndexObject(mutated, meta, pk, rk);

                var changes = new List<ObjectChange>
                {
                    new() { TypeName = typeof(T).Name, Key = $"{pk}|{rk}", Kind = ChangeKind.Saved, Object = mutated }
                };
                var errors = new List<BuilderError>();

                await NotifyObserversAsync(mutated, pk, rk, ChangeKind.Saved);
                await RunBuildersAsync(mutated, typeof(T), pk, rk, TriggerKind.Saved, changes, errors, null, ct);

                return new ObjectResult { Changes = changes, Errors = errors };
            }
            // ETag mismatch — retry
        }

        throw new InvalidOperationException($"ChangeAsync exceeded max retries ({maxRetries}) for {typeof(T).Name} ({partitionKey}, {rowKey}).");
    }

    public async Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = meta.GetPartitionKey(entity);
        var rk = meta.GetRowKey(entity);
        return await ChangeAsync<T>(pk, rk, mutate, ct);
    }

    public async Task<ObjectResult> DeleteAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();

        // Load before delete (for builders)
        var (existing, _) = await _tableStore.GetAsync<T>(meta.TableName, partitionKey, rowKey);

        // Delete from table storage
        await _tableStore.DeleteAsync(meta.TableName, partitionKey, rowKey);

        // Remove from Lucene
        RemoveFromIndex<T>(meta, partitionKey, rowKey);

        // Remove ETag tracking
        RemoveETag(typeof(T), partitionKey, rowKey);

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = $"{partitionKey}|{rowKey}", Kind = ChangeKind.Deleted, Object = existing }
        };
        var errors = new List<BuilderError>();

        await NotifyObserversAsync(existing, partitionKey, rowKey, ChangeKind.Deleted);

        // Run builders with the pre-delete object
        if (existing != null)
            await RunBuildersAsync(existing, typeof(T), partitionKey, rowKey, TriggerKind.Deleted, changes, errors, null, ct);

        return new ObjectResult { Changes = changes, Errors = errors };
    }

    public async Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = meta.GetPartitionKey(entity);
        // For time-keyed objects, GetRowKey generates a new key each call.
        // Use the tracked RowKey from the last save instead.
        var rk = GetTrackedRowKey(typeof(T), entity, pk) ?? meta.GetRowKey(entity);
        return await DeleteAsync<T>(pk, rk, ct);
    }

    // === Read (table storage) ===

    public async Task<T?> GetAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var (result, etag) = await _tableStore.GetAsync<T>(meta.TableName, partitionKey, rowKey);
        if (result != null && etag != null)
            TrackETag(typeof(T), partitionKey, rowKey, etag);
        return result;
    }

    public async Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = meta.GetPartitionKey(entity);
        var rk = GetTrackedRowKey(typeof(T), entity, pk) ?? meta.GetRowKey(entity);

        if (!force)
        {
            var trackedETag = GetTrackedETag(typeof(T), pk, rk);
            if (trackedETag != null)
            {
                var currentETag = _tableStore.GetETag(meta.TableName, pk, rk);
                if (currentETag == trackedETag)
                    return null; // Unchanged
            }
        }

        return await GetAsync<T>(pk, rk, ct);
    }

    public IQueryable<T> Query<T>() where T : class, new()
    {
        var meta = GetMetadata<T>();
        // TODO: When Azure.Data.Tables supports IQueryable with OData pushdown,
        // wire that in here. For now, use in-memory queryable over table storage.
        return _tableStore.QueryAll<T>(meta.TableName).AsQueryable();
    }

    // === Read (Lucene) ===

    public IQueryable<T> Search<T>() where T : class, new()
    {
        var index = GetOrCreateLuceneIndex<T>();
        return index.Search<T>();
    }

    public IQueryable<T> Search<T>(string query) where T : class, new()
    {
        // TODO: Parse Lucene query string and apply as pre-filter
        return Search<T>();
    }

    // === Observe ===

    public IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new()
    {
        var list = _observers.GetOrAdd(typeof(T), _ => new List<object>());
        lock (list)
        {
            list.Add(handler);
        }
        return new ObserverDisposable<T>(list, handler);
    }

    // === Maintain ===

    public Task RebuildIndex<T>(CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var index = GetOrCreateLuceneIndex<T>();

        index.WriteWithSession<T>(session =>
        {
            session.DeleteAll();
            foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
            {
                session.Add(item);
            }
        });

        return Task.CompletedTask;
    }

    // === Lucene internals ===

    private LuceneIndex GetOrCreateLuceneIndex<T>() where T : class, new()
    {
        return _luceneIndexes.GetOrAdd(typeof(T), _ =>
        {
            var dir = _options.DirectoryProvider!.GetDirectory(typeof(T).Name.ToLowerInvariant());
            return new LuceneIndex(dir);
        });
    }

    private void IndexObject<T>(T obj, TypeMetadata meta, string pk, string rk) where T : class, new()
    {
        try
        {
            var index = GetOrCreateLuceneIndex<T>();
            index.WriteWithSession<T>(session =>
            {
                session.DeleteAll();
                foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
                {
                    session.Add(item);
                }
            });
        }
        catch
        {
            // If Lucene can't index this type (e.g., unsupported field types), skip silently.
        }
    }

    private void RemoveFromIndex<T>(TypeMetadata meta, string pk, string rk) where T : class, new()
    {
        try
        {
            var index = GetOrCreateLuceneIndex<T>();
            index.WriteWithSession<T>(session =>
            {
                session.DeleteAll();
                foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
                {
                    session.Add(item);
                }
            });
        }
        catch { }
    }


    // === ETag tracking ===

    private void TrackETag(Type type, string pk, string rk, string etag)
    {
        var key = ETagKey(type, pk, rk);
        _etagTracker[key] = (pk, rk, etag);
    }

    private string? GetTrackedETag(Type type, string pk, string rk)
    {
        var key = ETagKey(type, pk, rk);
        return _etagTracker.TryGetValue(key, out var entry) ? entry.etag : null;
    }

    private void RemoveETag(Type type, string pk, string rk)
    {
        _etagTracker.TryRemove(ETagKey(type, pk, rk), out _);
    }

    private static string ETagKey(Type type, string pk, string rk) => $"{type.FullName}||{pk}||{rk}";

    private void ReportBuilderError(List<BuilderError> errors, BuilderError error)
    {
        errors.Add(error);
        _failureSink?.ReportAsync(error).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    // RowKey tracking for time-keyed objects
    private void TrackRowKey(Type type, object entity, string pk, string rk)
    {
        // Use identity hash to track the specific object instance
        var key = $"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}";
        _rowKeyTracker[key] = $"{pk}||{rk}";
    }

    private string? GetTrackedRowKey(Type type, object entity, string pk)
    {
        var key = $"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}";
        if (_rowKeyTracker.TryGetValue(key, out var stored))
        {
            var parts = stored.Split("||");
            if (parts.Length == 2 && parts[0] == pk)
                return parts[1];
        }
        return null;
    }

    // === Observer notifications ===

    private async Task NotifyObserversAsync<T>(T? obj, string pk, string rk, ChangeKind kind) where T : class, new()
    {
        if (!_observers.TryGetValue(typeof(T), out var list)) return;

        List<object> snapshot;
        lock (list) { snapshot = list.ToList(); }

        var change = new ObjectChange<T>
        {
            Key = $"{pk}|{rk}",
            Object = obj,
            Kind = kind
        };

        foreach (var handler in snapshot)
        {
            if (handler is Func<ObjectChange<T>, Task> typed)
            {
                try { await typed(change); }
                catch { /* observer errors don't propagate */ }
            }
        }
    }

    // === Builder engine ===

    private async Task RunBuildersAsync(object entity, Type triggerType, string pk, string rk,
        TriggerKind trigger, List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string>? cycleGuard, CancellationToken ct)
    {
        cycleGuard ??= new HashSet<string>();
        var cycleKey = $"{triggerType.FullName}||{pk}||{rk}";
        if (!cycleGuard.Add(cycleKey))
            return; // Cycle detected

        // Find explicit builders for this trigger type
        foreach (var reg in _options.BuilderRegistrations.Where(b => b.TriggerType == triggerType))
        {
            try
            {
                await RunSingleBuilderAsync(entity, reg, trigger, changes, errors, cycleGuard, ct);
            }
            catch (Exception ex)
            {
                ReportBuilderError(errors, new BuilderError
                {
                    BuilderName = reg.BuilderType.Name,
                    TriggerTypeName = triggerType.Name,
                    TriggerKey = $"{pk}|{rk}",
                    Exception = ex
                });
            }
        }

        // Find CreateView definitions that depend on this trigger type
        foreach (var viewReg in _options.ViewRegistrations)
        {
            try
            {
                await RunViewBuilderAsync(entity, triggerType, pk, rk, trigger, viewReg, changes, errors, cycleGuard, ct);
            }
            catch (Exception ex)
            {
                ReportBuilderError(errors, new BuilderError
                {
                    BuilderName = $"CreateView<{viewReg.ViewType.Name}>",
                    TriggerTypeName = triggerType.Name,
                    TriggerKey = $"{pk}|{rk}",
                    Exception = ex
                });
            }
        }
    }

    private async Task RunSingleBuilderAsync(object entity, BuilderRegistration reg,
        TriggerKind trigger, List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string> cycleGuard, CancellationToken ct)
    {
        // Dispatch to a generic method that can iterate IAsyncEnumerable<BuildResult<TDerived>> properly
        var method = typeof(LottaDBInstance)
            .GetMethod(nameof(RunTypedBuilderAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(reg.TriggerType, reg.DerivedType);

        var builder = Activator.CreateInstance(reg.BuilderType)!;
        await (Task)method.Invoke(this, new object[] { builder, entity, trigger, changes, errors, cycleGuard, ct })!;
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
                var deleteResult = await DeleteDerivedAsync<TDerived>(result.Key, cycleGuard, ct);
                changes.AddRange(deleteResult.Changes);
                errors.AddRange(deleteResult.Errors);
            }
        }

        // Auto-delete on delete trigger with no results:
        // Re-run builder with Saved to discover what keys it *would have* produced, then delete those.
        if (trigger == TriggerKind.Deleted && !hasResults)
        {
            var derivedMeta = _metadata[typeof(TDerived)];
            var keysToDelete = new List<(string pk, string rk)>();

            try
            {
                await foreach (var result in builder.BuildAsync((TTrigger)entity, TriggerKind.Saved, this, ct))
                {
                    if (result.Object != null)
                    {
                        var derivedPk = derivedMeta.GetPartitionKey(result.Object);
                        var derivedRk = derivedMeta.GetRowKey(result.Object);
                        keysToDelete.Add((derivedPk, derivedRk));
                    }
                }
            }
            catch { /* auto-delete is best-effort */ }

            foreach (var (derivedPk, derivedRk) in keysToDelete)
            {
                await _tableStore.DeleteAsync(derivedMeta.TableName, derivedPk, derivedRk);
                RemoveFromIndex<TDerived>(derivedMeta, derivedPk, derivedRk);
                changes.Add(new ObjectChange { TypeName = typeof(TDerived).Name, Key = $"{derivedPk}|{derivedRk}", Kind = ChangeKind.Deleted });
                await NotifyObserversAsync<TDerived>(null, derivedPk, derivedRk, ChangeKind.Deleted);
            }
        }
    }

    private async Task<ObjectResult> SaveDerivedAsync<T>(T entity, HashSet<string> cycleGuard, CancellationToken ct) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = meta.GetPartitionKey(entity);
        var rk = meta.GetRowKey(entity);
        return await SaveInternalAsync(entity, pk, rk, meta, ct, cycleGuard);
    }

    private async Task<ObjectResult> DeleteDerivedAsync<T>(string key, HashSet<string> cycleGuard, CancellationToken ct) where T : class, new()
    {
        var meta = GetMetadata<T>();
        // Key could be pk|rk or just the row key — try to find it
        var parts = key.Split('|');
        string pk, rk;
        if (parts.Length == 2)
        {
            pk = parts[0];
            rk = parts[1];
        }
        else
        {
            pk = key;
            rk = key;
        }
        return await DeleteAsync<T>(pk, rk, ct);
    }

    private async Task RunViewBuilderAsync(object entity, Type triggerType, string pk, string rk,
        TriggerKind trigger, ViewRegistration viewReg, List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string> cycleGuard, CancellationToken ct)
    {
        if (!_metadata.ContainsKey(viewReg.ViewType))
            return;

        // Parse the view expression to detect dependencies
        var parser = new ViewExpressionParser();
        var viewDef = parser.Parse(viewReg.Expression, viewReg.ViewType);
        if (viewDef == null || !viewDef.DependsOn.Contains(triggerType))
            return;

        var viewMeta = _metadata[viewReg.ViewType];

        // Execute the full view expression to produce new views
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
                TriggerKey = $"{pk}|{rk}",
                Exception = ex
            });
            return;
        }

        // Clear existing views of this type and replace with new results
        _tableStore.ClearTable(viewMeta.TableName);

        var saveMethod = typeof(LottaDBInstance)
            .GetMethod(nameof(SaveDerivedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(viewReg.ViewType);

        foreach (var viewObj in newViews)
        {
            var result = await (Task<ObjectResult>)saveMethod.Invoke(this, new object[] { viewObj, cycleGuard, ct })!;
            changes.AddRange(result.Changes);
            errors.AddRange(result.Errors);
        }

        // If trigger was delete and no views were produced, that's the auto-delete behavior
        if (trigger == TriggerKind.Deleted && newViews.Count == 0)
        {
            // Views already cleared above
        }
    }

    private void RebuildIndexInternal<T>() where T : class, new()
    {
        try
        {
            var meta = GetMetadata<T>();
            var index = GetOrCreateLuceneIndex<T>();
            index.WriteWithSession<T>(session =>
            {
                session.DeleteAll();
                foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
                {
                    session.Add(item);
                }
            });
        }
        catch { }
    }
}

internal class ObserverDisposable<T> : IDisposable where T : class, new()
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
        lock (_list)
        {
            _list.Remove(_handler);
        }
    }
}
