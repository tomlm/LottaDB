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
    private readonly InMemoryTableStore _tableStore = new();
    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, object> _luceneProviders = new();
    private readonly ConcurrentDictionary<string, (string pk, string rk, string etag)> _etagTracker = new();
    private readonly ConcurrentDictionary<Type, List<object>> _observers = new();

    private const LuceneVersion AppLuceneVersion = LuceneVersion.LUCENE_48;

    public LottaDBInstance(LottaDBOptions options)
    {
        _options = options;
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
        _tableStore.Upsert(meta.TableName, pk, rk, entity, out var etag);

        // Track ETag
        TrackETag(typeof(T), pk, rk, etag);

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
            var current = _tableStore.Get<T>(meta.TableName, partitionKey, rowKey, out var currentETag);
            if (current == null)
                throw new InvalidOperationException($"Object {typeof(T).Name} with key ({partitionKey}, {rowKey}) not found.");

            var mutated = mutate(current);
            var pk = meta.GetPartitionKey(mutated);
            var rk = meta.GetRowKey(mutated);

            if (_tableStore.TryUpsertWithETag(meta.TableName, pk, rk, mutated, currentETag!, out var newETag))
            {
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
        var existing = _tableStore.Get<T>(meta.TableName, partitionKey, rowKey, out _);

        // Delete from table storage
        _tableStore.Delete(meta.TableName, partitionKey, rowKey, out _);

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
        var rk = meta.GetRowKey(entity);
        return await DeleteAsync<T>(pk, rk, ct);
    }

    // === Read (table storage) ===

    public Task<T?> GetAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var result = _tableStore.Get<T>(meta.TableName, partitionKey, rowKey, out var etag);
        if (result != null && etag != null)
            TrackETag(typeof(T), partitionKey, rowKey, etag);
        return Task.FromResult(result);
    }

    public Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var pk = meta.GetPartitionKey(entity);
        var rk = meta.GetRowKey(entity);

        if (!force)
        {
            var trackedETag = GetTrackedETag(typeof(T), pk, rk);
            if (trackedETag != null)
            {
                var currentETag = _tableStore.GetETag(meta.TableName, pk, rk);
                if (currentETag == trackedETag)
                    return Task.FromResult<T?>(null); // Unchanged
            }
        }

        return GetAsync<T>(pk, rk, ct);
    }

    public async IAsyncEnumerable<T> QueryAsync<T>() where T : class, new()
    {
        var meta = GetMetadata<T>();
        foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
        {
            yield return item;
        }
        await Task.CompletedTask;
    }

    // === Read (Lucene) ===

    public async IAsyncEnumerable<T> SearchAsync<T>() where T : class, new()
    {
        foreach (var item in Search<T>())
        {
            yield return item;
        }
        await Task.CompletedTask;
    }

    public IQueryable<T> Search<T>() where T : class, new()
    {
        var provider = GetOrCreateLuceneProvider<T>();
        return provider.AsQueryable<T>();
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

    public async Task RebuildIndex<T>(CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMetadata<T>();
        var provider = GetOrCreateLuceneProvider<T>();

        // Clear the index
        using (var writer = CreateIndexWriter<T>())
        {
            writer.DeleteAll();
            writer.Commit();
        }

        // Re-index all objects from table storage
        await foreach (var item in QueryAsync<T>())
        {
            var pk = meta.GetPartitionKey(item);
            var rk = meta.GetRowKey(item);
            IndexObject(item, meta, pk, rk);
        }
    }

    // === Lucene internals ===

    private Lucene.Net.Linq.LuceneDataProvider GetOrCreateLuceneProvider<T>() where T : class, new()
    {
        return (Lucene.Net.Linq.LuceneDataProvider)_luceneProviders.GetOrAdd(typeof(T), _ =>
        {
            var dir = _options.DirectoryProvider!.GetDirectory(typeof(T).Name.ToLowerInvariant());
            // Ensure the index exists
            using (var writer = new IndexWriter(dir, new IndexWriterConfig(AppLuceneVersion, new StandardAnalyzer(AppLuceneVersion))))
            {
                writer.Commit();
            }
            return new Lucene.Net.Linq.LuceneDataProvider(dir, AppLuceneVersion);
        });
    }

    private void IndexObject<T>(T obj, TypeMetadata meta, string pk, string rk) where T : class, new()
    {
        var provider = GetOrCreateLuceneProvider<T>();
        using var session = provider.OpenSession<T>();
        session.Add(obj);
    }

    private void RemoveFromIndex<T>(TypeMetadata meta, string pk, string rk) where T : class, new()
    {
        // For removal we need to query and delete
        // This is a simplified approach — delete all and re-add remaining
        var provider = GetOrCreateLuceneProvider<T>();
        using var session = provider.OpenSession<T>();
        // Find and remove by searching for the object
        var existing = provider.AsQueryable<T>().ToList();
        session.DeleteAll();

        // Re-add everything except the deleted one
        foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
        {
            session.Add(item);
        }
    }

    private IndexWriter CreateIndexWriter<T>() where T : class, new()
    {
        var dir = _options.DirectoryProvider!.GetDirectory(typeof(T).Name.ToLowerInvariant());
        return new IndexWriter(dir, new IndexWriterConfig(AppLuceneVersion, new StandardAnalyzer(AppLuceneVersion)));
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
                errors.Add(new BuilderError
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
                errors.Add(new BuilderError
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
        // Create builder instance
        var builder = Activator.CreateInstance(reg.BuilderType)!;

        // Call BuildAsync via reflection
        var buildMethod = reg.BuilderType.GetMethod("BuildAsync")!;
        var asyncEnum = buildMethod.Invoke(builder, new object[] { entity, trigger, this, ct })!;

        // Iterate results
        var moveNextMethod = asyncEnum.GetType().GetMethod("MoveNextAsync")!;
        var currentProp = asyncEnum.GetType().GetProperty("Current")!;

        bool hasResults = false;

        await using var enumerator = ((IAsyncDisposable)asyncEnum);
        // Use reflection to iterate the IAsyncEnumerable
        var enumerableType = typeof(IAsyncEnumerable<>).MakeGenericType(typeof(BuildResult<>).MakeGenericType(reg.DerivedType));
        await foreach (var resultObj in IterateAsyncEnumerable(asyncEnum, reg.DerivedType))
        {
            hasResults = true;
            var objProp = resultObj.GetType().GetProperty("Object")!;
            var keyProp = resultObj.GetType().GetProperty("Key")!;
            var derivedObj = objProp.GetValue(resultObj);
            var deleteKey = keyProp.GetValue(resultObj) as string;

            if (derivedObj != null)
            {
                // Save derived object through the pipeline
                var saveMethod = typeof(LottaDBInstance)
                    .GetMethod(nameof(SaveDerivedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(reg.DerivedType);
                var result = await (Task<ObjectResult>)saveMethod.Invoke(this, new object[] { derivedObj, cycleGuard, ct })!;
                changes.AddRange(result.Changes);
                errors.AddRange(result.Errors);
            }
            else if (deleteKey != null)
            {
                // Delete derived object
                var deleteMethod = typeof(LottaDBInstance)
                    .GetMethod(nameof(DeleteDerivedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(reg.DerivedType);
                var result = await (Task<ObjectResult>)deleteMethod.Invoke(this, new object[] { deleteKey, cycleGuard, ct })!;
                changes.AddRange(result.Changes);
                errors.AddRange(result.Errors);
            }
        }

        // Auto-delete on delete trigger with no results
        if (trigger == TriggerKind.Deleted && !hasResults)
        {
            // Auto-delete derived objects by trigger key
            var meta = _metadata[reg.DerivedType];
            var triggerMeta = _metadata[entity.GetType()];
            var triggerPk = triggerMeta.GetPartitionKey(entity);
            var triggerRk = triggerMeta.GetRowKey(entity);
            // Try to delete derived with same key
            var existing = _tableStore.Get<object>(meta.TableName, triggerPk, triggerRk, out _);
            if (existing != null || _tableStore.Delete(meta.TableName, triggerPk, triggerRk, out _))
            {
                changes.Add(new ObjectChange { TypeName = reg.DerivedType.Name, Key = $"{triggerPk}|{triggerRk}", Kind = ChangeKind.Deleted });
            }
        }
    }

    private async IAsyncEnumerable<object> IterateAsyncEnumerable(object asyncEnumerable, Type itemType)
    {
        // Get the generic IAsyncEnumerable<BuildResult<T>> and iterate
        var getEnumeratorMethod = asyncEnumerable.GetType().GetMethod("GetAsyncEnumerator")!;
        var enumerator = getEnumeratorMethod.Invoke(asyncEnumerable, new object[] { CancellationToken.None })!;
        var moveNextMethod = enumerator.GetType().GetMethod("MoveNextAsync")!;
        var currentProp = enumerator.GetType().GetProperty("Current")!;

        try
        {
            while (true)
            {
                var moveNextResult = (ValueTask<bool>)moveNextMethod.Invoke(enumerator, Array.Empty<object>())!;
                if (!await moveNextResult) break;
                var current = currentProp.GetValue(enumerator)!;
                yield return current;
            }
        }
        finally
        {
            if (enumerator is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
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
        // Parse the view expression to find if this trigger type is a dependency
        // For now, we use the ViewExpressionParser (to be implemented in Phase 10)
        // Skip if not implemented yet
        var parser = new ViewExpressionParser();
        var viewDef = parser.Parse(viewReg.Expression, viewReg.ViewType);
        if (viewDef == null || !viewDef.DependsOn.Contains(triggerType))
            return;

        if (trigger == TriggerKind.Deleted)
        {
            // Find affected views and delete them
            await DeleteAffectedViewsAsync(entity, triggerType, viewDef, changes, errors, cycleGuard, ct);
            return;
        }

        // Execute the LINQ expression to rebuild affected views
        await RebuildAffectedViewsAsync(entity, triggerType, viewDef, changes, errors, cycleGuard, ct);
    }

    private async Task DeleteAffectedViewsAsync(object entity, Type triggerType, ViewDefinition viewDef,
        List<ObjectChange> changes, List<BuilderError> errors, HashSet<string> cycleGuard, CancellationToken ct)
    {
        // Find views affected by this entity via join keys
        var affected = viewDef.FindAffectedViewKeys(entity, triggerType, this);
        foreach (var (viewPk, viewRk) in affected)
        {
            var meta = _metadata[viewDef.ViewType];
            _tableStore.Delete(meta.TableName, viewPk, viewRk, out _);
            RemoveFromIndexByKey(viewDef.ViewType, meta, viewPk, viewRk);
            changes.Add(new ObjectChange { TypeName = viewDef.ViewType.Name, Key = $"{viewPk}|{viewRk}", Kind = ChangeKind.Deleted });
        }
        await Task.CompletedTask;
    }

    private async Task RebuildAffectedViewsAsync(object entity, Type triggerType, ViewDefinition viewDef,
        List<ObjectChange> changes, List<BuilderError> errors, HashSet<string> cycleGuard, CancellationToken ct)
    {
        // Execute the compiled view expression
        var results = viewDef.Execute(this);
        foreach (var viewObj in results)
        {
            var saveMethod = typeof(LottaDBInstance)
                .GetMethod(nameof(SaveDerivedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .MakeGenericMethod(viewDef.ViewType);
            var result = await (Task<ObjectResult>)saveMethod.Invoke(this, new object[] { viewObj, cycleGuard, ct })!;
            changes.AddRange(result.Changes);
            errors.AddRange(result.Errors);
        }
    }

    private void RemoveFromIndexByKey(Type viewType, TypeMetadata meta, string pk, string rk)
    {
        // Simplified: rebuild index from table storage for this type
        if (_luceneProviders.TryGetValue(viewType, out var providerObj))
        {
            var rebuildMethod = typeof(LottaDBInstance)
                .GetMethod(nameof(RebuildIndexInternal), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .MakeGenericMethod(viewType);
            rebuildMethod.Invoke(this, Array.Empty<object>());
        }
    }

    private void RebuildIndexInternal<T>() where T : class, new()
    {
        var meta = GetMetadata<T>();
        var provider = GetOrCreateLuceneProvider<T>();

        using var session = provider.OpenSession<T>();
        session.DeleteAll();
        foreach (var item in _tableStore.QueryAll<T>(meta.TableName))
        {
            session.Add(item);
        }
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
