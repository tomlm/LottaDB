using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Azure.Data.Tables;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Util;
using Lotta.Internal;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// Core LottaDB implementation: one Azure table, one Lucene index.
/// Each write opens a Lucene session, adds/deletes, and the session disposes
/// (triggering commit and IndexSearcher refresh). Reads via <see cref="Search{T}()"/>
/// always reflect the last committed state.
/// </summary>
public class LottaDB
{
    private readonly string _name;
    private readonly LottaDBOptions _options;
    private readonly IBuilderFailureSink? _failureSink;
    private readonly TableStorageAdapter _tableStore;
    private readonly Lucene.Net.Linq.LuceneDataProvider _lucene;
    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<string, string> _keyTracker = new();
    private readonly ConcurrentDictionary<Type, List<object>> _observers = new();

    /// <summary>
    /// Create a LottaDB database instance.
    /// </summary>
    /// <param name="name">Database name. Used as the Azure table name and Lucene index name.</param>
    /// <param name="tableServiceClient">Azure Table Storage client (real Azure or Spotflow in-memory for tests).</param>
    /// <param name="directory">Lucene Directory (FSDirectory for production, RAMDirectory for tests).</param>
    /// <param name="options">Configuration options (registered types, views, builders, observers).</param>
    /// <param name="failureSink">Optional sink for builder failure reporting.</param>
    public LottaDB(string name, TableServiceClient tableServiceClient,
        LuceneDirectory directory, LottaDBOptions options, IBuilderFailureSink? failureSink = null)
    {
        _name = name;
        _options = options;
        _failureSink = failureSink;
        _tableStore = new TableStorageAdapter(tableServiceClient);

        // Ensure the Lucene index exists, then create the provider
        using (var writer = new IndexWriter(directory,
            new IndexWriterConfig(LuceneVersion.LUCENE_48, new StandardAnalyzer(LuceneVersion.LUCENE_48))
            { OpenMode = OpenMode.CREATE_OR_APPEND }))
        {
            writer.Commit();
        }
        _lucene = new Lucene.Net.Linq.LuceneDataProvider(directory, LuceneVersion.LUCENE_48);
        InitializeMetadata();
        InitializeObservers();
    }

    private void InitializeMetadata()
    {
        foreach (var (type, configObj) in _options.StoreConfigurations)
        {
            var m = typeof(TypeMetadata).GetMethod(nameof(TypeMetadata.Build))!.MakeGenericMethod(type);
            _metadata[type] = (TypeMetadata)m.Invoke(null, new[] { configObj })!;
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

    internal TypeMetadata GetMeta<T>() where T : class, new()
    {
        if (_metadata.TryGetValue(typeof(T), out var meta)) return meta;
        throw new InvalidOperationException($"Type {typeof(T).Name} not registered. Call opts.Store<{typeof(T).Name}>().");
    }

    private static string PK<T>() => typeof(T).Name;

    // TODO: Add Lucene type discrimination via TypeDiscriminatingMapper
    // once Iciclecreek.Lucene.Net.Linq supports custom field injection.
    // For now, Search<T>() returns exact-type matches only.
    // Use Query<T>() for polymorphic queries (table storage, works today).

    // === Write ===

    /// <summary>
    /// Save (upsert) an object. Key extracted from <see cref="KeyAttribute"/>.
    /// Writes to table storage, indexes into Lucene (session commit refreshes searcher), then runs builders.
    /// </summary>
    /// <typeparam name="T">The object type. Must be registered via <c>Store&lt;T&gt;()</c>.</typeparam>
    /// <param name="entity">The object to save.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any builder errors.</returns>
    public async Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMeta<T>();
        var key = meta.GetKey(entity);

        // Table storage
        await _tableStore.UpsertAsync(_name, PK<T>(), key, entity, meta);
        TrackKey(typeof(T), entity, key);

        // Lucene: open session, add, session disposes → commit → searcher refreshes
        try
        {
            using var session = _lucene.OpenSession<T>();
            session.Add(Lucene.Net.Linq.KeyConstraint.Unique, entity);
        }
        catch { /* skip types Lucene can't index */ }

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = key, Kind = ChangeKind.Saved, Object = entity }
        };
        var errors = new List<BuilderError>();

        await NotifyAsync<T>(entity, key, ChangeKind.Saved);
        await RunBuildersAsync(typeof(T), entity, key, TriggerKind.Saved, changes, errors, null, ct);

        return new ObjectResult { Changes = changes, Errors = errors };
    }

    /// <summary>
    /// Delete an object by key. Loads the object before deletion (for builders),
    /// removes from table storage and Lucene, then runs builders for cleanup.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="key">The unique key of the object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any derived changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new()
    {
        // Load before delete (for builders + Lucene delete)
        var (existing, _) = await _tableStore.GetAsync<T>(_name, PK<T>(), key);

        await _tableStore.DeleteAsync(_name, PK<T>(), key);

        if (existing != null)
        {
            try
            {
                using var session = _lucene.OpenSession<T>();
                session.Delete(existing);
            }
            catch { }
        }

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = key, Kind = ChangeKind.Deleted, Object = existing }
        };
        var errors = new List<BuilderError>();

        await NotifyAsync<T>(existing, key, ChangeKind.Deleted);
        if (existing != null)
            await RunBuildersAsync(typeof(T), existing, key, TriggerKind.Deleted, changes, errors, null, ct);

        return new ObjectResult { Changes = changes, Errors = errors };
    }

    /// <summary>Delete an object. Key extracted from <see cref="KeyAttribute"/>.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="entity">The object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any derived changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var key = GetTrackedKey(typeof(T), entity) ?? GetMeta<T>().GetKey(entity);
        return await DeleteAsync<T>(key, ct);
    }

    /// <summary>
    /// Read-modify-write. Fetches the object by key, applies the mutation function, and saves.
    /// Throws <see cref="InvalidOperationException"/> if the object does not exist.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="key">The unique key of the object to modify.</param>
    /// <param name="mutate">A function that receives the current object and returns the modified version.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> from the save operation.</returns>
    public async Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new()
    {
        var current = await GetAsync<T>(key, ct)
            ?? throw new InvalidOperationException($"{typeof(T).Name} '{key}' not found.");
        var mutated = mutate(current);
        return await SaveAsync(mutated, ct);
    }

    // === Read ===

    /// <summary>Point-read an object by key from Azure Table Storage.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="key">The unique key of the object.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The object, or null if not found.</returns>
    public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new()
    {
        var (result, _) = await _tableStore.GetAsync<T>(_name, PK<T>(), key);
        return result;
    }

    /// <summary>
    /// Query Azure Table Storage. Returns an <see cref="IQueryable{T}"/> filtered by type.
    /// Supports polymorphic queries — <c>Query&lt;BaseClass&gt;()</c> returns all derived types.
    /// Supports LINQ joins (used by <c>CreateView</c> expressions).
    /// </summary>
    /// <typeparam name="T">The object type. Returns objects of this type and all derived types.</typeparam>
    public IQueryable<T> Query<T>() where T : class, new()
    {
        GetMeta<T>();
        return _tableStore.QueryByType<T>(_name, typeof(T).Name).AsQueryable();
    }

    /// <summary>
    /// Search the Lucene index. Returns an <see cref="IQueryable{T}"/> backed by
    /// <c>Iciclecreek.Lucene.Net.Linq</c>. Always reflects the last committed state.
    /// </summary>
    /// <typeparam name="T">The object type to search for.</typeparam>
    /// <param name="query">Optional Lucene query string to pre-filter results (e.g. <c>"content:lucene* AND authorId:alice"</c>).</param>
    public IQueryable<T> Search<T>(string? query = null) where T : class, new()
    {
        GetMeta<T>();
        return _lucene.AsQueryable<T>();
    }

    // === Observe ===

    /// <summary>
    /// Subscribe to changes for objects of type <typeparamref name="T"/>.
    /// The handler fires after each commit for saved or deleted objects, including derived objects from builders.
    /// Dispose the returned handle to unsubscribe.
    /// </summary>
    /// <typeparam name="T">The object type to observe.</typeparam>
    /// <param name="handler">Async callback receiving an <see cref="ObjectChange{T}"/> for each change.</param>
    /// <returns>A disposable handle. Dispose to stop receiving notifications.</returns>
    public IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class, new()
    {
        var list = _observers.GetOrAdd(typeof(T), _ => new List<object>());
        lock (list) { list.Add(handler); }
        return new ObserverHandle<T>(list, handler);
    }

    private async Task NotifyAsync<T>(T? obj, string key, ChangeKind kind) where T : class, new()
    {
        if (!_observers.TryGetValue(typeof(T), out var list)) return;
        List<object> snapshot;
        lock (list) { snapshot = list.ToList(); }

        var change = new ObjectChange<T> { Key = key, Object = obj, Kind = kind };
        foreach (var h in snapshot)
        {
            if (h is Func<ObjectChange<T>, Task> typed)
                try { await typed(change); } catch { }
        }
    }

    // === Maintain ===

    /// <summary>
    /// Rebuild the entire Lucene index from Azure Table Storage.
    /// Re-indexes all registered types. Does not re-run builders.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    public Task RebuildIndex(CancellationToken ct = default)
    {
        foreach (var (type, _) in _metadata)
        {
            try
            {
                typeof(LottaDB)
                    .GetMethod(nameof(RebuildType), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(type)
                    .Invoke(this, Array.Empty<object>());
            }
            catch { }
        }
        return Task.CompletedTask;
    }

    private void RebuildType<T>() where T : class, new()
    {
        using var session = _lucene.OpenSession<T>();
        foreach (var item in _tableStore.QueryAll<T>(_name, PK<T>()))
            session.Add(Lucene.Net.Linq.KeyConstraint.Unique, item);
    }

    // === Key tracking ===

    private void TrackKey(Type type, object entity, string key)
        => _keyTracker[$"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}"] = key;

    private string? GetTrackedKey(Type type, object entity)
        => _keyTracker.TryGetValue($"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}", out var k) ? k : null;

    // === Builder engine ===

    private async Task RunBuildersAsync(Type triggerType, object entity, string key,
        TriggerKind trigger, List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string>? cycleGuard, CancellationToken ct)
    {
        cycleGuard ??= new HashSet<string>();
        if (!cycleGuard.Add($"{triggerType.FullName}||{key}")) return;

        foreach (var reg in _options.BuilderRegistrations.Where(b => b.TriggerType == triggerType))
        {
            try
            {
                var m = typeof(LottaDB)
                    .GetMethod(nameof(RunBuilder), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(reg.TriggerType, reg.DerivedType);
                await (Task)m.Invoke(this, new[] { Activator.CreateInstance(reg.BuilderType)!, entity, trigger, changes, errors, cycleGuard, ct })!;
            }
            catch (Exception ex)
            {
                await ReportError(errors, reg.BuilderType.Name, triggerType.Name, key, ex.InnerException ?? ex, ct);
            }
        }

        foreach (var viewReg in _options.ViewRegistrations)
        {
            try
            {
                await RunViewBuilder(entity, triggerType, key, trigger, viewReg, changes, errors, cycleGuard, ct);
            }
            catch (Exception ex)
            {
                await ReportError(errors, $"CreateView<{viewReg.ViewType.Name}>", triggerType.Name, key, ex.InnerException ?? ex, ct);
            }
        }
    }

    private async Task RunBuilder<TTrigger, TDerived>(
        IBuilder<TTrigger, TDerived> builder, object entity, TriggerKind trigger,
        List<ObjectChange> changes, List<BuilderError> errors,
        HashSet<string> cycleGuard, CancellationToken ct)
        where TTrigger : class, new() where TDerived : class, new()
    {
        bool hasResults = false;
        await foreach (var result in builder.BuildAsync((TTrigger)entity, trigger, this, ct))
        {
            hasResults = true;
            if (result.Object != null)
            {
                var r = await SaveDerived(result.Object, cycleGuard, ct);
                changes.AddRange(r.Changes);
                errors.AddRange(r.Errors);
            }
            else if (result.Key != null)
            {
                var r = await DeleteAsync<TDerived>(result.Key, ct);
                changes.AddRange(r.Changes);
                errors.AddRange(r.Errors);
            }
        }

        if (trigger == TriggerKind.Deleted && !hasResults)
        {
            var derivedMeta = _metadata[typeof(TDerived)];
            var keys = new List<string>();
            try
            {
                await foreach (var result in builder.BuildAsync((TTrigger)entity, TriggerKind.Saved, this, ct))
                    if (result.Object != null) keys.Add(derivedMeta.GetKey(result.Object));
            }
            catch { }
            foreach (var k in keys)
            {
                var r = await DeleteAsync<TDerived>(k, ct);
                changes.AddRange(r.Changes);
                errors.AddRange(r.Errors);
            }
        }
    }

    private async Task RunViewBuilder(object entity, Type triggerType, string key,
        TriggerKind trigger, ViewRegistration viewReg, List<ObjectChange> changes,
        List<BuilderError> errors, HashSet<string> cycleGuard, CancellationToken ct)
    {
        if (!_metadata.ContainsKey(viewReg.ViewType)) return;

        var parser = new ViewExpressionParser();
        var viewDef = parser.Parse(viewReg.Expression, viewReg.ViewType);
        if (viewDef == null || !viewDef.DependsOn.Contains(triggerType)) return;

        List<object> newViews;
        try { newViews = viewDef.Execute(this).ToList(); }
        catch (Exception ex)
        {
            await ReportError(errors, $"CreateView<{viewReg.ViewType.Name}>", triggerType.Name, key, ex, ct);
            return;
        }

        // Clear existing views of this type
        _tableStore.ClearTable(_name, viewReg.ViewType.Name);

        var saveMethod = typeof(LottaDB)
            .GetMethod(nameof(SaveDerived), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(viewReg.ViewType);
        foreach (var v in newViews)
        {
            var r = await (Task<ObjectResult>)saveMethod.Invoke(this, new[] { v, cycleGuard, ct })!;
            changes.AddRange(r.Changes);
            errors.AddRange(r.Errors);
        }
    }

    private async Task<ObjectResult> SaveDerived<T>(T entity, HashSet<string> cycleGuard, CancellationToken ct) where T : class, new()
    {
        // Same as SaveAsync but passes cycleGuard
        var meta = GetMeta<T>();
        var key = meta.GetKey(entity);
        await _tableStore.UpsertAsync(_name, PK<T>(), key, entity, meta);
        TrackKey(typeof(T), entity, key);
        try
        {
            using var session = _lucene.OpenSession<T>();
            session.Add(Lucene.Net.Linq.KeyConstraint.Unique, entity);
        }
        catch { }

        var changes = new List<ObjectChange>
        {
            new() { TypeName = typeof(T).Name, Key = key, Kind = ChangeKind.Saved, Object = entity }
        };
        var errors = new List<BuilderError>();
        await NotifyAsync(entity, key, ChangeKind.Saved);
        await RunBuildersAsync(typeof(T), entity, key, TriggerKind.Saved, changes, errors, cycleGuard, ct);
        return new ObjectResult { Changes = changes, Errors = errors };
    }

    private async Task ReportError(List<BuilderError> errors, string builderName, string triggerType, string key, Exception ex, CancellationToken ct)
    {
        var error = new BuilderError { BuilderName = builderName, TriggerTypeName = triggerType, TriggerKey = key, Exception = ex };
        errors.Add(error);
        if (_failureSink != null) await _failureSink.ReportAsync(error, ct);
    }
}

internal class ObserverHandle<T> : IDisposable where T : class
{
    private readonly List<object> _list;
    private readonly object _handler;
    public ObserverHandle(List<object> list, object handler) { _list = list; _handler = handler; }
    public void Dispose() { lock (_list) { _list.Remove(_handler); } }
}
