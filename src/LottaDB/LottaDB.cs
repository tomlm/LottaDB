using Azure.Data.Tables;
using Lotta.Internal;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Util;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using static Lucene.Net.Search.FieldValueHitQueue;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta;

/// <summary>
/// A LottaDB database: one Azure table, one Lucene index.
/// Each write opens a Lucene session, commits, and the IndexSearcher refreshes.
/// On&lt;T&gt; handlers run inline after each write.
/// </summary>
public class LottaDB
{
    private readonly string _name;
    private readonly LottaConfiguration _options;
    private readonly TableStorageAdapter _tableAdapter;
    private readonly Lucene.Net.Linq.LuceneDataProvider _lucene;
    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, object> _mappers = new();
    private readonly ConcurrentDictionary<string, string> _keyTracker = new();
    private readonly ConcurrentDictionary<Type, List<object>> _handlers = new();

    // Cycle detection: tracks object keys being processed in the current call chain
    private static readonly AsyncLocal<HashSet<string>> _processing = new();
    // Collects changes across the entire call chain (root save + handler saves)
    private static readonly AsyncLocal<List<ObjectChange>?> _chainChanges = new();
    private static readonly AsyncLocal<List<Exception>?> _chainErrors = new();

    /// <summary>
    /// Create a LottaDB database instance.
    /// </summary>
    /// <param name="name">Database name. Used as the Azure table name and Lucene index name.</param>
    /// <param name="tableServiceClient">Azure Table Storage client.</param>
    /// <param name="directory">Lucene Directory.</param>
    /// <param name="options">Configuration (registered types, On&lt;T&gt; handlers).</param>
    public LottaDB(string name, TableServiceClient tableServiceClient,
        LuceneDirectory directory, LottaConfiguration options)
    {
        _name = name;
        _options = options;
        _tableAdapter = new TableStorageAdapter(tableServiceClient);

        using (var writer = new IndexWriter(directory,
            new IndexWriterConfig(LuceneVersion.LUCENE_48, new StandardAnalyzer(LuceneVersion.LUCENE_48))
            { OpenMode = OpenMode.CREATE_OR_APPEND }))
        {
            writer.Commit();
        }
        _lucene = new Lucene.Net.Linq.LuceneDataProvider(directory, LuceneVersion.LUCENE_48);
        _lucene.MapperFactory = (type, version, analyzer) =>
        {
            var mapperType = typeof(LottaDocumentMapper<>).MakeGenericType(type);
            _metadata.TryGetValue(type, out var meta);
            return Activator.CreateInstance(mapperType, version, meta)!;
        };

        InitializeMetadata();
        InitializeHandlers();
    }

    private void InitializeMetadata()
    {
        foreach (var (type, configObj) in _options.StorageConfigurations)
        {
            var m = typeof(TypeMetadata).GetMethod(nameof(TypeMetadata.Build))!.MakeGenericMethod(type);
            _metadata[type] = (TypeMetadata)m.Invoke(null, new[] { configObj })!;
        }
    }

    private void InitializeHandlers()
    {
        foreach (var reg in _options.OnRegistrations)
        {
            var list = _handlers.GetOrAdd(reg.ObjectType, _ => new List<object>());
            list.Add(reg.Handler);
        }
    }

    internal TypeMetadata GetMeta(Type type)
    {
        if (_metadata.TryGetValue(type, out var meta)) return meta;
            throw new InvalidOperationException($"Type {type.Name} not registered. Call opts.Store<{type.Name}>().");

    }

    internal TypeMetadata GetMeta<T>() where T : class, new()
    {
        return GetMeta(typeof(T));
    }

    private static string PartitionKey<T>() => TableStorageAdapter.PK;

    private Lucene.Net.Linq.Mapping.IDocumentMapper<T> GetMapper<T>() where T : class, new()
    {
        return (Lucene.Net.Linq.Mapping.IDocumentMapper<T>)_mappers.GetOrAdd(typeof(T), _ =>
        {
            _metadata.TryGetValue(typeof(T), out var meta);
            return new LottaDocumentMapper<T>(Version.LUCENE_48, meta);
        });
    }

    // === Write ===

    /// <summary>
    /// Save (upsert) an object. Key extracted from [Key] attribute.
    /// Writes to table storage, indexes into Lucene, then runs On&lt;T&gt; handlers inline.
    /// </summary>
    /// <param name="entity">The object to save.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any handler errors.</returns>
    public async Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMeta<T>();
        var key = meta.GetKey(entity);

        // For Auto keys, set the generated key back on the entity
        // so subsequent GetKey calls return the same value
        if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
            meta.SetKey(entity, key);

        await _tableAdapter.UpsertAsync(_name, key, entity, meta);
        TrackKey(typeof(T), entity, key);

        using (var session = _lucene.OpenSession<T>(GetMapper<T>()))
        {
            session.Add(Lucene.Net.Linq.KeyConstraint.Unique, entity);
        }

        var change = new ObjectChange { Type = entity.GetType(), Key = key, Kind = ChangeKind.Saved, Object = entity };

        // If we're inside a handler chain, add to the chain's collection
        var isRoot = _chainChanges.Value == null;
        var changes = _chainChanges.Value ??= new List<ObjectChange>();
        var errors = _chainErrors.Value ??= new List<Exception>();
        changes.Add(change);

        await RunHandlersAsync(entity, TriggerKind.Saved, errors, ct);

        if (isRoot)
        {
            var result = new ObjectResult { Changes = changes, Errors = errors };
            _chainChanges.Value = null;
            _chainErrors.Value = null;
            return result;
        }

        return new ObjectResult { Changes = new[] { change }, Errors = errors };
    }

    /// <summary>
    /// Delete an object by key. Removes from table storage and Lucene, then runs On&lt;T&gt; handlers.
    /// </summary>
    /// <param name="key">The unique key of the object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default) where T : class, new()
    {
        var (existing, _) = await _tableAdapter.GetAsync<T>(_name, key);

        await _tableAdapter.DeleteAsync(_name, key);

        if (existing != null)
        {
            using var session = _lucene.OpenSession<T>(GetMapper<T>());
            session.Delete(existing);
        }

        var change = new ObjectChange { Type = typeof(T), Key = key, Kind = ChangeKind.Deleted, Object = existing };

        var isRoot = _chainChanges.Value == null;
        var changes = _chainChanges.Value ??= new List<ObjectChange>();
        var errors = _chainErrors.Value ??= new List<Exception>();
        changes.Add(change);

        if (existing != null)
            await RunHandlersAsync(existing, TriggerKind.Deleted, errors, ct);

        if (isRoot)
        {
            var result = new ObjectResult { Changes = changes, Errors = errors };
            _chainChanges.Value = null;
            _chainErrors.Value = null;
            return result;
        }

        return new ObjectResult { Changes = new[] { change }, Errors = errors };
    }

    /// <summary>Delete an object. Key extracted from [Key] attribute.</summary>
    /// <param name="entity">The object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        var key = GetTrackedKey(typeof(T), entity) ?? GetMeta<T>().GetKey(entity);
        return await DeleteAsync<T>(key, ct);
    }

    /// <summary>
    /// Delete all objects matching a predicate. Queries table storage, deletes each match,
    /// removes from Lucene, and runs On&lt;T&gt; handlers for each deletion.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="predicate">Filter expression — objects matching this are deleted.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all deletions and handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(System.Linq.Expressions.Expression<Func<T, bool>> predicate, CancellationToken ct = default) where T : class, new()
    {
        var matches = Query<T>().Where(predicate).ToList();
        if (matches.Count == 0)
            return new ObjectResult();

        // Initialize the chain so inner deletes add to our collection
        var wasRoot = _chainChanges.Value == null;
        _chainChanges.Value ??= new List<ObjectChange>();
        _chainErrors.Value ??= new List<Exception>();

        foreach (var entity in matches)
            await DeleteAsync(entity, ct);

        if (wasRoot)
        {
            var result = new ObjectResult { Changes = _chainChanges.Value!, Errors = _chainErrors.Value! };
            _chainChanges.Value = null;
            _chainErrors.Value = null;
            return result;
        }

        return new ObjectResult { Changes = _chainChanges.Value!, Errors = _chainErrors.Value! };
    }

    /// <summary>
    /// Read-modify-write. Fetches the object by key, applies the mutation function, and saves.
    /// Throws if the object does not exist.
    /// </summary>
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
    /// <param name="key">The unique key of the object.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The object, or null if not found.</returns>
    public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class, new()
    {
        var (result, _) = await _tableAdapter.GetAsync<T>(_name, key);
        return result;
    }

    /// <summary>
    /// Query Azure Table Storage. Returns an <see cref="IQueryable{T}"/> filtered by type.
    /// Supports polymorphic queries and LINQ joins.
    /// </summary>
    /// <typeparam name="T">The object type. Returns objects of this type and all derived types.</typeparam>
    public IQueryable<T> Query<T>() where T : class, new()
    {
        return _tableAdapter.Query<T>(_name);
    }

    /// <summary>Query Azure Table Storage with a predicate filter.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="predicate">Filter expression.</param>
    public IQueryable<T> Query<T>(System.Linq.Expressions.Expression<Func<T, bool>> predicate) where T : class, new()
        => Query<T>().Where(predicate);

    /// <summary>
    /// Search the Lucene index. Returns an <see cref="IQueryable{T}"/> with full POCO fidelity
    /// (deserialized from stored _json field). Always reflects the last committed state.
    /// </summary>
    /// <typeparam name="T">The object type to search for.</typeparam>
    /// <param name="query">Optional Lucene query string to pre-filter results.</param>
    public IQueryable<T> Search<T>(string? query = null) where T : class, new()
    {
        GetMeta<T>();
        return _lucene.AsQueryable<T>(GetMapper<T>());
    }

    /// <summary>Search the Lucene index with a predicate filter.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="predicate">Filter expression applied to Lucene results.</param>
    public IQueryable<T> Search<T>(System.Linq.Expressions.Expression<Func<T, bool>> predicate) where T : class, new()
        => Search<T>().Where(predicate);

    // === On<T> (runtime registration) ===

    /// <summary>
    /// Register a handler at runtime. Returns a disposable — dispose to unregister.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="handler">Async handler receiving the object, trigger kind, and DB instance.</param>
    /// <returns>A disposable handle. Dispose to stop receiving notifications.</returns>
    public IDisposable On<T>(Func<T, TriggerKind, LottaDB, Task> handler) where T : class, new()
    {
        var list = _handlers.GetOrAdd(typeof(T), _ => new List<object>());
        lock (list) { list.Add(handler); }
        return new HandlerHandle(list, handler);
    }

    // === Maintain ===

    /// <summary>
    /// Rebuild the entire Lucene index from Azure Table Storage.
    /// Re-indexes all registered types. Does not run On&lt;T&gt; handlers.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    public async Task RebuildIndex(CancellationToken ct = default)
    {
        using (var session = _lucene.OpenSession<object>(GetMapper<object>()))
        {
            session.DeleteAll();
            await foreach (var item in _tableAdapter.Query(_name).ToAsyncEnumerable())
            {
                session.Add(Lucene.Net.Linq.KeyConstraint.Unique, item);
            }
        }
    }

    // === Key tracking ===

    private void TrackKey(Type type, object entity, string key)
        => _keyTracker[$"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}"] = key;

    private string? GetTrackedKey(Type type, object entity)
        => _keyTracker.TryGetValue($"{type.FullName}||{RuntimeHelpers.GetHashCode(entity)}", out var k) ? k : null;

    // === On<T> handler engine ===

    private async Task RunHandlersAsync<T>(T entity, TriggerKind kind,
        List<Exception> errors, CancellationToken ct) where T : class, new()
    {
        // Cycle detection: if this type is already being processed in the current chain, stop.
        // This prevents A→B→A infinite loops regardless of key values.
        var visited = _processing.Value ??= new HashSet<string>();
        var cycleKey = typeof(T).Name;
        if (!visited.Add(cycleKey))
            return; // cycle detected — this type is already in the handler chain

        try
        {
            if (!_handlers.TryGetValue(typeof(T), out var list)) return;

            List<object> snapshot;
            lock (list) { snapshot = list.ToList(); }

            foreach (var handler in snapshot)
            {
                if (handler is Func<T, TriggerKind, LottaDB, Task> typed)
                {
                    try
                    {
                        await typed(entity, kind, this);
                    }
                    catch (Exception ex)
                    {
                        errors.Add(ex);
                    }
                }
            }
        }
        finally
        {
            visited.Remove(cycleKey);
        }
    }
}

internal class HandlerHandle : IDisposable
{
    private readonly List<object> _list;
    private readonly object _handler;
    public HandlerHandle(List<object> list, object handler) { _list = list; _handler = handler; }
    public void Dispose() { lock (_list) { _list.Remove(_handler); } }
}
