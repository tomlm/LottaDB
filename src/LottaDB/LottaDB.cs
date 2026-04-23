using Azure.Data.Tables;
using Lotta.Internal;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Analysis;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Util;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta;

/// <summary>
/// A LottaDB database: one Azure table, one Lucene index.
/// Each write opens a Lucene session, commits, and the IndexSearcher refreshes.
/// On&lt;T&gt; handlers run inline after each write.
/// </summary>
public class LottaDB : IDisposable
{
    private readonly object _lock = new object();
    private readonly string _name;
    private readonly LottaConfiguration _config;
    private readonly TableStorageAdapter _tableAdapter;
    private LuceneDirectory _directory;
    private ReadOnlyLuceneDataProvider _lucene;
    private Task _lazySearcherTask = Task.CompletedTask;
    private CancellationTokenSource _autoCommitCancelToken = new CancellationTokenSource();
    private IndexWriter _indexWriter;
    private volatile bool _indexDirty;
    private bool _disposed;

    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, IDocumentMapper> _mappers = new();
    private readonly ConcurrentDictionary<Type, List<object>> _handlers = new();

    // Cycle detection: tracks object keys being processed in the current call chain
    private static readonly AsyncLocal<HashSet<string>> _processing = new();
    // Collects changes across the entire call chain (root save + handler saves)
    private static readonly AsyncLocal<List<ObjectChange>?> _chainChanges = new();
    private static readonly AsyncLocal<List<Exception>?> _chainErrors = new();
    internal const string JSON_FIELD = "_json_";
    internal const string KEY_FIELD = "_key_";
    internal const string CONTENT_FIELD = "_content_";

    private static LottaConfiguration CreateConfig(string connectionString, Action<LottaConfiguration>? options)
    {
        var config = new LottaConfiguration(connectionString);
        options?.Invoke(config);
        return config;
    }

    /// <summary>
    /// Create a LottaDB database instance.
    /// </summary>
    /// <param name="name">Database name. Used as the Azure table name and Lucene index name.</param>
    /// <param name="config">Configuration (registered types, On&lt;T&gt; handlers, storage factories).</param>
    public LottaDB(string name, LottaConfiguration config)
    {
        _name = name;
        _config = config;
        _tableAdapter = new TableStorageAdapter(config.TableServiceClientFactory(name));
        _directory = config.LuceneDirectoryFactory(name);

        InitializeMetadata();
        InitializeMappers();
        InitializeHandlers();

        // Build a per-field analyzer that merges all mapper analyzers.
        // Default is KeywordAnalyzer (matching DocumentMapperBase) so unregistered
        // fields like _key_ are stored verbatim. Per-type field analyzers are merged below.
        var perFieldAnalyzer = new PerFieldAnalyzer(new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        perFieldAnalyzer.AddAnalyzer(KEY_FIELD, new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        foreach (var mapper in _mappers.Values)
            perFieldAnalyzer.Merge(mapper.Analyzer);

        _indexWriter = new IndexWriter(_directory,
            new IndexWriterConfig(LuceneVersion.LUCENE_48, perFieldAnalyzer)
            {
                OpenMode = OpenMode.CREATE_OR_APPEND,
                UseCompoundFile = true,
            });
        _indexWriter.Commit();
        _lucene = new ReadOnlyLuceneDataProvider(_directory, LuceneVersion.LUCENE_48);
        if (_config.EmbeddingGenerator != null)
            _lucene.Settings.EmbeddingGenerator = _config.EmbeddingGenerator;
        _lucene.MapperFactory = (type, version, analyzer) =>
        {
            var mapperType = typeof(LottaDocumentMapper<>).MakeGenericType(type);
            _metadata.TryGetValue(type, out var meta);
            return Activator.CreateInstance(mapperType, version, _config.Analyzer, meta, _config.EmbeddingGenerator)!;
        };
    }


    // Opens and immediately disposes a session per registered type so each mapper's
    // PerFieldAnalyzer (e.g. _content_ → EnglishAnalyzer) is merged into the shared
    // IndexWriter analyzer. Without this, RebuildIndex — which uses session<object>
    // and dispatches to per-type mappers through the document registry — would index
    // fields with the default KeywordAnalyzer because the registry's on-demand mappers
    // never get merged back into the provider's analyzer.
    private void InitializeMappers()
    {
        var method = typeof(LottaDB).GetMethod(nameof(WarmUpMapper),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        foreach (var type in _metadata.Keys)
            method.MakeGenericMethod(type).Invoke(this, null);
    }

    private void WarmUpMapper<T>() where T : class, new()
    {
        GetMapper<T>(); // pre-populate so the per-field analyzer can be merged
    }

    public LottaDB(string name, Action<LottaConfiguration>? options = null)
    : this(name, CreateConfig(null!, options))
    {
    }

    public LottaDB(string name, string connectionString, Action<LottaConfiguration>? options = null)
        : this(name, CreateConfig(connectionString, options))
    {
    }


    private void InitializeMetadata()
    {
        foreach (var (type, configObj) in _config.StorageConfigurations)
        {
            var m = typeof(TypeMetadata).GetMethod(nameof(TypeMetadata.Build))!.MakeGenericMethod(type);
            _metadata[type] = (TypeMetadata)m.Invoke(null, new[] { configObj })!;
        }
    }

    private void InitializeHandlers()
    {
        foreach (var reg in _config.OnRegistrations)
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
            return new LottaDocumentMapper<T>(Version.LUCENE_48, _config.Analyzer, meta, _config.EmbeddingGenerator);
        });
    }

    private IDocumentMapper GetMapper(Type type)
    {
        return _mappers.GetOrAdd(type, static (t, state) =>
        {
            var db = (LottaDB)state!;
            db._metadata.TryGetValue(t, out var meta);
            var mapperType = typeof(LottaDocumentMapper<>).MakeGenericType(t);
            return (IDocumentMapper)Activator.CreateInstance(mapperType, Version.LUCENE_48, db._config.Analyzer, meta, db._config.EmbeddingGenerator)!;
        }, this);
    }

    // === Write ===

    /// <summary>
    /// Save (upsert) an object. Key extracted from [Key] attribute.
    /// Writes to table storage, indexes into Lucene, then runs On&lt;T&gt; handlers inline.
    /// </summary>
    /// <param name="entity">The object to save.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any handler errors.</returns>
    public async Task<ObjectResult> SaveAsync(object entity, CancellationToken ct = default)
    {
        var meta = GetMeta(entity.GetType());
        var key = meta.GetKey(entity);
        var mapper = GetMapper(entity.GetType());

        // For Auto keys, set the generated key back on the entity
        // so subsequent GetKey calls return the same value
        if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
            meta.SetKey(entity, key);

        await _tableAdapter.UpsertAsync(_name, key, entity, meta);

        SaveLuceneObject(key, entity, mapper);

        _lazySearcherTask = ReloadSearcherAsync(lazy: true);

        var change = new ObjectChange { Type = entity.GetType(), Key = key, Kind = ChangeKind.Saved, Object = entity };

        // If we're inside a handler chain, add to the chain's collection
        var isRoot = _chainChanges.Value == null;
        var changes = _chainChanges.Value ??= new List<ObjectChange>();
        var errors = _chainErrors.Value ??= new List<Exception>();
        changes.Add(change);

        await RunHandlersAsync(entity, entity.GetType(), TriggerKind.Saved, errors, ct);

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

        return await DeleteAsync<T>(existing!, ct);
    }

    /// <summary>Delete an object. Key extracted from [Key] attribute.</summary>
    /// <param name="entity">The object to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default) where T : class, new()
    {
        if (entity == null)
        {
            return new ObjectResult();
        }
        var meta = GetMeta<T>();
        var key = meta.GetKey(entity);
        var mapper = GetMapper<T>();

        await _tableAdapter.DeleteAsync(_name, key);

        DeleteLuceneObject(key);

        _lazySearcherTask = ReloadSearcherAsync(lazy: true);

        var change = new ObjectChange { Type = typeof(T), Key = key, Kind = ChangeKind.Deleted, Object = entity };

        var isRoot = _chainChanges.Value == null;
        var changes = _chainChanges.Value ??= new List<ObjectChange>();
        var errors = _chainErrors.Value ??= new List<Exception>();
        changes.Add(change);

        await RunHandlersAsync(entity, TriggerKind.Deleted, errors, ct);

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
    /// Delete all objects matching a predicate. Queries table storage, deletes each match,
    /// removes from Lucene, and runs On&lt;T&gt; handlers for each deletion.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="predicate">Filter expression — objects matching this are deleted.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all deletions and handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(Expression<Func<T, bool>> predicate, CancellationToken ct = default) where T : class, new()
    {
        var matches = await GetManyAsync<T>(predicate).ToListAsync(ct);
        if (matches.Count == 0)
            return new ObjectResult();

        return await DeleteManyAsync(matches.Select(m => GetMeta<T>().GetKey(m)), ct);
    }

    /// <summary>
    /// Read-modify-write with optimistic concurrency. Fetches the object by key (capturing its
    /// ETag), applies the mutation, and commits with an <c>If-Match</c> condition. If another
    /// writer changed the row in between, the read-modify-write is retried against the latest
    /// state — so the mutation is guaranteed to be applied on top of the committed version.
    /// The mutation function may therefore be invoked more than once; it must be a pure function
    /// of its input. Throws if the object does not exist, or if retries are exhausted.
    /// </summary>
    /// <param name="key">The unique key of the object to modify.</param>
    /// <param name="mutate">A function that receives the current object to be mutated</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> from the save operation.</returns>
    public Task<ObjectResult> ChangeAsync<T>(string key, Action<T> mutate, CancellationToken ct = default) where T : class, new()
        => ChangeAsync<T>(key, entity =>
        {
            mutate(entity);
            return entity;
        }, ct);

    /// <summary>
    /// Read-modify-write with optimistic concurrency. Fetches the object by key (capturing its
    /// ETag), applies the mutation, and commits with an <c>If-Match</c> condition. If another
    /// writer changed the row in between, the read-modify-write is retried against the latest
    /// state — so the mutation is guaranteed to be applied on top of the committed version.
    /// The mutation function may therefore be invoked more than once; it must be a pure function
    /// of its input. Throws if the object does not exist, or if retries are exhausted.
    /// </summary>
    /// <param name="key">The unique key of the object to modify.</param>
    /// <param name="mutate">A function that receives the current object and returns the modified version.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> from the save operation.</returns>
    public async Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default) where T : class, new()
    {
        const int maxAttempts = 16;
        var meta = GetMeta<T>();
        var mapper = GetMapper<T>();

        try
        {

            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                ct.ThrowIfCancellationRequested();

                var (current, etag) = await _tableAdapter.GetAsync<T>(_name, key);
                if (current == null || string.IsNullOrEmpty(etag))
                    throw new InvalidOperationException($"{typeof(T).Name} '{key}' not found.");

                var mutated = mutate(current);

                var committed = await _tableAdapter.TryReplaceAsync(_name, key, mutated!, meta, etag);
                if (!committed)
                    continue; // someone else wrote between our read and write — re-read and retry

                SaveLuceneObject(key, mutated, mapper);

                var change = new ObjectChange { Type = mutated!.GetType(), Key = key, Kind = ChangeKind.Saved, Object = mutated };

                var isRoot = _chainChanges.Value == null;
                var changes = _chainChanges.Value ??= new List<ObjectChange>();
                var errors = _chainErrors.Value ??= new List<Exception>();
                changes.Add(change);

                await RunHandlersAsync(mutated, TriggerKind.Saved, errors, ct);

                if (isRoot)
                {
                    var result = new ObjectResult { Changes = changes, Errors = errors };
                    _chainChanges.Value = null;
                    _chainErrors.Value = null;
                    return result;
                }

                return new ObjectResult { Changes = new[] { change }, Errors = errors };
            }

            throw new InvalidOperationException(
                $"ChangeAsync<{typeof(T).Name}>('{key}') exceeded {maxAttempts} attempts due to concurrent ETag conflicts.");
        }
        finally
        {
            _lazySearcherTask = ReloadSearcherAsync(lazy: true); // best effort async refresh; handlers will see the new index state on their own reloads
        }
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
    /// <param name="maxPerPage">Maximum items per page for the underlying Azure Table Storage query. Defaults to 1000. Set to null for no limit (use with caution).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <summary>
    /// Get many objects from Azure Table Storage with an optional predicate filter.
    /// Returns an <see cref="IAsyncEnumerable{T}"/> supporting polymorphic queries and LINQ joins.
    /// </summary>
    /// <typeparam name="T">The object type. Returns objects of this type and all derived types.</typeparam>
    /// <param name="predicate">Optional filter expression.</param>
    /// <param name="maxPerPage">Maximum items per page for the underlying Azure Table Storage query.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public IAsyncEnumerable<T> GetManyAsync<T>(Expression<Func<T, bool>>? predicate = null,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default) where T : class, new()
    {
        return _tableAdapter.GetManyAsync<T>(_name, predicate, maxPerPage, cancellationToken);
    }

    internal (TableStorageAdapter adapter, string tableName) GetTableForTesting() => (_tableAdapter, _name);

    /// <summary>
    /// Search the Lucene index. Returns an <see cref="IQueryable{T}"/> with full POCO fidelity
    /// (deserialized from stored _json field). Always reflects the last committed state.
    /// </summary>
    /// <typeparam name="T">The object type to search for.</typeparam>
    /// <param name="query">Optional Lucene query string to pre-filter results.</param>
    public IQueryable<T> Search<T>(string? query = null) where T : class, new()
    {
        if (_indexDirty || !_lazySearcherTask.IsCompleted)
        {
            _lazySearcherTask = ReloadSearcherAsync();
            _lazySearcherTask.Wait();
        }

        lock (_lock)
        {
            GetMeta<T>();
            var mapper = GetMapper<T>();
            if (!String.IsNullOrEmpty(query))
            {
                var parser = new FieldMappingQueryParser<T>(_lucene.LuceneVersion, LottaDB.CONTENT_FIELD, mapper);
                return _lucene.AsQueryable<T>(mapper)
                    .Where(parser.Parse(query));
            }

            return _lucene.AsQueryable<T>(mapper);
        }
    }

    /// <summary>Search the Lucene index with a predicate filter.</summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="predicate">Filter expression applied to Lucene results.</param>
    public IQueryable<T> Search<T>(Expression<Func<T, bool>> predicate) where T : class, new()
        => Search<T>().Where(predicate);

    public async Task ReloadSearcherAsync()
    {
        _lazySearcherTask = ReloadSearcherAsync(lazy: false);
        await _lazySearcherTask.ConfigureAwait(false);
    }

    protected virtual async Task ReloadSearcherAsync(bool lazy)
    {
        if (!_autoCommitCancelToken.IsCancellationRequested)
        {
            lock (_lock)
            {
                _autoCommitCancelToken.Cancel();
                _autoCommitCancelToken.Dispose();
                _autoCommitCancelToken = new CancellationTokenSource();
            }
        }

        if (lazy)
        {
            try
            {
                await Task.Delay(_config.AutoCommitDelay, _autoCommitCancelToken.Token).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                return;
            }
        }

        lock (_lock)
        {
            if (!_disposed)
            {
                _indexWriter.Commit();
                _lucene.Refresh();
                _indexDirty = false;
            }
        }
    }

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
    public async Task RebuildSearchIndex(CancellationToken ct = default)
    {
        lock(_lock)
        {
            _indexWriter.DeleteAll();
            _indexDirty = true;
        }
        await SaveManyAsync(await _tableAdapter.GetAllAsync(_name, cancellationToken: ct).ToListAsync());
    }

    /// <summary>
    /// Reset the database: deletes and recreates the table, clears and reinitializes the Lucene index.
    /// </summary>
    public async Task ResetDatabaseAsync(CancellationToken ct = default)
    {
        // 1. Delete and recreate table storage
        await _tableAdapter.ResetTableAsync(_name);

        // 2. Delete all documents from Lucene index
        lock (_lock)
        {
            _indexWriter.DeleteAll();
            _indexWriter.Commit();
            _lucene.Refresh();
            _indexDirty = false;
        }
    }

    /// <summary>
    /// Deletes the table, deletes the index and disposes all resources. Use with caution — this is not reversible.
    /// This object will not be usable after calling this method. Dispose the LottaDB instance when you're done to free resources. 
    /// </summary>
    /// <param name="ct"></param>
    /// <returns></returns>
    public async Task DeleteDatabaseAsync(CancellationToken ct = default)
    {
        await _tableAdapter.DeleteTableAsync(_name, ct);

        lock (_lock)
        {
            _indexWriter.Dispose();
            _indexWriter = null!;
            _lucene.Dispose();
            _lucene = null!;

            // Then delete the directory
            foreach (var file in _directory.ListAll())
                _directory.DeleteFile(file);

            _directory.Dispose();
            _directory = null!;
        }
    }

    // === Lucene session helpers ===

    /// <summary>
    /// Add an entity to the Lucene index. Reuses a shared session if one is active (bulk operations),
    /// otherwise opens and immediately disposes a dedicated session.
    /// </summary>
    private void SaveLuceneObject(string key, object entity, IDocumentMapper mapper)
    {
        lock (_lock)
        {
            DeleteLuceneObject(key);

            var document = new Document();
            mapper.ToDocument(entity, document);
            _indexWriter.AddDocument(document);
            _indexDirty = true;
        }
    }

    /// <summary>
    /// Delete an entity from the Lucene index.
    /// </summary>
    private void DeleteLuceneObject(string key)
    {
        lock (_lock)
        {
            _indexWriter.DeleteDocuments([new Term(KEY_FIELD, key)]);
            _indexDirty = true;
        }
    }

    // === Bulk operations ===

    /// <summary>
    /// Save (upsert) multiple objects in bulk. Table storage writes are batched transactionally
    /// (auto-flushed at 100 ops or on duplicate key). The Lucene IndexSearcher refreshes once
    /// at the end. On&lt;T&gt; handlers run inline and share the Lucene session.
    /// </summary>
    /// <param name="entities">The objects to save.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any handler errors.</returns>
    public async Task<ObjectResult> SaveManyAsync(IEnumerable<object> entities, CancellationToken ct = default)
    {
        var allChanges = new List<ObjectChange>();
        var allErrors = new List<Exception>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new HashSet<string>();
        // Buffer Lucene updates until table batch commits successfully
        var pendingLucene = new List<(string key, object entity, IDocumentMapper mapper)>();

        try
        {
            foreach (var entity in entities)
            {
                ct.ThrowIfCancellationRequested();

                var meta = GetMeta(entity.GetType());
                var key = meta.GetKey(entity);
                var mapper = GetMapper(entity.GetType());
                if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
                    meta.SetKey(entity, key);

                // Auto-flush on duplicate key
                if (pendingKeys.Contains(key))
                {
                    await _tableAdapter.SubmitTransactionAsync(_name, pendingActions);
                    ApplyPendingLuceneSaves(pendingLucene);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                pendingActions.Add(TableStorageAdapter.CreateUpsertAction(key, entity, meta));
                pendingKeys.Add(key);
                pendingLucene.Add((key, entity, mapper));

                // Auto-flush at 100 operations
                if (pendingActions.Count >= 100)
                {
                    await _tableAdapter.SubmitTransactionAsync(_name, pendingActions);
                    ApplyPendingLuceneSaves(pendingLucene);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                var change = new ObjectChange { Type = entity.GetType(), Key = key, Kind = ChangeKind.Saved, Object = entity };
                allChanges.Add(change);

                _chainChanges.Value = allChanges;
                _chainErrors.Value = allErrors;
                await RunHandlersAsync(entity, entity.GetType(), TriggerKind.Saved, allErrors, ct);
            }

            // Flush remaining table storage batch, then apply Lucene
            if (pendingActions.Count > 0)
            {
                await _tableAdapter.SubmitTransactionAsync(_name, pendingActions);
                ApplyPendingLuceneSaves(pendingLucene);
            }
        }
        finally
        {
            _lazySearcherTask = ReloadSearcherAsync(lazy: true);
            _chainChanges.Value = null;
            _chainErrors.Value = null;
        }

        return new ObjectResult { Changes = allChanges, Errors = allErrors };
    }

    private void ApplyPendingLuceneSaves(List<(string key, object entity, IDocumentMapper mapper)> pending)
    {
        foreach (var (key, entity, mapper) in pending)
            SaveLuceneObject(key, entity, mapper);
        pending.Clear();
    }

    public async Task<ObjectResult> DeleteManyAsync<T>(IEnumerable<T> entities, CancellationToken ct = default) where T : class, new()
    {
        var meta = GetMeta<T>();
        var keys = entities.Select(e => meta.GetKey(e));
        return await DeleteManyAsync(keys, ct);
    }

    /// <summary>
    /// Delete multiple objects by key in bulk. Table storage writes are batched transactionally
    /// (auto-flushed at 100 ops or on duplicate key). The Lucene IndexSearcher refreshes once
    /// at the end. On&lt;T&gt; handlers run inline and share the Lucene session.
    /// </summary>
    /// <param name="keys">The keys of the objects to delete.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all deletions and any handler errors.</returns>
    public async Task<ObjectResult> DeleteManyAsync(IEnumerable<string> keys, CancellationToken ct = default)
    {
        var allChanges = new List<ObjectChange>();
        var allErrors = new List<Exception>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new HashSet<string>();
        var pendingLuceneDeletes = new List<string>();

        // Single query to fetch all entities (for handlers) instead of N point reads
        var keyList = keys.ToList();
        var entityLookup = new Dictionary<string, (object obj, Type type)>();
        if (keyList.Count > 0)
        {
            await foreach (var (key, obj, type) in _tableAdapter.GetManyRawAsync(_name, keyList, ct))
                entityLookup[key] = (obj, type);
        }

        try
        {
            foreach (var key in keyList)
            {
                ct.ThrowIfCancellationRequested();

                entityLookup.TryGetValue(key, out var entry);
                var (existing, entityType) = (entry.obj, entry.type);

                // Auto-flush on duplicate key
                if (pendingKeys.Contains(key))
                {
                    await _tableAdapter.SubmitTransactionAsync(_name, pendingActions);
                    ApplyPendingLuceneDeletes(pendingLuceneDeletes);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                pendingActions.Add(TableStorageAdapter.CreateDeleteAction(key));
                pendingKeys.Add(key);
                pendingLuceneDeletes.Add(key);

                if (pendingActions.Count >= 100)
                {
                    await _tableAdapter.SubmitTransactionAsync(_name, pendingActions);
                    ApplyPendingLuceneDeletes(pendingLuceneDeletes);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                var change = new ObjectChange { Type = entityType, Key = key, Kind = ChangeKind.Deleted, Object = existing };
                allChanges.Add(change);

                _chainChanges.Value = allChanges;
                _chainErrors.Value = allErrors;
                if (existing != null && entityType != null)
                    await RunHandlersAsync(existing, entityType, TriggerKind.Deleted, allErrors, ct);
            }

            if (pendingActions.Count > 0)
            {
                await _tableAdapter.SubmitTransactionAsync(_name, pendingActions);
                ApplyPendingLuceneDeletes(pendingLuceneDeletes);
            }
        }
        finally
        {
            _lazySearcherTask = ReloadSearcherAsync(lazy: true);
            _chainChanges.Value = null;
            _chainErrors.Value = null;
        }

        return new ObjectResult { Changes = allChanges, Errors = allErrors };
    }

    private void ApplyPendingLuceneDeletes(List<string> pending)
    {
        foreach (var key in pending)
            DeleteLuceneObject(key);
        pending.Clear();
    }

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

    /// <summary>
    /// Run handlers for an entity whose type is only known at runtime.
    /// Dispatches to the generic RunHandlersAsync&lt;T&gt; via reflection.
    /// </summary>
    private async Task RunHandlersAsync(object entity, Type entityType, TriggerKind kind,
        List<Exception> errors, CancellationToken ct)
    {
        if (!_handlers.ContainsKey(entityType)) return;

        var method = typeof(LottaDB).GetMethods(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            .First(m => m.Name == nameof(RunHandlersAsync) && m.IsGenericMethod)
            .MakeGenericMethod(entityType);

        await (Task)method.Invoke(this, new[] { entity, kind, errors, ct })!;
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
            if (disposing)
            {
                lock (_lock)
                {
                    _autoCommitCancelToken.Cancel();
                    _indexWriter?.Commit();
                    _indexWriter?.Dispose();
                    _lucene?.Dispose();
                    _directory?.Dispose();
                }
            }
        }
    }

    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~LottaDB()
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}

internal class HandlerHandle : IDisposable
{
    private readonly List<object> _list;
    private readonly object _handler;
    public HandlerHandle(List<object> list, object handler) { _list = list; _handler = handler; }
    public void Dispose() { lock (_list) { _list.Remove(_handler); } }
}
