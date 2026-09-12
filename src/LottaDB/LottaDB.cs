using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Lotta.Internal;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Analysis;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Util;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq.Expressions;
using System.Text.Json;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.LuceneVersion;

namespace Lotta;

/// <summary>
/// A LottaDB database instance. Multiple databases can share the same Azure Table
/// (catalog) while maintaining isolation via distinct partition keys (database IDs).
/// Each database has its own Lucene index.
/// </summary>
public class LottaDB : IDisposable
{
    private readonly object _lock = new object();
    internal readonly LottaCatalog _lottaCatalog;
    private readonly string _databaseId;
    private readonly LottaConfiguration _config;
    private readonly TableStorageAdapter _tableAdapter;
    private LuceneDirectory _directory;

    /// <summary>Returns the underlying Lucene directory (for testing/diagnostics).</summary>
    internal LuceneDirectory GetLuceneDirectory() => _directory;
    private ReadOnlyLuceneDataProvider _lucene;
    private long _lastWriteTimestamp;
    private Task? _refreshTask;

    // Whether RefreshLoopAsync is live. Guarded by _lock — see StartRefreshLoopIfNeededLocked.
    private bool _refreshLoopRunning;

    // The Lucene IndexWriter owns the cross-process write lock (write.lock on FSDirectory,
    // a blob lease on AzureDirectory). It is created lazily on the first write and released
    // after WriteLockReleaseDelay of inactivity, so multiple processes can share a database.
    // Null means "this process does not currently hold the writer role".
    private IndexWriter? _indexWriter;

    // Live analyzer shared by the writer and the JsonExpression translator. Held separately
    // from the IndexWriter because schema registration merges into it while no writer exists.
    private readonly PerFieldAnalyzer _perFieldAnalyzer;

    // Lock-free reader used by the untyped Search paths. Refreshed alongside _lucene.
    // Null until the index exists — only reachable on a read-only instance opened before the
    // writer ever created it.
    private Lucene.Net.Search.SearcherManager? _searcherManager;
    private volatile bool _indexAvailable;

    // Serializes writer acquisition and release. Guarantees at most one thread per database
    // is ever blocked inside Lucene's Lock.Obtain; everyone else waits asynchronously here.
    private readonly SemaphoreSlim _writerGate = new(1, 1);

    // Count of in-flight write leases. The writer cannot be released until this drains to 0.
    private int _activeWriters;

    // Reentrancy for handler chains: a nested write inherits the outer lease rather than
    // taking its own (mirrors _heldLocks for the striped key locks).
    private readonly AsyncLocal<int> _writerLeaseDepth = new();

    private long _lastRefreshTimestamp;
    private long _writerAcquiredTimestamp;

    private volatile bool _indexDirty;
    private bool _disposed;
    private readonly CancellationTokenSource _disposeCts = new();

    // Striped per-key locks: ensures table write + Lucene handler execute atomically for a given key.
    // Prevents out-of-order Lucene writes when multiple tasks write to the same key concurrently.
    private const int LockStripeCount = 1024;
    private readonly SemaphoreSlim[] _keyLocks = Enumerable.Range(0, LockStripeCount)
        .Select(_ => new SemaphoreSlim(1, 1)).ToArray();

    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, IDocumentMapper> _mappers = new();
    private readonly ConcurrentDictionary<Type, ImmutableArray<object>> _handlers = new();
    internal readonly ConcurrentDictionary<string, JsonMetadata> _schemas = new();
    private readonly ConcurrentDictionary<string, JsonDocumentMapper> _dynamicMappers = new();
    // Cached MethodInfo for the generic RunHandlersAsync<T> overload — resolved once to avoid per-call reflection
    private static readonly System.Reflection.MethodInfo _runHandlersAsyncMethod =
        typeof(LottaDB).GetMethods(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            .First(m => m.Name == nameof(RunHandlersAsync) && m.IsGenericMethod);
    // Compiled delegate cache: one strongly-typed trampoline per concrete entity type, built once via expression trees.
    // Eliminates both the per-call MakeGenericMethod allocation and the object[] boxing from MethodInfo.Invoke.
    private static readonly ConcurrentDictionary<Type, Func<LottaDB, object, TriggerKind, List<Exception>, CancellationToken, Task>>
        _runHandlersAsyncDelegateCache = new();
    // Cycle detection: tracks object keys being processed in the current call chain
    private static readonly AsyncLocal<HashSet<string>> _processing = new();
    // Collects changes across the entire call chain (root save + handler saves)
    private static readonly AsyncLocal<List<ObjectChange>?> _chainChanges = new();
    private static readonly AsyncLocal<List<Exception>?> _chainErrors = new();

    /// <summary>
    /// Create a LottaDB database instance. Use <see cref="LottaCatalog.GetDatabaseAsync"/> instead of calling this directly.
    /// </summary>
    /// <param name="catalog">The owning catalog (provides storage factories, analyzer, embedding generator).</param>
    /// <param name="databaseId">Database ID within the catalog. Used as the partition key and Lucene index subdirectory.</param>
    /// <param name="config">Per-database configuration (registered types, On&lt;T&gt; handlers).</param>
    internal LottaDB(LottaCatalog catalog, string databaseId, LottaConfiguration config)
    {
        _lottaCatalog = catalog;
        _databaseId = databaseId;
        _config = config;
        _tableAdapter = new TableStorageAdapter(catalog.GetTableServiceClient(), partitionKey: databaseId);
        _directory = catalog.LuceneDirectoryFactory($"{catalog.Name}/{databaseId}/Search");

        // Auto-register JsonSchema if not already registered
        if (!_config.StorageConfigurations.ContainsKey(typeof(JsonSchema)))
        {
            var jsonSchemaConfig = new StorageConfiguration<JsonSchema>();
            _config.StorageConfigurations[typeof(JsonSchema)] = jsonSchemaConfig;
        }

        InitializeMetadata();
        InitializeMappers();
        InitializeHandlers();
        InitializeLuceneHandlers();

        // Build a per-field analyzer that merges all mapper analyzers.
        // Default is KeywordAnalyzer (matching DocumentMapperBase) so unregistered
        // fields like _key_ are stored verbatim. Per-type field analyzers are merged below.
        _perFieldAnalyzer = new PerFieldAnalyzer(new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        _perFieldAnalyzer.AddAnalyzer(Internal.StorageFields.Key, new Lucene.Net.Analysis.Core.KeywordAnalyzer());
        foreach (var mapper in _mappers.Values)
            _perFieldAnalyzer.Merge(mapper.Analyzer);
        foreach (var mapper in _dynamicMappers.Values)
            _perFieldAnalyzer.Merge(mapper.Analyzer);

        // No IndexWriter here — the cross-process write lock is taken lazily on the first
        // write (see AcquireWriterAsync) so read-only processes never contend for it.
        // The index must still have at least one commit before any reader opens it, or
        // Lucene.Net.Linq's Context.CreateSearcher falls back to creating a temporary
        // IndexWriter of its own — taking the write lock from inside a read path.
        // A read-only instance never creates the index, so it may legitimately open before the
        // writer has created one. In that case searches serve empty results until it appears.
        if (EnsureIndexBootstrapped())
            OpenSearcherManager();

        _lucene = new ReadOnlyLuceneDataProvider(_directory, LuceneVersion.LUCENE_48);
        if (catalog.EmbeddingGenerator != null)
            _lucene.Settings.EmbeddingGenerator = catalog.EmbeddingGenerator;
        _lucene.MapperFactory = (type, version, analyzer) =>
        {
            var mapperType = typeof(TypeDocumentMapper<>).MakeGenericType(type);
            _metadata.TryGetValue(type, out var meta);
            return Activator.CreateInstance(mapperType, version, catalog.Analyzer, meta, catalog.EmbeddingGenerator, this)!;
        };
    }

    // === Cross-process write lock ===

    /// <summary>
    /// True if this instance currently owns the cross-process Lucene write lock.
    /// </summary>
    public bool HoldsWriteLock => Volatile.Read(ref _indexWriter) != null;

    /// <summary>
    /// The IndexWriter this call is already covered by. Index-mutating code always runs inside
    /// a <see cref="WriterLease"/> taken by the enclosing write path, so a null writer here is
    /// a bug — failing loudly beats silently dropping an index update.
    /// </summary>
    private IndexWriter RequireWriter() =>
        _indexWriter ?? throw new InvalidOperationException(
            $"Lucene index was modified for database '{_databaseId}' without holding a writer lease. " +
            "This is a bug in LottaDB — the write path should have called AcquireWriterAsync first.");

    private IndexWriterConfig NewWriterConfig() =>
        new IndexWriterConfig(LuceneVersion.LUCENE_48, _perFieldAnalyzer)
        {
            OpenMode = OpenMode.CREATE_OR_APPEND,
            UseCompoundFile = true,
            WriteLockTimeout = _config.WriteLockTimeout,
        };

    /// <summary>
    /// Lay down an empty commit if the index does not exist yet, so that readers can open it
    /// without taking the write lock. Costs one directory listing for every database that has
    /// ever been written to; only the very first process to touch a brand-new database creates
    /// the index, and it releases the lock immediately.
    /// Returns whether the index exists (and so can be opened for reading).
    /// </summary>
    private bool EnsureIndexBootstrapped()
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            if (DirectoryReader.IndexExists(_directory))
                return true;

            // A read-only instance creates nothing — not even an empty index.
            if (_config.ReadOnly)
                return false;

            try
            {
                using var bootstrap = new IndexWriter(_directory, NewWriterConfig());
                bootstrap.Commit();
                return true;
            }
            catch (Lucene.Net.Store.LockObtainFailedException)
            {
                // Another process is creating the index right now — re-check IndexExists.
            }
        }

        if (DirectoryReader.IndexExists(_directory))
            return true;

        throw new WriteLockUnavailableException(_databaseId);
    }

    private void OpenSearcherManager()
    {
        _searcherManager = new Lucene.Net.Search.SearcherManager(_directory, null);
        _lastRefreshTimestamp = Stopwatch.GetTimestamp();
        _indexAvailable = true;
    }

    /// <summary>The open searcher. Only valid after <see cref="IndexAvailable"/> returns true.</summary>
    private Lucene.Net.Search.SearcherManager Searchers =>
        _searcherManager ?? throw new InvalidOperationException(
            $"Lucene index for database '{_databaseId}' is not open. This is a bug — callers must check IndexAvailable() first.");

    /// <summary>
    /// True once the Lucene index exists and the searcher is open. Only a read-only instance
    /// opened before the writer ever created the index can see false, and it latches true as
    /// soon as the index appears — so the directory check costs nothing in the normal case.
    /// </summary>
    private bool IndexAvailable()
    {
        if (_indexAvailable) return true;

        lock (_lock)
        {
            if (_indexAvailable) return true;
            if (!DirectoryReader.IndexExists(_directory)) return false;
            OpenSearcherManager();
            return true;
        }
    }

    /// <summary>
    /// Take a write lease, creating the IndexWriter (and acquiring the cross-process write
    /// lock) if this process does not already hold it. Must be called before any storage
    /// mutation so that a lock failure leaves nothing half-written.
    /// </summary>
    private async ValueTask<WriterLease> AcquireWriterAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_config.ReadOnly)
            throw new InvalidOperationException(
                $"Database '{_databaseId}' was opened read-only (ILottaConfiguration.ReadOnly). Writes are not permitted.");

        // Reentrant call from an On<T> handler chain — the outer lease already covers us.
        if (_writerLeaseDepth.Value > 0)
        {
            _writerLeaseDepth.Value++;
            return new WriterLease(null);
        }

        await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_indexWriter == null)
            {
                IndexWriter writer;
                try
                {
                    writer = new IndexWriter(_directory, NewWriterConfig());
                }
                catch (Lucene.Net.Store.LockObtainFailedException ex)
                {
                    throw new WriteLockUnavailableException(_databaseId, ex);
                }
                lock (_lock)
                {
                    _indexWriter = writer;
                }
                _writerAcquiredTimestamp = Stopwatch.GetTimestamp();
            }
            Interlocked.Increment(ref _activeWriters);
        }
        finally
        {
            _writerGate.Release();
        }

        _writerLeaseDepth.Value = 1;
        return new WriterLease(this);
    }

    /// <summary>
    /// Commit pending changes, close the IndexWriter and release the cross-process write lock.
    /// Waits for in-flight writes to drain, holding the gate so no new lease can appear.
    /// Returns false if the drain did not complete within <paramref name="drainTimeout"/>.
    /// </summary>
    private async Task<bool> TryReleaseWriterAsync(TimeSpan drainTimeout, CancellationToken cancellationToken)
    {
        await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Timeout.InfiniteTimeSpan is -1ms, so a naive elapsed > drainTimeout comparison is
            // true on the very first check and would abandon the release while reporting success.
            var unbounded = drainTimeout < TimeSpan.Zero;

            var start = Stopwatch.GetTimestamp();
            while (Volatile.Read(ref _activeWriters) > 0)
            {
                if (!unbounded && Stopwatch.GetElapsedTime(start) > drainTimeout)
                    return false;
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }

            lock (_lock)
            {
                if (_indexWriter != null)
                {
                    if (_indexDirty)
                        _indexWriter.Commit();
                    _indexWriter.Dispose();   // releases write.lock / blob lease
                    _indexWriter = null;
                    _indexDirty = false;
                }
                RefreshReadersLocked();
            }
            return true;
        }
        finally
        {
            _writerGate.Release();
        }
    }

    /// <summary>
    /// Commit any pending index changes, close the Lucene IndexWriter and release the
    /// cross-process write lock so another process can take over the writer role.
    /// Reads and searches are unaffected, and the writer is transparently re-acquired on the
    /// next write. Safe to call when the lock is not held (no-op).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public Task ReleaseWriteLockAsync(CancellationToken cancellationToken = default)
        => TryReleaseWriterAsync(Timeout.InfiniteTimeSpan, cancellationToken);

    /// <summary>
    /// Scope holding one in-flight write lease. A null database means the lease was reentrant
    /// (an On&lt;T&gt; handler chain) and the outer scope owns the real lease.
    /// </summary>
    internal readonly struct WriterLease : IDisposable
    {
        private readonly LottaDB? _db;

        internal WriterLease(LottaDB? db) => _db = db;

        public void Dispose()
        {
            if (_db == null) return;
            Interlocked.Decrement(ref _db._activeWriters);
            _db._writerLeaseDepth.Value = 0;
            // The lease ending is the last write activity, and it is the only signal every
            // write path shares — maintenance operations like RebuildSearchIndex never run a
            // Lucene handler, so arming the release here is what stops them holding the
            // cross-process lock forever.
            _db.ScheduleWriterRelease();
        }
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

    private void InitializeMetadata()
    {
        foreach (var (type, configObj) in _config.StorageConfigurations)
        {
            var m = typeof(TypeMetadata).GetMethod(nameof(TypeMetadata.Build))!.MakeGenericMethod(type);
            _metadata[type] = (TypeMetadata)m.Invoke(null, new object[] { configObj, _config.AutoKeyProperties })!;
        }

        // Note: JsonDocument is NOT registered in _metadata — it goes through the
        // schema-aware path (JsonDocumentMapper) instead of the POCO path (TypeDocumentMapper).
    }

    private void InitializeHandlers()
    {
        foreach (var reg in _config.OnRegistrations)
        {
            _handlers.AddOrUpdate(reg.ObjectType,
                _ => ImmutableArray.Create(reg.Handler),
                (_, existing) => existing.Add(reg.Handler));
        }

        // Built-in handler: index JsonDocument in Lucene via the schema-aware mapper
        var jsonDocHandler = (EntityHandler<JsonDocument>)((doc, kind, db, cancellationToken) =>
        {
            var schemaName = doc.GetSchema() ?? Internal.StorageFields.DefaultSchema;
            var key = doc.GetKey()!;
            lock (_lock)
            {
                var writer = RequireWriter();
                writer.DeleteDocuments([new Term(Internal.StorageFields.Key, key)]);
                if (kind == TriggerKind.Saved && _dynamicMappers.TryGetValue(schemaName, out var mapper))
                {
                    var document = new Document();
                    mapper.ToDocument(doc, document);
                    var etag = doc.GetETag() ?? "";
                    Internal.EntityMapper.AddMetadataToLuceneDocument(document, typeof(JsonDocument).FullName!, etag, schemaName);
                    writer.AddDocument(document);
                }
                _indexDirty = true;
            }
            ScheduleRefresh();
            return Task.CompletedTask;
        });
        _handlers.AddOrUpdate(typeof(JsonDocument),
            _ => ImmutableArray.Create<object>(jsonDocHandler),
            (_, existing) => existing.Add(jsonDocHandler));

        // Built-in handler: when a JsonSchema is saved/deleted, update the dynamic mappers
        var jsonSchemaHandler = (EntityHandler<JsonSchema>)(async (schema, kind, db, cancellationToken) =>
        {
            if (kind == TriggerKind.Saved)
            {
                var newDynamic = JsonMetadata.Parse(schema);
                var oldDynamic = _schemas.GetValueOrDefault(schema.Name);
                RegisterJsonMetadata(schema);

                // Reindex if schema changed (properties differ)
                if (oldDynamic != null)
                {
                    var oldHash = JsonMetadata.ComputeHash(oldDynamic);
                    var newHash = JsonMetadata.ComputeHash(newDynamic);
                    if (oldHash != newHash)
                    {
                        await ReindexJsonMetadataAsync(schema.Name, cancellationToken);
                    }
                }
            }
            else if (kind == TriggerKind.Deleted)
            {
                _schemas.TryRemove(schema.Name, out _);
                _dynamicMappers.TryRemove(schema.Name, out _);
                // Delete Lucene documents for this schema
                lock (_lock)
                {
                    RequireWriter().DeleteDocuments(new Term(Internal.StorageFields.Schema, schema.Name));
                    _indexDirty = true;
                }
                ScheduleRefresh();
            }
        });
        _handlers.AddOrUpdate(typeof(JsonSchema),
            _ => ImmutableArray.Create<object>(jsonSchemaHandler),
            (_, existing) => existing.Add(jsonSchemaHandler));
    }

    // ===== LUCENE HANDLER ===
    private void InitializeLuceneHandlers()
    {
        var method = typeof(LottaDB).GetMethod(nameof(RegisterLuceneHandler),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        foreach (var type in _metadata.Keys)
            method.MakeGenericMethod(type).Invoke(this, null);
    }

    private void RegisterLuceneHandler<T>() where T : class, new()
    {
        var luceneHandler = (EntityHandler<T>)((entity, kind, db, cancellationToken) =>
        {
            var meta = GetMeta<T>();
            var key = meta.GetKey(entity);
            lock (_lock)
            {
                var writer = RequireWriter();
                writer.DeleteDocuments([new Term(Internal.StorageFields.Key, key)]);
                if (kind == TriggerKind.Saved)
                {
                    var mapper = GetMapper<T>();
                    var document = new Document();
                    mapper.ToDocument(entity, document);
                    var etag = entity.GetETag()
                        ?? throw new InvalidOperationException(
                            $"Cannot index {typeof(T).Name} '{key}': entity has no ETag. This is a bug — SetETag should have been called before the Lucene handler.");
                    Internal.EntityMapper.AddMetadataToLuceneDocument(document, entity.GetType().FullName!, etag, typeof(T).Name);
                    writer.AddDocument(document);
                }
                _indexDirty = true;
                ScheduleRefresh();
            }
            return Task.CompletedTask;
        });
        _handlers.AddOrUpdate(typeof(T),
            _ => ImmutableArray.Create<object>(luceneHandler),
            (_, existing) => existing.Add(luceneHandler));
    }

    /// <summary>
    /// Rebuild the entire Lucene index from Azure Table Storage.
    /// Re-indexes all registered types. Does not run On&lt;T&gt; handlers.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task RebuildSearchIndex(CancellationToken cancellationToken = default)
    {
        using var lease = await AcquireWriterAsync(cancellationToken);

        lock (_lock)
        {
            RequireWriter().DeleteAll();
            _indexDirty = true;
        }

        // Single pass over all rows — dispatch to typed or dynamic mapper based on Type column
        await foreach (var tableEntity in _tableAdapter.GetAllRawAsync(_lottaCatalog.Name, cancellationToken: cancellationToken))
        {
            var typeName = tableEntity.GetString(Internal.StorageFields.Type);
            if (typeName == null) continue;

            var document = new Document();

            if (JsonMetadata.IsJsonTypeName(typeName))
            {
                // JSON document — look up schema by Schema column
                var schemaName = tableEntity.TryGetValue(Internal.StorageFields.Schema, out var rebuildSchemaObj) && rebuildSchemaObj is string rebuildSchemaStr
                    ? rebuildSchemaStr : Internal.StorageFields.DefaultSchema;
                if (!_dynamicMappers.TryGetValue(schemaName, out var dynamicMapper)) continue;
                var bytes = tableEntity.GetObjectBytes();
                if (bytes.Length == 0) continue;
                var jsonDoc = System.Text.Json.JsonDocument.Parse(bytes);
                jsonDoc.SetKey(TableStorageAdapter.DecodeKey(tableEntity.RowKey));
                dynamicMapper.ToDocument(jsonDoc, document);
            }
            else
            {
                // Typed entity — deserialize via the standard path
                var entity = TableStorageAdapter.DeserializeEntity(tableEntity);
                if (entity == null || !_metadata.ContainsKey(entity.GetType())) continue;
                var mapper = GetMapper(entity.GetType());
                mapper.ToDocument(entity, document);
            }

            // Store metadata (ETag + Schema)
            var rebuildTypeName = tableEntity.GetString(Internal.StorageFields.Type) ?? "";
            var schemaFallback = JsonMetadata.IsJsonTypeName(rebuildTypeName)
                ? Internal.StorageFields.DefaultSchema
                : rebuildTypeName;
            var rebuildSchema = tableEntity.TryGetValue(Internal.StorageFields.Schema, out var schemaObj) && schemaObj is string schemaStr
                ? schemaStr : schemaFallback;
            Internal.EntityMapper.AddMetadataToLuceneDocument(document, rebuildTypeName, tableEntity.ETag.ToString(), rebuildSchema);

            lock (_lock)
            {
                RequireWriter().AddDocument(document);
                _indexDirty = true;
            }
        }

        ReloadSearcher();
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

    private Lucene.Net.Linq.Mapping.IDocumentMapper<T> GetMapper<T>() where T : class, new()
    {
        return (Lucene.Net.Linq.Mapping.IDocumentMapper<T>)_mappers.GetOrAdd(typeof(T), _ =>
        {
            _metadata.TryGetValue(typeof(T), out var meta);
            return new TypeDocumentMapper<T>(Version.LUCENE_48, _lottaCatalog.Analyzer, meta, _lottaCatalog.EmbeddingGenerator, this);
        });
    }

    private IDocumentMapper GetMapper(Type type)
    {
        return _mappers.GetOrAdd(type, static (t, state) =>
        {
            var db = (LottaDB)state!;
            db._metadata.TryGetValue(t, out var meta);
            var mapperType = typeof(TypeDocumentMapper<>).MakeGenericType(t);
            return (IDocumentMapper)Activator.CreateInstance(mapperType, Version.LUCENE_48, db._lottaCatalog.Analyzer, meta, db._lottaCatalog.EmbeddingGenerator, db)!;
        }, this);
    }

    // === Write ===

    /// <summary>
    /// Save an object. Key extracted from [Key] attribute.
    /// If the object has an ETag (from a previous read), performs a conditional write
    /// that throws <see cref="ConcurrencyException"/> on conflict. Otherwise, performs an unconditional upsert.
    /// Writes to table storage, then runs On&lt;T&gt; handlers (including Lucene indexing).
    /// </summary>
    /// <param name="entity">The object to save.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any handler errors.</returns>
    public async Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken cancellationToken = default) where T : class
    {
        // Compute key before acquiring lock
        string key;
        if (entity is JsonDocument jsonDoc0)
        {
            var schema0 = GetSchema(ResolveSchemaName(jsonDoc0));
            key = schema0.GetKey(jsonDoc0);
        }
        else
        {
            var meta0 = GetMeta(entity.GetType());
            key = meta0.GetKey(entity);
            if (meta0.KeyMode == KeyMode.Auto && meta0.SetKey != null)
                meta0.SetKey(entity, key);
        }

        // Writer lease first: acquiring the cross-process write lock before the table write
        // means a lock failure leaves nothing half-written, and serializes the
        // [table write -> lucene index] sequence across processes.
        using var writerLease = await AcquireWriterAsync(cancellationToken);

        // Per-key lock: table write + Lucene handler are atomic for this key
        using var keyLock = await AcquireLockAsync(key, cancellationToken);
        {
            string newETag;

            if (entity is JsonDocument jsonDoc)
            {
                var schemaName = ResolveSchemaName(jsonDoc);
                var schema = GetSchema(schemaName);

                var existingETag = jsonDoc.GetETag();
                if (existingETag != null)
                {
                    try
                    {
                        newETag = await _tableAdapter.ReplaceJsonDocumentAsync(_lottaCatalog.Name, key, schemaName, jsonDoc, schema, existingETag, cancellationToken);
                    }
                    catch (Azure.RequestFailedException ex) when (ex.Status == 412)
                    {
                        throw new ConcurrencyException(key, typeof(JsonDocument));
                    }
                }
                else
                {
                    newETag = await _tableAdapter.UpsertJsonDocumentAsync(_lottaCatalog.Name, key, schemaName, jsonDoc, schema, cancellationToken);
                }
                Internal.EntityMapper.AnnotateAfterWrite(jsonDoc, key, newETag, schemaName);
            }
            else
            {
                var meta = GetMeta(entity.GetType());

                var existingETag = entity.GetETag();
                if (existingETag != null)
                {
                    try
                    {
                        newETag = await _tableAdapter.ReplaceAsync(_lottaCatalog.Name, key, entity, meta, existingETag, cancellationToken);
                    }
                    catch (Azure.RequestFailedException ex) when (ex.Status == 412)
                    {
                        throw new ConcurrencyException(key, entity.GetType());
                    }
                }
                else
                {
                    newETag = await _tableAdapter.UpsertAsync(_lottaCatalog.Name, key, entity, meta, cancellationToken);
                }
                Internal.EntityMapper.AnnotateAfterWrite(entity, key, newETag);
            }

            if (entity is BlobFile bf) bf.Database = this;

            var change = new ObjectChange { Type = entity.GetType(), Key = key, Kind = ChangeKind.Saved, Object = entity };

            var isRoot = _chainChanges.Value == null;
            var changes = _chainChanges.Value ??= new List<ObjectChange>();
            var errors = _chainErrors.Value ??= new List<Exception>();
            changes.Add(change);

            await RunHandlersAsync(entity, entity.GetType(), TriggerKind.Saved, errors, cancellationToken);

            if (isRoot)
            {
                var result = new ObjectResult { Changes = changes, Errors = errors };
                _chainChanges.Value = null;
                _chainErrors.Value = null;
                return result;
            }

            return new ObjectResult { Changes = new[] { change }, Errors = errors };
        }
        // keyLock released by using/Dispose
    }

    /// <summary>
    /// Delete an object by key. Removes from table storage, then runs On&lt;T&gt; handlers (including Lucene cleanup).
    /// </summary>
    /// <param name="key">The unique key of the object to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken cancellationToken = default) where T : class, new()
    {
        var (existing, _) = await _tableAdapter.GetAsync<T>(_lottaCatalog.Name, key, cancellationToken: cancellationToken);

        return await DeleteAsync<T>(existing!, cancellationToken);
    }

    /// <summary>Delete an object. Key extracted from [Key] attribute.</summary>
    /// <param name="entity">The object to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken cancellationToken = default) where T : class, new()
    {
        if (entity == null)
        {
            return new ObjectResult();
        }
        var meta = GetMeta<T>();
        var key = meta.GetKey(entity);

        using var writerLease = await AcquireWriterAsync(cancellationToken);
        using var keyLock = await AcquireLockAsync(key, cancellationToken);
        {
            await _tableAdapter.DeleteAsync(_lottaCatalog.Name, key, cancellationToken);

            var change = new ObjectChange { Type = typeof(T), Key = key, Kind = ChangeKind.Deleted, Object = entity };

            var isRoot = _chainChanges.Value == null;
            var changes = _chainChanges.Value ??= new List<ObjectChange>();
            var errors = _chainErrors.Value ??= new List<Exception>();
            changes.Add(change);

            await RunHandlersAsync(entity, TriggerKind.Deleted, errors, cancellationToken);

            if (isRoot)
            {
                var result = new ObjectResult { Changes = changes, Errors = errors };
                _chainChanges.Value = null;
                _chainErrors.Value = null;
                return result;
            }

            return new ObjectResult { Changes = new[] { change }, Errors = errors };
        }
        // keyLock released by using/Dispose
    }

    /// <summary>
    /// Delete any object by key, regardless of type. Removes from table storage and Lucene index.
    /// Runs On&lt;T&gt; handlers if the entity type is registered.
    /// </summary>
    /// <param name="key">The unique key of the object to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing the deletion and any handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        using var writerLease = await AcquireWriterAsync(cancellationToken);
        using var keyLock = await AcquireLockAsync(key, cancellationToken);
        {
            // Fetch to discover type and run handlers
            var (existing, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken);

            await _tableAdapter.DeleteAsync(_lottaCatalog.Name, key, cancellationToken);

            lock (_lock)
            {
                RequireWriter().DeleteDocuments([new Term(Internal.StorageFields.Key, key)]);
            }
            ScheduleRefresh();

            var entityType = existing?.GetType() ?? typeof(object);
            var change = new ObjectChange { Type = entityType, Key = key, Kind = ChangeKind.Deleted, Object = existing };

            var isRoot = _chainChanges.Value == null;
            var changes = _chainChanges.Value ??= new List<ObjectChange>();
            var errors = _chainErrors.Value ??= new List<Exception>();
            changes.Add(change);

            if (existing != null)
            {
                await RunHandlersAsync(existing, entityType, TriggerKind.Deleted, errors, cancellationToken);
            }

            if (isRoot)
            {
                var result = new ObjectResult { Changes = changes, Errors = errors };
                _chainChanges.Value = null;
                _chainErrors.Value = null;
                return result;
            }

            return new ObjectResult { Changes = new[] { change }, Errors = errors };
        }
        // keyLock released by using/Dispose
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
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> from the save operation.</returns>
    public Task<ObjectResult> ChangeAsync<T>(string key, Action<T> mutate, CancellationToken cancellationToken = default) where T : class, new()
        => ChangeAsync<T>(key, entity =>
        {
            mutate(entity);
            return entity;
        }, cancellationToken);

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
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> from the save operation.</returns>
    public async Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken cancellationToken = default) where T : class, new()
    {
        const int maxAttempts = 50;
        var meta = GetMeta<T>();

        // One lease for the whole retry loop. The 412 retries come from in-process writers,
        // which the cross-process lock does not exclude, so they remain necessary — but
        // re-acquiring the writer on every attempt would be pure thrash.
        using var writerLease = await AcquireWriterAsync(cancellationToken);

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (current, etag) = await _tableAdapter.GetAsync<T>(_lottaCatalog.Name, key, cancellationToken: cancellationToken);
            if (current == null || string.IsNullOrEmpty(etag))
                throw new InvalidOperationException($"{typeof(T).Name} '{key}' not found.");

            var mutated = mutate(current);

            // Per-key lock: table write + Lucene handler are atomic for this key
            using var keyLock = await AcquireLockAsync(key, cancellationToken);
            {
                string newETag;
                try
                {
                    newETag = await _tableAdapter.ReplaceAsync(_lottaCatalog.Name, key, mutated!, meta, etag, cancellationToken: cancellationToken);
                }
                catch (Azure.RequestFailedException ex) when (ex.Status == 412)
                {
                    continue; // someone else wrote between our read and write — re-read and retry
                }

                // Annotate the mutated entity so the Lucene handler picks up the ETag
                Internal.EntityMapper.AnnotateAfterWrite(mutated!, key, newETag);

                var change = new ObjectChange { Type = mutated!.GetType(), Key = key, Kind = ChangeKind.Saved, Object = mutated };

                var isRoot = _chainChanges.Value == null;
                var changes = _chainChanges.Value ??= new List<ObjectChange>();
                var errors = _chainErrors.Value ??= new List<Exception>();
                changes.Add(change);

                await RunHandlersAsync(mutated, TriggerKind.Saved, errors, cancellationToken);

                if (isRoot)
                {
                    var result = new ObjectResult { Changes = changes, Errors = errors };
                    _chainChanges.Value = null;
                    _chainErrors.Value = null;
                    return result;
                }

                return new ObjectResult { Changes = new[] { change }, Errors = errors };
            }
        }

        throw new InvalidOperationException(
            $"ChangeAsync<{typeof(T).Name}>('{key}') exceeded {maxAttempts} attempts due to concurrent ETag conflicts. Last etag seen: unknown");
    }

    // === Read ===

    /// <summary>Point-read an object by key from Azure Table Storage.</summary>
    /// <param name="key">The unique key of the object.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The object, or null if not found.</returns>
    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class, new()
    {
        var (result, etag) = await _tableAdapter.GetAsync<T>(_lottaCatalog.Name, key, cancellationToken: cancellationToken);
        if (result != null)
        {
            if (etag != null) result.SetETag(etag);
            result.SetKey(key);
            result.SetSchema(result.GetSchema() ?? result.GetType().Name);
            if (result is BlobFile bf) bf.Database = this;
        }
        return result;
    }

    /// <summary>
    /// Get many objects from Azure Table Storage with an optional predicate filter.
    /// Returns an <see cref="IAsyncEnumerable{T}"/> supporting polymorphic queries.
    /// </summary>
    /// <typeparam name="T">The object type. Returns objects of this type and all derived types.</typeparam>
    /// <param name="predicate">Optional filter expression.</param>
    /// <param name="maxPerPage">Maximum items per page for the underlying Azure Table Storage query.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async IAsyncEnumerable<T> GetManyAsync<T>(Expression<Func<T, bool>>? predicate = null,
        int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class, new()
    {
        await foreach (var item in _tableAdapter.GetManyAsync<T>(_lottaCatalog.Name, predicate, maxPerPage, cancellationToken))
        {
            var meta = GetMeta(item.GetType());
            var itemKey = meta.GetKey(item);
            item.SetKey(itemKey);
            item.SetSchema(item.GetType().Name);
            if (item is BlobFile bf) bf.Database = this;
            yield return item;
        }
    }

    internal (TableStorageAdapter adapter, string tableName) GetTableForTesting() => (_tableAdapter, _lottaCatalog.Name);

    /// <summary>
    /// Search the Lucene index. Returns an <see cref="IQueryable{T}"/> with full POCO fidelity
    /// (deserialized from stored _json field). Always reflects the last committed state.
    /// </summary>
    /// <typeparam name="T">The object type to search for.</typeparam>
    /// <param name="query">Optional Lucene query string to pre-filter results.</param>
    public IQueryable<T> Search<T>(string? query = null) where T : class, new()
    {
        // A read-only replica can open before the writer has ever created the index.
        if (!IndexAvailable())
            return Enumerable.Empty<T>().AsQueryable();

        // Search<object>() → untyped search across all types using ObjectDocumentMapper
        if (typeof(T) == typeof(object))
        {
            MaybeReloadSearcher();

            Lucene.Net.Search.Query luceneQuery;
            if (!String.IsNullOrEmpty(query))
            {
                var parser = new Lucene.Net.QueryParsers.Classic.QueryParser(
                    Lucene.Net.Util.LuceneVersion.LUCENE_48, Internal.StorageFields.Content, _lottaCatalog.Analyzer);
                parser.AllowLeadingWildcard = true;
                luceneQuery = parser.Parse(query);
            }
            else
            {
                luceneQuery = new Lucene.Net.Search.MatchAllDocsQuery();
            }

            // Read through the SearcherManager rather than the IndexWriter — this path must
            // work on a process that does not hold the write lock.
            var searcher = Searchers.Acquire();
            try
            {
                var hits = searcher.Search(luceneQuery, int.MaxValue);
                var results = new List<T>();
                foreach (var hit in hits.ScoreDocs)
                {
                    var doc = searcher.Doc(hit.Doc);
                    var obj = Internal.EntityMapper.FromLuceneDocument(doc, this);
                    results.Add((T)obj);
                }
                return results.AsQueryable();
            }
            finally
            {
                Searchers.Release(searcher);
            }
        }

        MaybeReloadSearcher();

        lock (_lock)
        {
            GetMeta<T>();
            var mapper = GetMapper<T>();
            if (!String.IsNullOrEmpty(query))
            {
                var parser = new FieldMappingQueryParser<T>(_lucene.LuceneVersion, mapper.DefaultSearchProperty, mapper);
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

    /// <summary>
    /// Search JSON documents using a <see cref="JsonExpression"/> predicate.
    /// <para><c>db.Search(j =&gt; j["name"] == "alice" &amp;&amp; j["age"] &gt; 20);</c></para>
    /// </summary>
    public IEnumerable<JsonDocument> Search(Expression<Func<JsonExpression, bool>> predicate)
    {
        // A read-only replica can open before the writer has ever created the index.
        if (!IndexAvailable())
            return Enumerable.Empty<JsonDocument>();

        MaybeReloadSearcher();

        Lucene.Net.Search.Query luceneQuery;
        lock (_lock)
        {
            // Under _lock because RegisterJsonMetadata can merge into _perFieldAnalyzer concurrently.
            luceneQuery = Internal.JsonExpressionLuceneVisitor.Translate(predicate, _perFieldAnalyzer);
        }

        // Also filter to only JsonDocument types
        var bq = new Lucene.Net.Search.BooleanQuery();
        bq.Add(luceneQuery, Lucene.Net.Search.Occur.MUST);
        bq.Add(new Lucene.Net.Search.TermQuery(new Lucene.Net.Index.Term(Internal.StorageFields.Type, typeof(JsonDocument).FullName!)),
            Lucene.Net.Search.Occur.MUST);

        var searcher = Searchers.Acquire();
        try
        {
            var hits = searcher.Search(bq, int.MaxValue);
            var results = new List<JsonDocument>();
            foreach (var hit in hits.ScoreDocs)
            {
                var doc = searcher.Doc(hit.Doc);
                var obj = Internal.EntityMapper.FromLuceneDocument(doc, this);
                if (obj is JsonDocument jsonDoc)
                    results.Add(jsonDoc);
            }
            return results;
        }
        finally
        {
            Searchers.Release(searcher);
        }
    }

    /// <summary>
    /// Query JSON documents from Table Storage using a <see cref="JsonExpression"/> predicate.
    /// <para><c>db.GetManyAsync(j =&gt; j.GetSchema() == "Person" &amp;&amp; j["age"] &gt; 20);</c></para>
    /// </summary>
    public async IAsyncEnumerable<JsonDocument> GetManyAsync(Expression<Func<JsonExpression, bool>> predicate,
        int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var odataFilter = Internal.JsonExpressionODataVisitor.Translate(predicate);
        var combinedFilter = $"PartitionKey eq '{_tableAdapter.PartitionKey}' and {Internal.StorageFields.Type} eq '{typeof(JsonDocument).FullName}' and ({odataFilter})";

        await foreach (var doc in _tableAdapter.QueryJsonDocumentsRawAsync(_lottaCatalog.Name, combinedFilter, maxPerPage, cancellationToken))
        {
            yield return doc;
        }
    }

    /// <summary>
    /// Delete JSON documents matching a <see cref="JsonExpression"/> predicate.
    /// Finds matching docs via Table Storage query, then deletes each one.
    /// <para><c>db.DeleteManyAsync(j =&gt; j.GetSchema() == "Person" &amp;&amp; j["age"] &lt; 18);</c></para>
    /// </summary>
    public async Task<ObjectResult> DeleteManyAsync(Expression<Func<JsonExpression, bool>> predicate, CancellationToken cancellationToken = default)
    {
        var allChanges = new List<ObjectChange>();

        using var writerLease = await AcquireWriterAsync(cancellationToken);

        await foreach (var doc in GetManyAsync(predicate, cancellationToken: cancellationToken))
        {
            var key = doc.GetKey();
            if (key == null) continue;

            var schemaName = doc.GetSchema() ?? Internal.StorageFields.DefaultSchema;
            await _tableAdapter.DeleteAsync(_lottaCatalog.Name, key, cancellationToken);

            lock (_lock)
            {
                RequireWriter().DeleteDocuments([new Term(Internal.StorageFields.Key, key)]);
            }
            ScheduleRefresh();

            allChanges.Add(new ObjectChange { Type = typeof(JsonDocument), Key = key, Kind = ChangeKind.Deleted, Object = doc });
        }

        return new ObjectResult { Changes = allChanges };
    }

    /// <summary>
    /// Get all entities from Table Storage regardless of type.
    /// Returns POCOs and JsonDocuments together.
    /// </summary>
    public async IAsyncEnumerable<object> GetManyAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var entity in _tableAdapter.GetAllAsync(_lottaCatalog.Name, cancellationToken: cancellationToken))
        {
            if (entity is BlobFile bf) bf.Database = this;
            if (entity.GetSchema() == null)
                entity.SetSchema(entity.GetType().Name);
            yield return entity;
        }
    }

    // === Untyped API ===

    /// <summary>
    /// Get any object by key, regardless of type. Returns a typed CLR object or JsonDocument.
    /// Key uniqueness across types is the caller's responsibility.
    /// </summary>
    public async Task<object?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        // DeserializeEntity sets Key, ETag, Schema for both POCOs and JsonDocuments
        var (result, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken);
        if (result == null) return null;
        if (result is BlobFile bf) bf.Database = this;
        return result;
    }

    /// <summary>
    /// Loads all JsonSchema entities from Table Storage and registers their dynamic mappers.
    /// Called by LottaCatalog.GetDatabaseAsync after construction.
    /// </summary>
    internal async Task InitializeJsonSchemasAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var schema in _tableAdapter.GetManyAsync<JsonSchema>(_lottaCatalog.Name, cancellationToken: cancellationToken))
        {
            RegisterJsonMetadata(schema);
        }

        // Register the default schema in memory for schemaless JsonDocument storage
        if (!_schemas.ContainsKey(Internal.StorageFields.DefaultSchema))
        {
            RegisterJsonMetadata(new JsonSchema
            {
                Name = Internal.StorageFields.DefaultSchema,
                KeyMode = KeyMode.Auto,
                AutoQueryable = true,
            });
        }
    }

    /// <summary>
    /// Registers (or re-registers) a dynamic schema's mapper from a JsonSchema entity.
    /// </summary>
    internal void RegisterJsonMetadata(JsonSchema schema)
    {
        var dynSchema = JsonMetadata.Parse(schema);
        dynSchema.AutoKeyProperties = _config.AutoKeyProperties;
        _schemas[schema.Name] = dynSchema;
        var mapper = new JsonDocumentMapper(dynSchema, LuceneVersion.LUCENE_48, _lottaCatalog.Analyzer, _lottaCatalog.EmbeddingGenerator);
        _dynamicMappers[schema.Name] = mapper;

        // Merge the mapper's per-field analyzer into the shared analyzer. A writer created
        // later picks this up automatically, since it is configured with the same instance.
        lock (_lock)
        {
            _perFieldAnalyzer.Merge(mapper.Analyzer);
        }
    }

    /// <summary>Reindex only the documents for a specific dynamic schema.</summary>
    private async Task ReindexJsonMetadataAsync(string schemaName, CancellationToken cancellationToken = default)
    {
        if (!_schemas.TryGetValue(schemaName, out var dynSchema)) return;
        if (!_dynamicMappers.TryGetValue(schemaName, out var mapper)) return;

        using var lease = await AcquireWriterAsync(cancellationToken);

        lock (_lock)
        {
            RequireWriter().DeleteDocuments(new Term(Internal.StorageFields.Schema, schemaName));
            _indexDirty = true;
        }

        await foreach (var doc in _tableAdapter.GetManyJsonDocumentsAsync(
            _lottaCatalog.Name, schemaName, cancellationToken: cancellationToken))
        {
            var document = new Document();
            mapper.ToDocument(doc, document);
            var etag = doc.GetETag();
            Internal.EntityMapper.AddMetadataToLuceneDocument(document, typeof(JsonDocument).FullName!, etag ?? "", schemaName);
            lock (_lock)
            {
                RequireWriter().AddDocument(document);
                _indexDirty = true;
            }
        }

        ReloadSearcher();
    }

    /// <summary>
    /// Resolve the schema name for a JsonDocument:
    /// 1. Explicit SetSchema() on the document
    /// 2. First matching discriminator from registered schemas
    /// 3. Default schema
    /// </summary>
    private string ResolveSchemaName(JsonDocument doc)
    {
        // 1. Explicit schema assignment
        var explicitSchema = doc.GetSchema();
        if (!string.IsNullOrEmpty(explicitSchema) && _schemas.ContainsKey(explicitSchema))
            return explicitSchema;

        // 2. Discriminator matching
        foreach (var (name, schema) in _schemas)
        {
            if (name == Internal.StorageFields.DefaultSchema) continue;
            if (!string.IsNullOrEmpty(schema.Match) && schema.MatchesDocument(doc.RootElement))
            {
                doc.SetSchema(name);
                return name;
            }
        }

        // 3. Default
        return Internal.StorageFields.DefaultSchema;
    }

    private JsonMetadata GetSchema(string schemaName)
    {
        if (_schemas.TryGetValue(schemaName, out var schema))
            return schema;
        throw new InvalidOperationException(
            $"Schema '{schemaName}' not registered. Save a JsonSchema with Name=\"{schemaName}\" first.");
    }

    /// <summary>
    /// Commit any pending index changes and refresh the searcher, including commits made by
    /// other processes. Always re-checks the directory, bypassing
    /// <see cref="ILottaConfiguration.MaxSearchStaleness"/>.
    /// Normally this happens automatically via <see cref="LottaConfiguration.AutoCommitDelay"/>.
    /// </summary>
    public void ReloadSearcher() => ReloadSearcherCore(force: true);

    /// <summary>
    /// Refresh before a search, honouring <see cref="ILottaConfiguration.MaxSearchStaleness"/>
    /// so the directory re-check does not run on every single query.
    /// </summary>
    private void MaybeReloadSearcher() => ReloadSearcherCore(force: false);

    private void ReloadSearcherCore(bool force)
    {
        lock (_lock)
        {
            if (_disposed) return;

            if (_indexDirty && _indexWriter != null)
            {
                _indexWriter.Commit();
                _indexDirty = false;
                force = true;   // our own writes must be visible immediately
            }

            if (!force)
            {
                var maxStale = _config.MaxSearchStaleness;
                if (maxStale < 0) return;   // never poll for other processes' commits
                if (maxStale > 0 &&
                    Stopwatch.GetElapsedTime(_lastRefreshTimestamp).TotalMilliseconds < maxStale)
                    return;
            }

            RefreshReadersLocked();
        }
    }

    /// <summary>
    /// Re-open both readers against the directory, picking up commits from any process.
    /// Cheap when the index is unchanged — both end in DirectoryReader.OpenIfChanged, which
    /// returns null and short-circuits. Callers must hold <c>_lock</c>.
    /// </summary>
    private void RefreshReadersLocked()
    {
        if (_searcherManager == null) return;   // index does not exist yet
        _searcherManager.MaybeRefresh();
        _lucene.Refresh();
        _lastRefreshTimestamp = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Mark write activity as of now and make sure the refresh/release loop is running, so the
    /// write lock is eventually given back. Unlike <see cref="ScheduleRefresh"/> this does not
    /// mark the index dirty — it is for write paths that have already committed their own work.
    /// </summary>
    private void ScheduleWriterRelease()
    {
        lock (_lock)
        {
            if (_disposed || _indexWriter == null) return;

            _lastWriteTimestamp = Stopwatch.GetTimestamp();
            StartRefreshLoopIfNeededLocked();
        }
    }

    /// <summary>
    /// Start the refresh/release loop unless one is already running. Callers must hold
    /// <c>_lock</c>.
    /// </summary>
    /// <remarks>
    /// Tracked with an explicit flag rather than <c>_refreshTask.IsCompleted</c>: a task that
    /// has decided to return has not yet transitioned to completed, so a write landing in that
    /// window would see "still running", decline to start a replacement, and then be left with
    /// no loop at all — holding the write lock indefinitely. The flag is cleared by the loop
    /// itself under this same lock, which closes that race.
    /// </remarks>
    private void StartRefreshLoopIfNeededLocked()
    {
        if (_refreshLoopRunning) return;
        _refreshLoopRunning = true;
        _refreshTask = RefreshLoopAsync();
    }

    private void ScheduleRefresh()
    {
        lock (_lock)
        {
            _indexDirty = true;
            _lastWriteTimestamp = Stopwatch.GetTimestamp();
            StartRefreshLoopIfNeededLocked();
        }
    }

    // Cap on a single hold-phase wait, so a long WriteLockReleaseDelay still responds
    // promptly to Dispose.
    private const int HoldPollCapMs = 250;

    /// <summary>
    /// Debounced commit loop, followed by a hold phase that releases the cross-process write
    /// lock once writes have been idle for <see cref="ILottaConfiguration.WriteLockReleaseDelay"/>.
    /// Exits after releasing; <see cref="ScheduleRefresh"/> restarts it on the next write.
    /// </summary>
    private async Task RefreshLoopAsync()
    {
        try
        {
            while (!_disposed)
            {
                // ---- commit / debounce phase ----
                var delayMs = _config.AutoCommitDelay;
                var maxHoldExpired = false;
                while (true)
                {
                    await Task.Delay(delayMs, _disposeCts.Token).ConfigureAwait(false);

                    lock (_lock)
                    {
                        if (_disposed) return;

                        // Max hold is deliberately evaluated here as well as in the hold phase.
                        // Under continuous writes every write refreshes _lastWriteTimestamp, so
                        // this loop would debounce forever and the hold phase — where the limit
                        // used to be checked — would never be reached. That is exactly the
                        // workload WriteLockMaxHoldTime exists to serve.
                        if (MaxHoldExceededLocked())
                        {
                            if (_indexDirty)
                            {
                                _indexWriter?.Commit();
                                RefreshReadersLocked();
                                _indexDirty = false;
                            }
                            maxHoldExpired = true;
                            break;
                        }

                        var elapsed = Stopwatch.GetElapsedTime(_lastWriteTimestamp);
                        var remaining = _config.AutoCommitDelay - (int)elapsed.TotalMilliseconds;
                        if (remaining > 0)
                        {
                            // A write arrived during our wait -- only wait the remaining delta
                            delayMs = remaining;
                            continue;
                        }
                        if (_indexDirty)
                        {
                            _indexWriter?.Commit();
                            RefreshReadersLocked();
                            _indexDirty = false;
                        }

                        // Re-check: did a write arrive while we were committing?
                        elapsed = Stopwatch.GetElapsedTime(_lastWriteTimestamp);
                        remaining = _config.AutoCommitDelay - (int)elapsed.TotalMilliseconds;
                        if (remaining > 0)
                        {
                            delayMs = remaining;
                            continue;
                        }
                    }
                    break;
                }

                // ---- hold phase: wait out the idle window, then release the write lock ----
                var hold = _config.WriteLockReleaseDelay;

                // A max-hold expiry releases even when the caller asked to hold indefinitely --
                // yielding the writer role is the whole point of the setting.
                if (hold < 0 && !maxHoldExpired)
                    return;   // configured to hold until Dispose/ReleaseWriteLockAsync

                var backToCommit = false;
                while (true)
                {
                    int wait;
                    lock (_lock)
                    {
                        if (_disposed) return;
                        if (_indexWriter == null) return;          // already released elsewhere

                        if (maxHoldExpired || MaxHoldExceededLocked())
                        {
                            wait = 0;
                        }
                        else
                        {
                            if (_indexDirty) { backToCommit = true; break; }

                            wait = hold - (int)Stopwatch.GetElapsedTime(_lastWriteTimestamp).TotalMilliseconds;
                            if (_config.WriteLockMaxHoldTime > 0)
                            {
                                wait = Math.Min(wait, _config.WriteLockMaxHoldTime
                                    - (int)Stopwatch.GetElapsedTime(_writerAcquiredTimestamp).TotalMilliseconds);
                            }
                        }
                    }

                    if (wait <= 0)
                    {
                        if (await TryReleaseWriterAsync(TimeSpan.FromSeconds(5), _disposeCts.Token)
                                .ConfigureAwait(false))
                            return;     // released -- the next write restarts this loop
                        wait = 50;      // writes still draining; try again shortly
                    }

                    await Task.Delay(Math.Min(wait, HoldPollCapMs), _disposeCts.Token).ConfigureAwait(false);
                }

                if (!backToCommit) return;
            }
        }
        catch (OperationCanceledException)
        {
            // _disposeCts fired -- Dispose is draining us.
        }
        finally
        {
            // Cleared under the lock so a write racing our exit reliably starts a replacement
            // loop rather than seeing a task that has decided to return but not yet completed.
            lock (_lock)
            {
                _refreshLoopRunning = false;
            }
        }
    }

    /// <summary>
    /// Whether the writer has been held longer than <see cref="ILottaConfiguration.WriteLockMaxHoldTime"/>.
    /// Callers must hold <c>_lock</c>.
    /// </summary>
    private bool MaxHoldExceededLocked()
    {
        var max = _config.WriteLockMaxHoldTime;
        if (max <= 0 || _indexWriter == null) return false;
        return Stopwatch.GetElapsedTime(_writerAcquiredTimestamp).TotalMilliseconds >= max;
    }

    // === On<T> (runtime registration) ===

    /// <summary>
    /// Register a handler at runtime. Returns a disposable — dispose to unregister.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="handler">Async handler receiving the object, trigger kind, and DB instance.</param>
    /// <returns>A disposable handle. Dispose to stop receiving notifications.</returns>
    public IDisposable On<T>(EntityHandler<T> handler) where T : class, new()
    {
        _handlers.AddOrUpdate(typeof(T),
            _ => ImmutableArray.Create<object>(handler),
            (_, existing) => existing.Add(handler));
        return new HandlerHandle(_handlers, typeof(T), handler);
    }

    // === Blobs ===

    private BlobContainerClient? _blobContainer;

    private BlobContainerClient GetBlobContainer()
    {
        if (_blobContainer == null)
        {
            _blobContainer = _lottaCatalog.GetBlobServiceClient()
                .GetBlobContainerClient(_lottaCatalog.Name);
            _blobContainer.CreateIfNotExists();
        }
        return _blobContainer;
    }

    private string GetBlobPath(string path) => $"{_databaseId}/Blobs/{path}";

    /// <summary>
    /// Upload a blob to this database's blob storage.
    /// If an OnUpload handler is registered, the stream is tee'd concurrently to the handler
    /// for metadata extraction, and the resulting BlobFile entity is saved automatically.
    /// </summary>
    /// <param name="path">Blob path relative to this database (e.g. "photos/avatar.jpg").</param>
    /// <param name="content">The content to upload.</param>
    /// <param name="contentType">Optional MIME type (e.g. "image/jpeg"). If null, detected from file extension.</param>
    /// <param name="overwrite">Whether to overwrite an existing blob. Defaults to true.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The extracted metadata if an OnUpload handler is registered, otherwise null.</returns>
    public async Task<BlobFile?> UploadBlobAsync(string path, Stream content, string? contentType = null, bool overwrite = true, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var blob = container.GetBlobClient(GetBlobPath(path));
        var handler = _config.UploadHandler;
        var resolvedContentType = contentType ?? DefaultBlobHandler.GetMimeType(Path.GetExtension(path));
        var options = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = resolvedContentType },
            Conditions = overwrite ? null : new BlobRequestConditions { IfNoneMatch = new ETag("*") }
        };

        if (handler == null)
        {
            await blob.UploadAsync(content, options, cancellationToken: cancellationToken);
            return null;
        }

        // TeeStream: blob upload and handler run concurrently, zero buffering
        var pipe = new Pipe();
        var teeStream = new TeeStream(content, pipe.Writer);
        var handlerStream = pipe.Reader.AsStream();

        var uploadTask = blob.UploadAsync(teeStream, options, cancellationToken: cancellationToken);
        var parseTask = handler(path, resolvedContentType, handlerStream, this, cancellationToken);

        await Task.WhenAll(uploadTask, parseTask);
        return await SaveBlobFileAsync(parseTask.Result, path, cancellationToken);
    }

    /// <summary>
    /// Upload a blob from a byte array.
    /// </summary>
    /// <param name="path">Blob path relative to this database.</param>
    /// <param name="content">The byte array to upload.</param>
    /// <param name="contentType">Optional MIME type (e.g. "image/jpeg"). If null, detected from file extension.</param>
    /// <param name="overwrite">Whether to overwrite an existing blob. Defaults to true.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The extracted metadata if an OnUpload handler is registered, otherwise null.</returns>
    public async Task<BlobFile?> UploadBlobAsync(string path, byte[] content, string? contentType = null, bool overwrite = true, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var blob = container.GetBlobClient(GetBlobPath(path));
        var handler = _config.UploadHandler;
        var resolvedContentType = contentType ?? DefaultBlobHandler.GetMimeType(Path.GetExtension(path));
        var options = new BlobUploadOptions
        {
            HttpHeaders = new BlobHttpHeaders { ContentType = resolvedContentType },
            Conditions = overwrite ? null : new BlobRequestConditions { IfNoneMatch = new ETag("*") }
        };

        using var uploadStream = new MemoryStream(content);
        await blob.UploadAsync(uploadStream, options, cancellationToken: cancellationToken);

        if (handler == null)
            return null;

        using var handlerStream = new MemoryStream(content);
        var blobFile = await handler(path, resolvedContentType, handlerStream, this, cancellationToken);
        return await SaveBlobFileAsync(blobFile, path, cancellationToken);
    }

    /// <summary>
    /// Upload a blob from a string (stored as UTF-8).
    /// </summary>
    /// <param name="path">Blob path relative to this database.</param>
    /// <param name="content">The string content to upload.</param>
    /// <param name="contentType">Optional MIME type. If null, detected from file extension.</param>
    /// <param name="overwrite">Whether to overwrite an existing blob. Defaults to true.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The extracted metadata if OnUpload handlers are registered, otherwise null.</returns>
    public async Task<BlobFile?> UploadBlobAsync(string path, string content, string? contentType = null, bool overwrite = true, CancellationToken cancellationToken = default)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(content);
        return await UploadBlobAsync(path, bytes, contentType, overwrite, cancellationToken);
    }

    private async Task<BlobFile?> SaveBlobFileAsync(BlobFile? blobFile, string path, CancellationToken cancellationToken)
    {
        if (blobFile != null)
        {
            blobFile.Path = path;
            await SaveAsync(blobFile, cancellationToken);
        }
        return blobFile;
    }

    /// <summary>
    /// Download a blob from this database's blob storage.
    /// </summary>
    /// <param name="path">Blob path relative to this database.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The blob content as a stream, or null if the blob does not exist.</returns>
    public async Task<Stream?> DownloadBlobAsync(string path, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var blob = container.GetBlobClient(GetBlobPath(path));
        try
        {
            var response = await blob.DownloadStreamingAsync(cancellationToken: cancellationToken);
            return response.Value.Content;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    /// <summary>
    /// Download a blob as a byte array.
    /// </summary>
    /// <param name="path">Blob path relative to this database.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The blob content as bytes, or null if the blob does not exist.</returns>
    public async Task<byte[]?> DownloadBlobBytesAsync(string path, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var blob = container.GetBlobClient(GetBlobPath(path));
        try
        {
            var response = await blob.DownloadContentAsync(cancellationToken: cancellationToken);
            return response.Value.Content.ToArray();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    /// <summary>
    /// Download a blob as a UTF-8 string.
    /// </summary>
    /// <param name="path">Blob path relative to this database.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The blob content as a string, or null if the blob does not exist.</returns>
    public async Task<string?> DownloadBlobStringAsync(string path, CancellationToken cancellationToken = default)
    {
        var bytes = await DownloadBlobBytesAsync(path, cancellationToken);
        return bytes != null ? System.Text.Encoding.UTF8.GetString(bytes) : null;
    }

    /// <summary>
    /// Delete a blob from this database's blob storage.
    /// Also deletes the associated BlobFile metadata entity if one exists.
    /// </summary>
    /// <param name="path">Blob path relative to this database.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the blob was deleted, false if it didn't exist.</returns>
    public async Task<bool> DeleteBlobAsync(string path, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var blob = container.GetBlobClient(GetBlobPath(path));
        var response = await blob.DeleteIfExistsAsync(cancellationToken: cancellationToken);

        // Cascade delete the metadata entity if BlobFile is registered
        if (_metadata.ContainsKey(typeof(BlobFile)))
        {
            var existing = await GetAsync<BlobFile>(path, cancellationToken);
            if (existing != null)
                await DeleteAsync(existing, cancellationToken);
        }

        return response.Value;
    }

    /// <summary>
    /// List blobs in this database's blob storage.
    /// </summary>
    /// <param name="folder">Optional folder path (e.g. "photos/"). Relative to this database. Null for root.</param>
    /// <param name="recursive">If true, includes blobs in all subfolders. If false, only blobs directly in the folder.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of blob paths relative to this database.</returns>
    public async Task<IReadOnlyList<string>> ListBlobsAsync(string? folder = null, bool recursive = true, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var folderNorm = NormalizeFolder(folder);
        var fullPrefix = GetBlobPath(folderNorm);
        var dbPrefix = GetBlobPath("");
        var blobs = new List<string>();

        if (recursive)
        {
            await foreach (var item in container.GetBlobsAsync(traits: BlobTraits.None, states: BlobStates.None, prefix: fullPrefix, cancellationToken: cancellationToken))
            {
                blobs.Add(StripDbPrefix(item.Name, dbPrefix));
            }
        }
        else
        {
            await foreach (var item in container.GetBlobsByHierarchyAsync(traits: BlobTraits.None, states: BlobStates.None, delimiter: "/", prefix: fullPrefix, cancellationToken: cancellationToken))
            {
                if (item.IsBlob)
                    blobs.Add(StripDbPrefix(item.Blob.Name, dbPrefix));
            }
        }

        return blobs;
    }

    /// <summary>
    /// List subfolders in this database's blob storage.
    /// </summary>
    /// <param name="folder">Optional folder path (e.g. "photos/"). Relative to this database. Null for root.</param>
    /// <param name="recursive">If true, includes all nested subfolders. If false, only immediate children.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of folder paths relative to this database (with trailing slash).</returns>
    public async Task<IReadOnlyList<string>> ListBlobFoldersAsync(string? folder = null, bool recursive = true, CancellationToken cancellationToken = default)
    {
        var container = GetBlobContainer();
        var folderNorm = NormalizeFolder(folder);
        var fullPrefix = GetBlobPath(folderNorm);
        var dbPrefix = GetBlobPath("");

        // BFS traversal using GetBlobsByHierarchyAsync — only fetches folder structure,
        // never enumerates blob contents. Efficient even for large blob stores.
        var folders = new List<string>();
        var queue = new Queue<string>();
        queue.Enqueue(fullPrefix);

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            await foreach (var item in container.GetBlobsByHierarchyAsync(traits: BlobTraits.None, states: BlobStates.None, delimiter: "/", prefix: current, cancellationToken: cancellationToken))
            {
                if (item.IsPrefix)
                {
                    folders.Add(StripDbPrefix(item.Prefix, dbPrefix));
                    if (recursive)
                        queue.Enqueue(item.Prefix);
                }
            }
        }

        return folders;
    }

    private static string NormalizeFolder(string? folder)
    {
        if (string.IsNullOrEmpty(folder)) return "";
        return folder.EndsWith('/') ? folder : folder + "/";
    }

    private static string StripDbPrefix(string fullPath, string dbPrefix)
    {
        return fullPath.StartsWith(dbPrefix) ? fullPath.Substring(dbPrefix.Length) : fullPath;
    }

    // === Maintain ===

    /// <summary>
    /// Deletes all documents from the Lucene index without touching Table Storage.
    /// Used by tests to verify that RebuildSearchIndex repopulates the index.
    /// </summary>
    internal async Task DeleteSearchIndexAsync(CancellationToken cancellationToken = default)
    {
        using var lease = await AcquireWriterAsync(cancellationToken);
        lock (_lock)
        {
            var writer = RequireWriter();
            writer.DeleteAll();
            writer.Commit();
            _indexDirty = false;
            RefreshReadersLocked();
        }
    }

    /// <summary>
    /// Reset this database: deletes all rows in this database's partition and clears the Lucene index.
    /// Other databases in the same catalog are not affected.
    /// </summary>
    public async Task ResetDatabaseAsync(CancellationToken cancellationToken = default)
    {
        using var writerLease = await AcquireWriterAsync(cancellationToken);

        // 1. Delete all rows in this database's partition
        await _tableAdapter.ResetPartitionAsync(_lottaCatalog.Name, cancellationToken: cancellationToken);

        // 2. Delete all documents from Lucene index
        lock (_lock)
        {
            var writer = RequireWriter();
            writer.DeleteAll();
            writer.Commit();
            _indexDirty = false;
            RefreshReadersLocked();
        }

        // 3. delete all blobs except search index directory
        var container = GetBlobContainer();
        var blobsPath = GetBlobPath("");
        await foreach (var blob in container.GetBlobsByHierarchyAsync(new GetBlobsByHierarchyOptions() { Prefix = blobsPath }, cancellationToken))
        {
            await container.DeleteBlobIfExistsAsync(blob.Blob.Name, cancellationToken: cancellationToken);
        }
    }

    /// <summary>
    /// Deletes all rows in this database's partition, deletes the Lucene index, and disposes all resources.
    /// Other databases in the same catalog are not affected. Use with caution — this is not reversible.
    /// This object will not be usable after calling this method.
    /// </summary>
    public async Task DeleteDatabaseAsync(CancellationToken cancellationToken = default)
    {
        using (var writerLease = await AcquireWriterAsync(cancellationToken))
        {
            await _tableAdapter.DeletePartitionAsync(_lottaCatalog.Name, cancellationToken);
            lock (_lock)
            {
                var writer = RequireWriter();
                writer.DeleteAll();
                writer.Commit();
                _indexDirty = false;
            }

            // Remove the manifest while still holding the writer. Releasing first would open a
            // window where another process sees a live manifest, opens the database and writes
            // rows we have already deleted — which the manifest removal would then orphan.
            await _lottaCatalog.RemoveDatabaseManifestAsync(_databaseId, cancellationToken);
        }

        // Don't keep holding the cross-process write lock for a database we just deleted.
        await ReleaseWriteLockAsync(cancellationToken);
    }

    // === Bulk operations ===

    /// <summary>
    /// Saves a collection of entities asynchronously, performing upsert operations for each entity in the batch.
    /// </summary>
    /// <remarks>Entities are processed in batches, with automatic flushing when a batch reaches 100
    /// operations or when duplicate keys are detected. The method collects all changes and errors encountered during
    /// the operation and returns them in the result. The operation is atomic per batch, but not across the entire
    /// collection.</remarks>
    /// <typeparam name="T">The type of the entities to be saved.</typeparam>
    /// <param name="entities">The collection of entities to save. Each entity will be upserted into the underlying data store. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the save operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains an ObjectResult with details about
    /// the changes made and any errors encountered during the save process.</returns>
    public async Task<ObjectResult> SaveManyAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
    {
        var allChanges = new List<ObjectChange>();
        var allErrors = new List<Exception>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new HashSet<string>();
        var pendingEntities = new List<(object entity, Type type)>();

        // One lease covering every batch flush.
        using var writerLease = await AcquireWriterAsync(cancellationToken);

        try
        {
            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();

                TableTransactionAction action;
                string key;

                if (entity is JsonDocument jsonDoc)
                {
                    var schemaName = ResolveSchemaName(jsonDoc);
                    var schema = GetSchema(schemaName);
                    key = schema.GetKey(jsonDoc);
                    action = new TableTransactionAction(
                        TableTransactionActionType.UpsertReplace,
                        Internal.EntityMapper.ToTableEntity(_tableAdapter.PartitionKey, key, jsonDoc, schema));
                    Internal.EntityMapper.AnnotateAfterWrite(jsonDoc, key, "", schemaName);
                }
                else
                {
                    var meta = GetMeta(entity.GetType());
                    key = meta.GetKey(entity);
                    if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
                        meta.SetKey(entity, key);
                    action = _tableAdapter.CreateUpsertAction(key, entity, meta);
                    entity.SetSchema(entity.GetType().Name);
                }

                // Auto-flush on duplicate key
                if (pendingKeys.Contains(key))
                    await FlushTypedAsync();

                pendingActions.Add(action);
                pendingKeys.Add(key);
                pendingEntities.Add((entity, entity.GetType()));

                // Auto-flush at 100 operations
                if (pendingActions.Count >= 100)
                    await FlushTypedAsync();
            }

            // Flush remaining
            if (pendingActions.Count > 0)
                await FlushTypedAsync();

            async Task FlushTypedAsync()
            {
                using var locks = await AcquireLocksAsync(pendingKeys, cancellationToken);
                var etags = await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
                for (int i = 0; i < pendingEntities.Count; i++)
                {
                    if (etags[i] != null)
                        pendingEntities[i].entity.SetETag(etags[i]!);
                }
                await RunPendingHandlersAsync(pendingEntities, TriggerKind.Saved, allChanges, allErrors, cancellationToken);
                pendingActions.Clear();
                pendingKeys.Clear();
            }
        }
        finally
        {
            _chainChanges.Value = null;
            _chainErrors.Value = null;
        }

        return new ObjectResult { Changes = allChanges, Errors = allErrors };
    }

    /// <summary>
    /// Delete all objects matching a predicate. Queries table storage, deletes each match,
    /// removes from Lucene, and runs On&lt;T&gt; handlers for each deletion.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="predicate">Filter expression — objects matching this are deleted.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all deletions and handler-triggered changes.</returns>
    public async Task<ObjectResult> DeleteManyAsync<T>(Expression<Func<T, bool>>? predicate = null, CancellationToken cancellationToken = default) where T : class, new()
    {
        var matches = await GetManyAsync<T>(predicate).ToListAsync(cancellationToken);
        if (matches.Count == 0)
            return new ObjectResult();

        return await DeleteManyAsync<T>(matches, cancellationToken);
    }


    /// <summary>
    /// Delete multiple objects by entity. Runs On&lt;T&gt; handlers (including Lucene cleanup) for each deletion.
    /// </summary>
    /// <typeparam name="T">The object type.</typeparam>
    /// <param name="entities">The objects to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all deletions and any handler errors.</returns>
    public async Task<ObjectResult> DeleteManyAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class
    {
        return await DeleteManyAsyncCore(entities.Select(e => GetDeleteTruple(e))
            .ToAsyncEnumerable(), cancellationToken);
    }

    /// <summary>
    /// Delete multiple objects by key in bulk. Table storage writes are batched transactionally
    /// (auto-flushed at 100 ops or on duplicate key). On&lt;T&gt; handlers (including Lucene cleanup)
    /// run after each batch commit succeeds.
    /// </summary>
    /// <param name="keys">The keys of the objects to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all deletions and any handler errors.</returns>
    public async Task<ObjectResult> DeleteManyAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        return await DeleteManyAsyncCore(_tableAdapter.GetManyAsync(_lottaCatalog.Name, keys, cancellationToken: cancellationToken)
            .Select(e => GetDeleteTruple(e)), cancellationToken);
    }

    private (object?, Type?, string) GetDeleteTruple(object e)
    {
        return ((object?)e, (Type?)e.GetType(), GetMeta(e.GetType()).GetKey(e))!;
    }


    private async Task<ObjectResult> DeleteManyAsyncCore(IAsyncEnumerable<(object? entity, Type? type, string key)> items, CancellationToken cancellationToken = default)
    {
        var allChanges = new List<ObjectChange>();
        var allErrors = new List<Exception>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new HashSet<string>();
        var pendingEntities = new List<(object? entity, Type? type, string key)>();

        // One lease covering every batch flush.
        using var writerLease = await AcquireWriterAsync(cancellationToken);

        try
        {
            await foreach (var (entity, type, key) in items)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Auto-flush on duplicate key
                if (pendingKeys.Contains(key))
                    await FlushDeleteAsync();

                pendingActions.Add(_tableAdapter.CreateDeleteAction(key));
                pendingKeys.Add(key);
                pendingEntities.Add((entity, type, key));

                if (pendingActions.Count >= 100)
                    await FlushDeleteAsync();
            }

            if (pendingActions.Count > 0)
                await FlushDeleteAsync();

            async Task FlushDeleteAsync()
            {
                using var locks = await AcquireLocksAsync(pendingKeys, cancellationToken);
                await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
                await RunPendingDeleteHandlersAsync(pendingEntities, allChanges, allErrors, cancellationToken);
                pendingActions.Clear();
                pendingKeys.Clear();
                pendingEntities.Clear();
            }
        }
        finally
        {
            _chainChanges.Value = null;
            _chainErrors.Value = null;
        }

        return new ObjectResult { Changes = allChanges, Errors = allErrors };
    }

    private async Task RunPendingHandlersAsync(
        List<(object entity, Type type)> pending,
        TriggerKind kind,
        List<ObjectChange> allChanges,
        List<Exception> allErrors,
        CancellationToken cancellationToken)
    {
        foreach (var (entity, type) in pending)
        {
            string key;
            if (entity is JsonDocument jd)
                key = jd.GetKey() ?? "";
            else
            {
                var meta = GetMeta(type);
                key = meta.GetKey(entity);
            }
            var changeKind = kind == TriggerKind.Saved ? ChangeKind.Saved : ChangeKind.Deleted;
            var change = new ObjectChange { Type = type, Key = key, Kind = changeKind, Object = entity };
            allChanges.Add(change);

            _chainChanges.Value = allChanges;
            _chainErrors.Value = allErrors;
            await RunHandlersAsync(entity, type, kind, allErrors, cancellationToken);
        }
        pending.Clear();
    }

    private async Task RunPendingDeleteHandlersAsync(
        List<(object? entity, Type? type, string key)> pending,
        List<ObjectChange> allChanges,
        List<Exception> allErrors,
        CancellationToken cancellationToken)
    {
        foreach (var (entity, type, key) in pending)
        {
            var change = new ObjectChange { Type = type!, Key = key, Kind = ChangeKind.Deleted, Object = entity };
            allChanges.Add(change);

            if (entity != null && type != null)
            {
                _chainChanges.Value = allChanges;
                _chainErrors.Value = allErrors;
                await RunHandlersAsync(entity, type, TriggerKind.Deleted, allErrors, cancellationToken);
            }
        }
        pending.Clear();
    }

    // === On<T> handler engine ===

    private async Task RunHandlersAsync<T>(T entity, TriggerKind kind,
        List<Exception> errors, CancellationToken cancellationToken) where T : class, new()
    {
        // Cycle detection: if this type is already being processed in the current chain, stop.
        // This prevents A→B→A infinite loops regardless of key values.
        var visited = _processing.Value ??= new HashSet<string>();
        var cycleKey = typeof(T).Name;
        if (!visited.Add(cycleKey))
            return; // cycle detected — this type is already in the handler chain

        try
        {
            if (!_handlers.TryGetValue(typeof(T), out var handlers)) return;

            foreach (var handler in handlers)
            {
                if (handler is EntityHandler<T> typed)
                {
                    try
                    {
                        await typed(entity, kind, this, cancellationToken);
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
    /// Walks the type hierarchy so handlers registered for base types also fire.
    /// For example, saving a BlobPhoto fires On&lt;BlobPhoto&gt;, On&lt;BlobFile&gt;, etc.
    /// </summary>
    private async Task RunHandlersAsync(object entity, Type entityType, TriggerKind kind,
        List<Exception> errors, CancellationToken cancellationToken)
    {
        // Walk up the type hierarchy: BlobPhoto → BlobFile → object
        for (var type = entityType; type != null && type != typeof(object); type = type.BaseType)
        {
            if (!_handlers.TryGetValue(type, out var handlers)) continue;

            // For types without new() constraint (e.g. JsonDocument), invoke handlers directly
            if (type.GetConstructor(Type.EmptyTypes) == null)
            {
                foreach (var handler in handlers)
                {
                    // Use dynamic dispatch to invoke the correctly-typed delegate
                    try { await ((dynamic)handler)((dynamic)entity, kind, this, cancellationToken); }
                    catch (Exception ex) { errors.Add(ex); }
                }
                continue;
            }

            var trampoline = _runHandlersAsyncDelegateCache.GetOrAdd(type, static (t, m) =>
            {
                // Build: (LottaDB db, object entity, TriggerKind kind, List<Exception> errors, CancellationToken ct)
                //            => db.RunHandlersAsync<T>((T)entity, kind, errors, ct)
                var dbParam = Expression.Parameter(typeof(LottaDB), "db");
                var entityParam = Expression.Parameter(typeof(object), "entity");
                var kindParam = Expression.Parameter(typeof(TriggerKind), "kind");
                var errorsParam = Expression.Parameter(typeof(List<Exception>), "errors");
                var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");
                var body = Expression.Call(
                    dbParam,
                    m.MakeGenericMethod(t),
                    Expression.Convert(entityParam, t),
                    kindParam, errorsParam, ctParam);
                return Expression.Lambda<Func<LottaDB, object, TriggerKind, List<Exception>, CancellationToken, Task>>(
                    body, dbParam, entityParam, kindParam, errorsParam, ctParam).Compile();
            }, _runHandlersAsyncMethod);
            await trampoline(this, entity, kind, errors, cancellationToken);
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
            _disposeCts.Cancel();
            if (disposing)
            {
                // Wait for the background refresh task to exit
                try { _refreshTask?.GetAwaiter().GetResult(); } catch { }

                lock (_lock)
                {
                    // The writer may already have been released by the idle timer — nothing
                    // is lost, since releasing commits first.
                    if (_indexWriter != null)
                    {
                        try { _indexWriter.Commit(); } catch { }
                        _indexWriter.Dispose();
                        _indexWriter = null;
                    }
                    _searcherManager?.Dispose();   // must precede _directory.Dispose()
                    _lucene?.Dispose();
                    _directory?.Dispose();
                }
                _writerGate.Dispose();
                _disposeCts.Dispose();
            }
        }
    }

    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~LottaDB()
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }

    // Tracks which stripe indices the current async call chain already holds,
    // so reentrant calls (e.g., handler chains) skip re-acquisition and avoid deadlock.
    private readonly AsyncLocal<HashSet<int>> _heldLocks = new();

    private async Task<KeyLock> AcquireLockAsync(string key, CancellationToken ct)
    {
        var index = (key.GetHashCode() & 0x7FFFFFFF) % LockStripeCount;
        var held = _heldLocks.Value ??= new HashSet<int>();
        if (held.Contains(index))
            return new KeyLock(null, -1, _heldLocks); // already held — no-op

        await _keyLocks[index].WaitAsync(ct);
        held.Add(index);
        return new KeyLock(_keyLocks[index], index, _heldLocks);
    }

    private async Task<KeyLocks> AcquireLocksAsync(IEnumerable<string> keys, CancellationToken ct)
    {
        var held = _heldLocks.Value ??= new HashSet<int>();
        var indices = keys
            .Select(k => (k.GetHashCode() & 0x7FFFFFFF) % LockStripeCount)
            .Distinct()
            .Where(i => !held.Contains(i)) // skip already-held stripes
            .OrderBy(i => i)
            .ToArray();

        foreach (var i in indices)
        {
            await _keyLocks[i].WaitAsync(ct);
            held.Add(i);
        }
        return new KeyLocks(_keyLocks, indices, _heldLocks);
    }

    internal readonly struct KeyLock : IDisposable
    {
        private readonly SemaphoreSlim? _semaphore;
        private readonly int _index;
        private readonly AsyncLocal<HashSet<int>> _held;

        internal KeyLock(SemaphoreSlim? semaphore, int index, AsyncLocal<HashSet<int>> held)
        {
            _semaphore = semaphore;
            _index = index;
            _held = held;
        }

        public void Dispose()
        {
            if (_semaphore == null) return; // was reentrant — nothing to release
            _semaphore.Release();
            _held.Value?.Remove(_index);
        }
    }

    internal readonly struct KeyLocks : IDisposable
    {
        private readonly SemaphoreSlim[] _allLocks;
        private readonly int[] _acquiredIndices;
        private readonly AsyncLocal<HashSet<int>> _held;

        internal KeyLocks(SemaphoreSlim[] allLocks, int[] acquiredIndices, AsyncLocal<HashSet<int>> held)
        {
            _allLocks = allLocks;
            _acquiredIndices = acquiredIndices;
            _held = held;
        }

        public void Dispose()
        {
            for (int i = _acquiredIndices.Length - 1; i >= 0; i--)
            {
                _allLocks[_acquiredIndices[i]].Release();
                _held.Value?.Remove(_acquiredIndices[i]);
            }
        }
    }


    /// <inheritdoc/>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}

internal class HandlerHandle : IDisposable
{
    private readonly ConcurrentDictionary<Type, ImmutableArray<object>> _handlers;
    private readonly Type _type;
    private readonly object _handler;
    public HandlerHandle(ConcurrentDictionary<Type, ImmutableArray<object>> handlers, Type type, object handler)
    { _handlers = handlers; _type = type; _handler = handler; }
    public void Dispose()
    {
        _handlers.AddOrUpdate(_type,
            _ => ImmutableArray<object>.Empty,
            (_, existing) => existing.Remove(_handler));
    }
}
