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
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq.Expressions;
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
    private ReadOnlyLuceneDataProvider _lucene;
    private long _lastWriteTimestamp;
    private Task? _refreshTask;
    private IndexWriter _indexWriter;
    private bool _indexDirty;
    private bool _disposed;

    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, IDocumentMapper> _mappers = new();
    private readonly ConcurrentDictionary<Type, List<object>> _handlers = new();

    // Cycle detection: tracks object keys being processed in the current call chain
    private static readonly AsyncLocal<HashSet<string>> _processing = new();
    // Collects changes across the entire call chain (root save + handler saves)
    private static readonly AsyncLocal<List<ObjectChange>?> _chainChanges = new();
    private static readonly AsyncLocal<List<Exception>?> _chainErrors = new();
    internal const string OBJECT_FIELD = "_object_";
    internal const string KEY_FIELD = "_key_";
    internal const string CONTENT_FIELD = "_content_";

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
        _tableAdapter = new TableStorageAdapter(catalog.GetTableServiceClient(), databaseId);
        _directory = catalog.LuceneDirectoryFactory($"{catalog.Name}/{databaseId}/Search");

        InitializeMetadata();
        InitializeMappers();
        InitializeHandlers();
        InitializeLuceneHandlers();

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
        if (catalog.EmbeddingGenerator != null)
            _lucene.Settings.EmbeddingGenerator = catalog.EmbeddingGenerator;
        _lucene.MapperFactory = (type, version, analyzer) =>
        {
            var mapperType = typeof(LottaDocumentMapper<>).MakeGenericType(type);
            _metadata.TryGetValue(type, out var meta);
            return Activator.CreateInstance(mapperType, version, catalog.Analyzer, meta, catalog.EmbeddingGenerator, this)!;
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
        var list = _handlers.GetOrAdd(typeof(T), _ => new List<object>());
        list.Add((EntityHandler<T>)((entity, kind, db) =>
        {
            var meta = GetMeta<T>();
            var key = meta.GetKey(entity);
            lock (_lock)
            {
                _indexWriter.DeleteDocuments([new Term(KEY_FIELD, key)]);
                if (kind == TriggerKind.Saved)
                {
                    var mapper = GetMapper<T>();
                    var document = new Document();
                    mapper.ToDocument(entity, document);
                    _indexWriter.AddDocument(document);
                }
            }
            ScheduleRefresh();
            return Task.CompletedTask;
        }));
    }

    /// <summary>
    /// Rebuild the entire Lucene index from Azure Table Storage.
    /// Re-indexes all registered types. Does not run On&lt;T&gt; handlers.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task RebuildSearchIndex(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            _indexWriter.DeleteAll();
            _indexDirty = true;
        }

        await foreach (var entity in _tableAdapter.GetAllAsync(_lottaCatalog.Name, cancellationToken: cancellationToken))
        {
            var meta = GetMeta(entity.GetType());
            var key = meta.GetKey(entity);
            var mapper = GetMapper(entity.GetType());
            var document = new Document();
            mapper.ToDocument(entity, document);
            lock (_lock)
            {
                _indexWriter.AddDocument(document);
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
            return new LottaDocumentMapper<T>(Version.LUCENE_48, _lottaCatalog.Analyzer, meta, _lottaCatalog.EmbeddingGenerator, this);
        });
    }

    private IDocumentMapper GetMapper(Type type)
    {
        return _mappers.GetOrAdd(type, static (t, state) =>
        {
            var db = (LottaDB)state!;
            db._metadata.TryGetValue(t, out var meta);
            var mapperType = typeof(LottaDocumentMapper<>).MakeGenericType(t);
            return (IDocumentMapper)Activator.CreateInstance(mapperType, Version.LUCENE_48, db._lottaCatalog.Analyzer, meta, db._lottaCatalog.EmbeddingGenerator, db)!;
        }, this);
    }

    // === Write ===

    /// <summary>
    /// Save (upsert) an object. Key extracted from [Key] attribute.
    /// Writes to table storage, then runs On&lt;T&gt; handlers (including Lucene indexing).
    /// </summary>
    /// <param name="entity">The object to save.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any handler errors.</returns>
    public async Task<ObjectResult> SaveAsync(object entity, CancellationToken cancellationToken = default)
    {
        var meta = GetMeta(entity.GetType());
        var key = meta.GetKey(entity);

        // For Auto keys, set the generated key back on the entity
        // so subsequent GetKey calls return the same value
        if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
            meta.SetKey(entity, key);

        await _tableAdapter.UpsertAsync(_lottaCatalog.Name, key, entity, meta);

        if (entity is BlobFile bf) bf.Database = this;

        var change = new ObjectChange { Type = entity.GetType(), Key = key, Kind = ChangeKind.Saved, Object = entity };

        // If we're inside a handler chain, add to the chain's collection
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

        await _tableAdapter.DeleteAsync(_lottaCatalog.Name, key);

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
        const int maxAttempts = 16;
        var meta = GetMeta<T>();

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (current, etag) = await _tableAdapter.GetAsync<T>(_lottaCatalog.Name, key, cancellationToken: cancellationToken);
            if (current == null || string.IsNullOrEmpty(etag))
                throw new InvalidOperationException($"{typeof(T).Name} '{key}' not found.");

            var mutated = mutate(current);

            var committed = await _tableAdapter.TryReplaceAsync(_lottaCatalog.Name, key, mutated!, meta, etag, cancellationToken: cancellationToken);
            if (!committed)
                continue; // someone else wrote between our read and write — re-read and retry

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

        throw new InvalidOperationException(
            $"ChangeAsync<{typeof(T).Name}>('{key}') exceeded {maxAttempts} attempts due to concurrent ETag conflicts.");
    }

    // === Read ===

    /// <summary>Point-read an object by key from Azure Table Storage.</summary>
    /// <param name="key">The unique key of the object.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The object, or null if not found.</returns>
    public async Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) where T : class, new()
    {
        var (result, _) = await _tableAdapter.GetAsync<T>(_lottaCatalog.Name, key, cancellationToken: cancellationToken);
        if (result is BlobFile bf) bf.Database = this;
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
        ReloadSearcher();

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
    /// Force a Lucene index commit and refresh the searcher if there are pending writes.
    /// Normally this happens automatically via <see cref="LottaConfiguration.AutoCommitDelay"/>.
    /// </summary>
    public void ReloadSearcher()
    {
        lock (_lock)
        {
            if (!_disposed)
            {
                if (_indexDirty)
                {
                    _indexWriter.Commit();
                    _lucene.Refresh();
                    _indexDirty = false;
                }
            }
        }
    }

    private void ScheduleRefresh()
    {
        lock (_lock)
        {
            _indexDirty = true;
            _lastWriteTimestamp = Stopwatch.GetTimestamp();

            if (_refreshTask == null || _refreshTask.IsCompleted)
            {
                _refreshTask = RefreshLoopAsync();
            }
        }
    }

    private async Task RefreshLoopAsync()
    {
        var delayMs = _config.AutoCommitDelay;

        while (!_disposed)
        {
            await Task.Delay(delayMs).ConfigureAwait(false);

            lock (_lock)
            {
                if (_disposed) return;

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
                    _lucene?.Refresh();
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

            return; // Done -- no more pending writes
        }
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
        var list = _handlers.GetOrAdd(typeof(T), _ => new List<object>());
        lock (list) { list.Add(handler); }
        return new HandlerHandle(list, handler);
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
        var parseTask = handler(path, resolvedContentType, handlerStream, this);

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
        var blobFile = await handler(path, resolvedContentType, handlerStream, this);
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
            await foreach (var item in container.GetBlobsAsync(prefix: fullPrefix, cancellationToken: cancellationToken))
            {
                blobs.Add(StripDbPrefix(item.Name, dbPrefix));
            }
        }
        else
        {
            await foreach (var item in container.GetBlobsByHierarchyAsync(delimiter: "/", prefix: fullPrefix, cancellationToken: cancellationToken))
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
            await foreach (var item in container.GetBlobsByHierarchyAsync(delimiter: "/", prefix: current, cancellationToken: cancellationToken))
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
    /// Reset this database: deletes all rows in this database's partition and clears the Lucene index.
    /// Other databases in the same catalog are not affected.
    /// </summary>
    public async Task ResetDatabaseAsync(CancellationToken cancellationToken = default)
    {
        // 1. Delete all rows in this database's partition
        await _tableAdapter.ResetPartitionAsync(_lottaCatalog.Name, cancellationToken: cancellationToken);

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
    /// Deletes all rows in this database's partition, deletes the Lucene index, and disposes all resources.
    /// Other databases in the same catalog are not affected. Use with caution — this is not reversible.
    /// This object will not be usable after calling this method.
    /// </summary>
    public async Task DeleteDatabaseAsync(CancellationToken cancellationToken = default)
    {
        await _tableAdapter.DeletePartitionAsync(_lottaCatalog.Name, cancellationToken);
        await _lottaCatalog.RemoveDatabaseManifestAsync(_databaseId, cancellationToken);

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

    // === Bulk operations ===

    /// <summary>
    /// Save (upsert) multiple objects in bulk. Table storage writes are batched transactionally
    /// (auto-flushed at 100 ops or on duplicate key). On&lt;T&gt; handlers (including Lucene indexing)
    /// run after each batch commit succeeds.
    /// </summary>
    /// <param name="entities">The objects to save.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An <see cref="ObjectResult"/> containing all changes and any handler errors.</returns>
    public async Task<ObjectResult> SaveManyAsync(IEnumerable<object> entities, CancellationToken cancellationToken = default)
    {
        var allChanges = new List<ObjectChange>();
        var allErrors = new List<Exception>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new HashSet<string>();
        var pendingEntities = new List<(object entity, Type type)>();

        try
        {
            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var meta = GetMeta(entity.GetType());
                var key = meta.GetKey(entity);
                if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
                    meta.SetKey(entity, key);

                // Auto-flush on duplicate key
                if (pendingKeys.Contains(key))
                {
                    await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions);
                    await RunPendingHandlersAsync(pendingEntities, TriggerKind.Saved, allChanges, allErrors, cancellationToken);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                pendingActions.Add(_tableAdapter.CreateUpsertAction(key, entity, meta));
                pendingKeys.Add(key);
                pendingEntities.Add((entity, entity.GetType()));

                // Auto-flush at 100 operations
                if (pendingActions.Count >= 100)
                {
                    await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions);
                    await RunPendingHandlersAsync(pendingEntities, TriggerKind.Saved, allChanges, allErrors, cancellationToken);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }
            }

            // Flush remaining
            if (pendingActions.Count > 0)
            {
                await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions);
                await RunPendingHandlersAsync(pendingEntities, TriggerKind.Saved, allChanges, allErrors, cancellationToken);
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
    public async Task<ObjectResult> DeleteManyAsync<T>(IEnumerable<T> entities, CancellationToken cancellationToken = default) where T : class, new()
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

        try
        {
            await foreach (var (entity, type, key) in items)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Auto-flush on duplicate key
                if (pendingKeys.Contains(key))
                {
                    await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions);
                    await RunPendingDeleteHandlersAsync(pendingEntities, allChanges, allErrors, cancellationToken);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                pendingActions.Add(_tableAdapter.CreateDeleteAction(key));
                pendingKeys.Add(key);
                pendingEntities.Add((entity, type, key));

                if (pendingActions.Count >= 100)
                {
                    await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions);
                    await RunPendingDeleteHandlersAsync(pendingEntities, allChanges, allErrors, cancellationToken);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }
            }

            if (pendingActions.Count > 0)
            {
                await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions);
                await RunPendingDeleteHandlersAsync(pendingEntities, allChanges, allErrors, cancellationToken);
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
            var meta = GetMeta(type);
            var key = meta.GetKey(entity);
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
            if (!_handlers.TryGetValue(typeof(T), out var list)) return;

            List<object> snapshot;
            lock (list) { snapshot = list.ToList(); }

            foreach (var handler in snapshot)
            {
                if (handler is EntityHandler<T> typed)
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
    /// Walks the type hierarchy so handlers registered for base types also fire.
    /// For example, saving a BlobPhoto fires On&lt;BlobPhoto&gt;, On&lt;BlobFile&gt;, etc.
    /// </summary>
    private async Task RunHandlersAsync(object entity, Type entityType, TriggerKind kind,
        List<Exception> errors, CancellationToken cancellationToken)
    {
        var method = typeof(LottaDB).GetMethods(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)
            .First(m => m.Name == nameof(RunHandlersAsync) && m.IsGenericMethod);

        // Walk up the type hierarchy: BlobPhoto → BlobFile → object
        for (var type = entityType; type != null && type != typeof(object); type = type.BaseType)
        {
            if (!_handlers.ContainsKey(type)) continue;

            await (Task)method.MakeGenericMethod(type).Invoke(this, new[] { entity, kind, errors, cancellationToken })!;
        }
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

    /// <inheritdoc/>
    public void Dispose()
    {
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
