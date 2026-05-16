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
    private ReadOnlyLuceneDataProvider _lucene;
    private long _lastWriteTimestamp;
    private Task? _refreshTask;
    private IndexWriter _indexWriter;
    private bool _indexDirty;
    private bool _disposed;

    internal readonly ConcurrentDictionary<Type, TypeMetadata> _metadata = new();
    private readonly ConcurrentDictionary<Type, IDocumentMapper> _mappers = new();
    private readonly ConcurrentDictionary<Type, List<object>> _handlers = new();
    internal readonly ConcurrentDictionary<string, JsonMetadata> _schemas = new();
    private readonly ConcurrentDictionary<string, JsonDocumentMapper> _dynamicMappers = new();
    // Cycle detection: tracks object keys being processed in the current call chain
    private static readonly AsyncLocal<HashSet<string>> _processing = new();
    // Collects changes across the entire call chain (root save + handler saves)
    private static readonly AsyncLocal<List<ObjectChange>?> _chainChanges = new();
    private static readonly AsyncLocal<List<Exception>?> _chainErrors = new();
    internal const string OBJECT_FIELD = "_object_";
    internal const string ETAG_FIELD = "_etag_";
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

        // Auto-register JsonDocumentType if not already registered
        if (!_config.StorageConfigurations.ContainsKey(typeof(JsonDocumentType)))
        {
            var jsonSchemaConfig = new StorageConfiguration<JsonDocumentType>();
            _config.StorageConfigurations[typeof(JsonDocumentType)] = jsonSchemaConfig;
        }

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
        foreach (var mapper in _dynamicMappers.Values)
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
            var mapperType = typeof(TypeDocumentMapper<>).MakeGenericType(type);
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

        // Built-in handler: when a JsonDocumentType is saved/deleted, update the dynamic mappers
        var jsonSchemaList = _handlers.GetOrAdd(typeof(JsonDocumentType), _ => new List<object>());
        jsonSchemaList.Add((EntityHandler<JsonDocumentType>)(async (schema, kind, db, cancellationToken) =>
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
                    _indexWriter.DeleteDocuments(new Term("_type_", JsonMetadata.StoragePrefix + schema.Name));
                    _indexDirty = true;
                }
                ScheduleRefresh();
            }
        }));
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
        list.Add((EntityHandler<T>)((entity, kind, db, cancellationToken) =>
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
                    var etag = entity.GetETag()
                        ?? throw new InvalidOperationException(
                            $"Cannot index {typeof(T).Name} '{key}': entity has no ETag. This is a bug — SetETag should have been called before the Lucene handler.");
                    document.Add(new StoredField(ETAG_FIELD, etag));
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

        // Single pass over all rows — dispatch to typed or dynamic mapper based on Type column
        await foreach (var tableEntity in _tableAdapter.GetAllRawAsync(_lottaCatalog.Name, cancellationToken: cancellationToken))
        {
            var typeName = tableEntity.GetString(TableEntityExtensions.TypeProperty);
            if (typeName == null) continue;

            var document = new Document();

            if (JsonMetadata.IsDynamicTypeName(typeName))
            {
                // Dynamic schema entity — look up by unprefixed name
                var schemaName = JsonMetadata.UnprefixTypeName(typeName);
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

            // Store ETag from Table Storage
            document.Add(new StoredField(ETAG_FIELD, tableEntity.ETag.ToString()));

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
    public async Task<ObjectResult> SaveAsync(object entity, CancellationToken cancellationToken = default)
    {
        var meta = GetMeta(entity.GetType());
        var key = meta.GetKey(entity);

        // For Auto keys, set the generated key back on the entity
        // so subsequent GetKey calls return the same value
        if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
            meta.SetKey(entity, key);

        entity.SetKey(key);

        var existingETag = entity.GetETag();
        string newETag;
        if (existingETag != null)
        {
            // Conditional write — entity has an ETag from a previous read
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
            // Unconditional upsert
            newETag = await _tableAdapter.UpsertAsync(_lottaCatalog.Name, key, entity, meta, cancellationToken);
        }
        entity.SetETag(newETag);

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
            mutated!.SetETag(newETag);

            var change = new ObjectChange { Type = mutated!.GetType(), Key = key, Kind = ChangeKind.Saved, Object = mutated };

            var isRoot = _chainChanges.Value == null;
            var changes = _chainChanges.Value ??= new List<ObjectChange>();
            var errors = _chainErrors.Value ??= new List<Exception>();
            changes.Add(change);

            mutated.SetKey(key);

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
        var (result, etag) = await _tableAdapter.GetAsync<T>(_lottaCatalog.Name, key, cancellationToken: cancellationToken);
        if (result != null && etag != null) result.SetETag(etag);
        if (result != null) result.SetKey(key);
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
            var meta = GetMeta(item.GetType());
            item.SetKey(meta.GetKey(item));
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

    // === Dynamic (JSON Schema) API ===

    /// <summary>
    /// Save a JSON document using a registered dynamic schema.
    /// The key is extracted from the JSON (or auto-generated for <see cref="KeyMode.Auto"/>).
    /// If the document has an ETag (from a previous read), performs a conditional write
    /// that throws <see cref="ConcurrencyException"/> on conflict. Otherwise, performs an unconditional upsert.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="json">The JSON document to save.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<ObjectResult> SaveAsync(string schemaName, JsonDocument json, CancellationToken cancellationToken = default)
    {
        var schema = GetSchema(schemaName);
        var key = schema.GetKey(json);
        json.SetKey(key);

        var existingETag = json.GetETag();
        string newETag;
        if (existingETag != null)
        {
            try
            {
                newETag = await _tableAdapter.ReplaceJsonDocumentAsync(_lottaCatalog.Name, key, schema.StorageTypeName, json, schema, existingETag, cancellationToken);
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 412)
            {
                throw new ConcurrencyException(key, typeof(JsonDocument));
            }
        }
        else
        {
            newETag = await _tableAdapter.UpsertJsonDocumentAsync(_lottaCatalog.Name, key, schema.StorageTypeName, json, schema, cancellationToken);
        }
        json.SetETag(newETag);

        // Index in Lucene
        var mapper = _dynamicMappers[schemaName];
        lock (_lock)
        {
            _indexWriter.DeleteDocuments([new Term(KEY_FIELD, key)]);
            var document = new Document();
            mapper.ToDocument(json, document);
            document.Add(new StoredField(ETAG_FIELD, newETag));
            _indexWriter.AddDocument(document);
        }
        ScheduleRefresh();

        var change = new ObjectChange { Type = typeof(JsonDocument), Key = key, Kind = ChangeKind.Saved };
        return new ObjectResult { Changes = [change], Errors = [] };
    }

    /// <summary>
    /// Get a JSON document by schema name and key.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="key">The document key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<JsonDocument?> GetAsync(string schemaName, string key, CancellationToken cancellationToken = default)
    {
        GetSchema(schemaName); // validate schema exists
        return await _tableAdapter.GetJsonDocumentAsync(_lottaCatalog.Name, key, cancellationToken);
    }

    /// <summary>
    /// Delete a JSON document by schema name and key.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="key">The document key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<ObjectResult> DeleteAsync(string schemaName, string key, CancellationToken cancellationToken = default)
    {
        GetSchema(schemaName); // validate schema exists

        // Fetch document before deleting (needed for handlers)
        var doc = await _tableAdapter.GetJsonDocumentAsync(_lottaCatalog.Name, key, cancellationToken);

        await _tableAdapter.DeleteAsync(_lottaCatalog.Name, key, cancellationToken);
        lock (_lock)
        {
            _indexWriter.DeleteDocuments([new Term(KEY_FIELD, key)]);
        }
        ScheduleRefresh();

        var change = new ObjectChange { Type = typeof(JsonDocument), Key = key, Kind = ChangeKind.Deleted };
        return new ObjectResult { Changes = [change], Errors = [] };
    }

    /// <summary>
    /// Search dynamic documents using a Lucene query string.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="query">Optional Lucene query string (e.g. "Name:Tom AND Age:[20 TO 30]").</param>
    public IEnumerable<JsonDocument> Search(string schemaName, string? query = null)
    {
        ReloadSearcher();
        var schema = GetSchema(schemaName);
        var mapper = _dynamicMappers[schemaName];

        // Build query: type filter AND user query
        var boolQuery = new Lucene.Net.Search.BooleanQuery();
        boolQuery.Add(new Lucene.Net.Search.TermQuery(new Term("_type_", schema.StorageTypeName)),
            Lucene.Net.Search.Occur.MUST);

        if (!string.IsNullOrEmpty(query))
        {
            var parser = new FieldMappingQueryParser(
                LuceneVersion.LUCENE_48, mapper.DefaultSearchProperty, mapper.Analyzer, mapper);
            boolQuery.Add(parser.Parse(query), Lucene.Net.Search.Occur.MUST);
        }

        // Execute search using IndexWriter's reader
        using var reader = _indexWriter.GetReader(applyAllDeletes: true);
        var searcher = new Lucene.Net.Search.IndexSearcher(reader);
        var hits = searcher.Search(boolQuery, int.MaxValue);
        var results = new List<JsonDocument>();
        foreach (var hit in hits.ScoreDocs)
        {
            var doc = searcher.Doc(hit.Doc);
            var json = doc.Get(OBJECT_FIELD);
            if (json != null)
            {
                var jsonDoc = JsonDocument.Parse(json);
                var etag = doc.Get(ETAG_FIELD);
                if (etag != null) jsonDoc.SetETag(etag);
                var keyValue = doc.Get(KEY_FIELD);
                if (keyValue != null) jsonDoc.SetKey(keyValue);
                results.Add(jsonDoc);
            }
        }
        return results;
    }

    /// <summary>
    /// Get many dynamic documents from Table Storage with an optional OData filter on queryable columns.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="filter">Optional OData filter expression (e.g. "Age gt 20 and Name eq 'Tom'").</param>
    /// <param name="maxPerPage">Maximum items per page for the underlying Azure Table Storage query.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async IAsyncEnumerable<JsonDocument> GetManyAsync(string schemaName,
        string? filter = null, int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var schema = GetSchema(schemaName); // validate schema exists
        await foreach (var doc in _tableAdapter.GetManyJsonDocumentsAsync(_lottaCatalog.Name, schema.StorageTypeName, filter, maxPerPage, cancellationToken))
        {
            yield return doc;
        }
    }

    /// <summary>
    /// Delete multiple dynamic documents matching an optional OData filter.
    /// Deletes from Table Storage and removes from Lucene index.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="filter">Optional OData filter. If null, deletes all documents of this schema type.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<ObjectResult> DeleteManyAsync(string schemaName, string? filter = null, CancellationToken cancellationToken = default)
    {
        var schema = GetSchema(schemaName);
        var allChanges = new List<ObjectChange>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new List<string>();

        async Task FlushAsync()
        {
            await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
            lock (_lock)
            {
                foreach (var key in pendingKeys)
                    _indexWriter.DeleteDocuments([new Term(KEY_FIELD, key)]);
                _indexDirty = true;
            }
            foreach (var key in pendingKeys)
                allChanges.Add(new ObjectChange { Type = typeof(JsonDocument), Key = key, Kind = ChangeKind.Deleted });
            pendingActions.Clear();
            pendingKeys.Clear();
        }

        await foreach (var doc in _tableAdapter.GetManyJsonDocumentsAsync(
            _lottaCatalog.Name, schema.StorageTypeName, filter, cancellationToken: cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = doc.GetKey()!;

            pendingActions.Add(_tableAdapter.CreateDeleteAction(key));
            pendingKeys.Add(key);

            if (pendingActions.Count >= 100)
                await FlushAsync();
        }

        if (pendingActions.Count > 0)
            await FlushAsync();

        ScheduleRefresh();
        return new ObjectResult { Changes = allChanges };
    }

    /// <summary>
    /// Save (upsert) multiple dynamic JSON documents in bulk. Table storage writes are batched
    /// transactionally (auto-flushed at 100 ops or on duplicate key). Lucene indexing and
    /// OnSchema handlers run after each batch commit.
    /// </summary>
    /// <param name="schemaName">The registered schema name.</param>
    /// <param name="documents">The JSON documents to save.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<ObjectResult> SaveManyAsync(string schemaName, IEnumerable<JsonDocument> documents, CancellationToken cancellationToken = default)
    {
        var schema = GetSchema(schemaName);
        var mapper = _dynamicMappers[schemaName];
        var allChanges = new List<ObjectChange>();
        var allErrors = new List<Exception>();
        var pendingActions = new List<TableTransactionAction>();
        var pendingKeys = new HashSet<string>();
        var pendingDocs = new List<(JsonDocument doc, string key)>();

        async Task FlushAsync()
        {
            var etags = await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);

            // Annotate each document with its ETag from the batch response
            for (int i = 0; i < pendingDocs.Count; i++)
            {
                if (etags[i] != null)
                    pendingDocs[i].doc.SetETag(etags[i]!);
            }

            // Index in Lucene after batch commit
            lock (_lock)
            {
                foreach (var (doc, key) in pendingDocs)
                {
                    _indexWriter.DeleteDocuments([new Term(KEY_FIELD, key)]);
                    var document = new Document();
                    mapper.ToDocument(doc, document);
                    var etag = doc.GetETag();
                    if (etag != null)
                        document.Add(new StoredField(ETAG_FIELD, etag));
                    _indexWriter.AddDocument(document);
                }
                _indexDirty = true;
            }

            // Record changes after batch commit
            foreach (var (doc, key) in pendingDocs)
            {
                allChanges.Add(new ObjectChange { Type = typeof(JsonDocument), Key = key, Kind = ChangeKind.Saved });
            }

            pendingActions.Clear();
            pendingKeys.Clear();
            pendingDocs.Clear();
        }

        foreach (var json in documents)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var key = schema.GetKey(json);
            var doc = json;
            doc.SetKey(key);

            // Auto-flush on duplicate key
            if (pendingKeys.Contains(key))
                await FlushAsync();

            pendingActions.Add(_tableAdapter.CreateJsonDocumentUpsertAction(key, schema.StorageTypeName, doc, schema));
            pendingKeys.Add(key);
            pendingDocs.Add((doc, key));

            // Auto-flush at 100 operations
            if (pendingActions.Count >= 100)
                await FlushAsync();
        }

        // Flush remaining
        if (pendingActions.Count > 0)
            await FlushAsync();

        ScheduleRefresh();
        return new ObjectResult { Changes = allChanges, Errors = allErrors };
    }

    /// <summary>
    /// Loads all JsonDocumentType entities from Table Storage and registers their dynamic mappers.
    /// Called by LottaCatalog.GetDatabaseAsync after construction.
    /// </summary>
    internal async Task InitializeJsonDocumentTypesAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var schema in _tableAdapter.GetManyAsync<JsonDocumentType>(_lottaCatalog.Name, cancellationToken: cancellationToken))
        {
            RegisterJsonMetadata(schema);
        }
    }

    /// <summary>
    /// Registers (or re-registers) a dynamic schema's mapper from a JsonDocumentType entity.
    /// </summary>
    internal void RegisterJsonMetadata(JsonDocumentType schema)
    {
        var dynSchema = JsonMetadata.Parse(schema);
        _schemas[schema.Name] = dynSchema;
        var mapper = new JsonDocumentMapper(dynSchema, LuceneVersion.LUCENE_48, _lottaCatalog.Analyzer, _lottaCatalog.EmbeddingGenerator);
        _dynamicMappers[schema.Name] = mapper;

        // Merge the mapper's per-field analyzer into the IndexWriter
        lock (_lock)
        {
            var writerAnalyzer = (Lucene.Net.Linq.Analysis.PerFieldAnalyzer)_indexWriter.Analyzer;
            writerAnalyzer.Merge(mapper.Analyzer);
        }
    }

    /// <summary>Reindex only the documents for a specific dynamic schema.</summary>
    private async Task ReindexJsonMetadataAsync(string schemaName, CancellationToken cancellationToken = default)
    {
        if (!_schemas.TryGetValue(schemaName, out var dynSchema)) return;
        if (!_dynamicMappers.TryGetValue(schemaName, out var mapper)) return;

        var storageTypeName = dynSchema.StorageTypeName;
        lock (_lock)
        {
            _indexWriter.DeleteDocuments(new Term("_type_", storageTypeName));
            _indexDirty = true;
        }

        await foreach (var doc in _tableAdapter.GetManyJsonDocumentsAsync(
            _lottaCatalog.Name, storageTypeName, cancellationToken: cancellationToken))
        {
            var document = new Document();
            mapper.ToDocument(doc, document);
            var etag = doc.GetETag();
            if (etag != null)
                document.Add(new StoredField(ETAG_FIELD, etag));
            lock (_lock)
            {
                _indexWriter.AddDocument(document);
                _indexDirty = true;
            }
        }

        ReloadSearcher();
    }

    private JsonMetadata GetSchema(string schemaName)
    {
        if (_schemas.TryGetValue(schemaName, out var schema))
            return schema;
        throw new InvalidOperationException(
            $"Schema '{schemaName}' not registered. Save a JsonDocumentType with Name=\"{schemaName}\" first.");
    }

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
    internal void DeleteSearchIndex()
    {
        lock (_lock)
        {
            _indexWriter.DeleteAll();
            _indexWriter.Commit();
            _lucene.Refresh();
            _indexDirty = false;
        }
    }

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
                    await FlushTypedAsync();

                pendingActions.Add(_tableAdapter.CreateUpsertAction(key, entity, meta));
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
                var etags = await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
                // Annotate each entity with its ETag from the batch response
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
                    await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
                    await RunPendingDeleteHandlersAsync(pendingEntities, allChanges, allErrors, cancellationToken);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }

                pendingActions.Add(_tableAdapter.CreateDeleteAction(key));
                pendingKeys.Add(key);
                pendingEntities.Add((entity, type, key));

                if (pendingActions.Count >= 100)
                {
                    await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
                    await RunPendingDeleteHandlersAsync(pendingEntities, allChanges, allErrors, cancellationToken);
                    pendingActions.Clear();
                    pendingKeys.Clear();
                }
            }

            if (pendingActions.Count > 0)
            {
                await _tableAdapter.SubmitTransactionAsync(_lottaCatalog.Name, pendingActions, cancellationToken);
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
