using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Lotta.Internal;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.En;
using Microsoft.Extensions.AI;
using System.Collections.Concurrent;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// A catalog groups multiple <see cref="LottaDB"/> databases under a single Azure Table
/// and blob container. Each database is isolated via its own partition key and Lucene index.
/// The catalog owns shared infrastructure (storage clients, embedding generator, analyzer)
/// while each database has its own type registrations and handlers.
/// </summary>
public class LottaCatalog : IDisposable
{
    /// <summary>The databases fully open: schemas loaded, any rebuild done, handlers started.</summary>
    private readonly ConcurrentDictionary<string, LottaDB> _databases = new();
    /// <summary>Opens in progress, shared by everyone asking for the same database meanwhile.</summary>
    private readonly ConcurrentDictionary<string, Task<LottaDB>> _opening = new();
    /// <summary>
    /// The token opens run under: the catalog's, cancelled when it is disposed, rather than that of
    /// whichever caller happened to start the open. Never disposed, so a second Dispose can still cancel it.
    /// </summary>
    private readonly CancellationTokenSource _closing = new();
    /// <summary>
    /// Taken by an open to keep its database, and by <see cref="Dispose"/> and <see cref="DeleteAsync"/>
    /// to take the databases kept, so none is kept after they have looked.
    /// </summary>
    private readonly object _lifetime = new();
    private volatile bool _disposed;
    /// <summary>How many times a <see cref="DeleteAsync"/> has started or ended.</summary>
    private int _deletionEvents;
    /// <summary>How many <see cref="DeleteAsync"/> calls are in progress.</summary>
    private int _deleting;
    private TableServiceClient? _tableServiceClient;
    private TableClient? _tableClient;
    private BlobServiceClient? _blobServiceClient;
    internal const string ManifestPartitionKey = "__database__";
    internal const string SchemaColumn = "Schema";

    /// <summary>
    /// The catalog name. Used as the Azure Table name and blob container name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Factory for creating a <see cref="TableServiceClient"/>.
    /// Set via the constructor's <c>configure</c> callback.
    /// </summary>
    /// <summary>
    /// Factory for creating a <see cref="TableServiceClient"/>.
    /// Set via the constructor's <c>configure</c> callback.
    /// </summary>
    public Func<TableServiceClient> TableServiceClientFactory { get; set; }
        = () => throw new InvalidOperationException("LottaCatalog.TableServiceClientFactory is not configured.");

    /// <summary>
    /// Factory for creating a <see cref="BlobServiceClient"/>.
    /// Set via the constructor's <c>configure</c> callback.
    /// </summary>
    public Func<BlobServiceClient> BlobServiceClientFactory { get; set; }
        = () => throw new InvalidOperationException("LottaCatalog.BlobServiceClientFactory is not configured.");

    /// <summary>
    /// Factory for creating a <see cref="QueueServiceClient"/>, or null when the storage provider
    /// has no queue storage. Queued rules (<c>When&lt;T&gt;().DoQueued(...)</c>, <c>.ModifyQueued(...)</c>) need one.
    /// </summary>
    public Func<QueueServiceClient>? QueueServiceClientFactory { get; set; }

    /// <summary>
    /// Factory for creating the Lucene Directory. Receives a composite path
    /// (<c>{catalog}/{databaseId}/Search</c>) to create per-database indexes.
    /// Set by the storage provider; see <see cref="LuceneDirectories"/> for ready-made factories.
    /// </summary>
    public Func<string, LuceneDirectory> LuceneDirectoryFactory { get; set; }
        = (_) => throw new InvalidOperationException($"LottaCatalog.LuceneDirectoryFactory is not configured");

    /// <summary>
    /// The storage provider registered with <see cref="UseStorage"/> (e.g. "Memory", "Lucene"),
    /// or null if none has been.
    /// </summary>
    public string? StorageProvider { get; private set; }

    /// <summary>
    /// Register this catalog's storage provider: table storage, blob storage and Lucene directories.
    /// Each <c>Use…()</c> provider calls this, and a catalog has exactly one provider — registering
    /// a second throws.
    /// </summary>
    /// <param name="provider">Provider name, used in error messages.</param>
    /// <param name="tableServiceClientFactory">Sets <see cref="TableServiceClientFactory"/>.</param>
    /// <param name="blobServiceClientFactory">Sets <see cref="BlobServiceClientFactory"/>.</param>
    /// <param name="luceneDirectoryFactory">Sets <see cref="LuceneDirectoryFactory"/>.</param>
    /// <exception cref="InvalidOperationException">A storage provider is already registered.</exception>
    public LottaCatalog UseStorage(
        string provider,
        Func<TableServiceClient> tableServiceClientFactory,
        Func<BlobServiceClient> blobServiceClientFactory,
        Func<string, LuceneDirectory> luceneDirectoryFactory)
    {
        if (StorageProvider != null)
            throw new InvalidOperationException(
                $"Catalog '{Name}' already uses {StorageProvider} storage, so it cannot also use {provider}. " +
                $"Each Use…() provider configures tables, blobs and the Lucene index, so register only one.");

        StorageProvider = provider;
        TableServiceClientFactory = tableServiceClientFactory;
        BlobServiceClientFactory = blobServiceClientFactory;
        LuceneDirectoryFactory = luceneDirectoryFactory;
        return this;
    }

    /// <summary>
    /// Register this catalog's storage provider, including queue storage, which queued rules
    /// (<c>When&lt;T&gt;().DoQueued(...)</c>, <c>.ModifyQueued(...)</c>) run from.
    /// </summary>
    /// <param name="provider">Provider name, used in error messages.</param>
    /// <param name="tableServiceClientFactory">Sets <see cref="TableServiceClientFactory"/>.</param>
    /// <param name="blobServiceClientFactory">Sets <see cref="BlobServiceClientFactory"/>.</param>
    /// <param name="queueServiceClientFactory">Sets <see cref="QueueServiceClientFactory"/>.</param>
    /// <param name="luceneDirectoryFactory">Sets <see cref="LuceneDirectoryFactory"/>.</param>
    /// <exception cref="InvalidOperationException">A storage provider is already registered.</exception>
    public LottaCatalog UseStorage(
        string provider,
        Func<TableServiceClient> tableServiceClientFactory,
        Func<BlobServiceClient> blobServiceClientFactory,
        Func<QueueServiceClient> queueServiceClientFactory,
        Func<string, LuceneDirectory> luceneDirectoryFactory)
    {
        UseStorage(provider, tableServiceClientFactory, blobServiceClientFactory, luceneDirectoryFactory);
        QueueServiceClientFactory = queueServiceClientFactory;
        return this;
    }

    /// <summary>
    /// Embedding generator for vector similarity search, shared across all databases in this catalog.
    /// Set via the constructor's <c>configure</c> callback, before any database is opened.
    /// Required for <c>.Similar()</c> queries; vector fields are indexed without embeddings when it is null.
    /// </summary>
    public IEmbeddingGenerator<string, Embedding<float>>? EmbeddingGenerator { get; set; }

    /// <summary>
    /// Default Analyzer to use for indexing/querying, shared across all databases in this catalog.
    /// Set via the constructor's <c>configure</c> callback.
    /// </summary>
    internal Analyzer Analyzer { get; set; } = new EnglishAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48);

    /// <summary>
    /// Create a catalog with an Azure Storage connection string. This registers Azure storage
    /// (<see cref="LottaCatalogExtensions.UseAzure"/>), so <paramref name="configure"/> must not
    /// register another storage provider.
    /// </summary>
    /// <param name="catalogName">Catalog name. Used as the Azure table name and blob container.</param>
    /// <param name="connectionString">Azure Storage connection string.</param>
    /// <param name="configure">Optional callback to configure catalog-level settings.</param>
    public LottaCatalog(string catalogName, string connectionString, Action<LottaCatalog>? configure = null)
    {
        Name = SanitizeName(catalogName);
        connectionString ??= "UseDevelopmentStorage=true";
        this.UseAzure(connectionString);
        configure?.Invoke(this);
    }

    /// <summary>
    /// Create a catalog without a connection string. Requires setting
    /// <see cref="TableServiceClientFactory"/>, <see cref="AzureDirectoryCacheFactory"/>,
    /// and <see cref="BlobServiceClientFactory"/> manually.
    /// </summary>
    /// <param name="catalogName">Catalog name. Used as the Azure table name and blob container.</param>
    /// <param name="configure">Optional callback to configure catalog-level settings.</param>
    public LottaCatalog(string catalogName, Action<LottaCatalog>? configure = null)
    {
        Name = SanitizeName(catalogName);

        configure?.Invoke(this);
    }

    /// <summary>
    /// Get or create a database within this catalog.
    /// Returns the same instance for the same database ID.
    /// On first creation, writes a manifest row with the schema definition.
    /// If the schema has changed since the last run, the Lucene index is automatically rebuilt.
    /// A database is kept only once it is fully open: if opening fails, nothing is kept and the
    /// next call opens it again from scratch.
    /// </summary>
    /// <param name="databaseId">Database ID within this catalog.</param>
    /// <param name="configure">Configuration callback for registering types and handlers for this database.</param>
    /// <param name="cancellationToken">
    /// Stops this call waiting. Callers asking for a database while it is being opened share one
    /// open, which runs on whether or not they cancel, and is kept for the next caller.
    /// </param>
    /// <exception cref="ObjectDisposedException">The catalog is disposed, or was disposed during the open.</exception>
    /// <exception cref="InvalidOperationException">
    /// The schema differs from that of the database already open, or the catalog was deleted
    /// (<see cref="DeleteAsync"/>) during the open; the next call opens it again.
    /// </exception>
    public async Task<LottaDB> GetDatabaseAsync(string databaseId = "default", Action<ILottaConfiguration>? configure = null, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_databases.TryGetValue(databaseId, out var existing))
        {
            CheckSameSchema(existing, databaseId, configure);
            return existing;
        }

        // Callers that ask while the database is being opened wait for that same open. It runs
        // under the catalog's token, and each caller waits with its own: one that gives up stops
        // waiting without failing the open for the others. An open that every caller has given up
        // on still finishes and is kept, as the simplest thing: its work, perhaps an index
        // rebuild, is not thrown away, and the next caller gets the database at once.
        var mine = new TaskCompletionSource<LottaDB>(TaskCreationOptions.RunContinuationsAsynchronously);
        var opening = _opening.GetOrAdd(databaseId, mine.Task);
        // Not on this caller's thread: parts of opening block (creating the queues), and the
        // caller must be free to stop waiting.
        if (opening == mine.Task)
            _ = Task.Run(() => ShareOpenAsync(databaseId, configure, mine));
        var db = await opening.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (opening != mine.Task)
            CheckSameSchema(db, databaseId, configure);
        return db;
    }

    /// <summary>
    /// Runs one open for everyone waiting on <paramref name="opened"/>, and takes it off
    /// <see cref="_opening"/> when it ends. That is done by the open and not by its callers,
    /// because a caller that gives up must not let a second open of the database start meanwhile.
    /// </summary>
    private async Task ShareOpenAsync(string databaseId, Action<ILottaConfiguration>? configure, TaskCompletionSource<LottaDB> opened)
    {
        var entry = new KeyValuePair<string, Task<LottaDB>>(databaseId, opened.Task);
        LottaDB db;
        try
        {
            db = await OpenDatabaseAsync(databaseId, configure, _closing.Token).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // Off the list before anyone hears of it, so a failed open is never handed to a later caller.
            _opening.TryRemove(entry);
            opened.SetException(e);
            return;
        }
        // By now the database is in _databases.
        _opening.TryRemove(entry);
        opened.SetResult(db);
    }

    /// <summary>Throws if <paramref name="configure"/> describes a different schema from the open database's.</summary>
    private static void CheckSameSchema(LottaDB existing, string databaseId, Action<ILottaConfiguration>? configure)
    {
        // Detect schema mismatch if configure is provided on a subsequent call
        if (configure != null)
        {
            var checkConfig = new LottaConfiguration();
            configure.Invoke(checkConfig);
            var existingSchema = TypeMetadata.ComputeSchemaJson(existing._metadata.Values);
            var newSchema = TypeMetadata.ComputeSchemaFromConfig(checkConfig);
            if (existingSchema != newSchema)
                throw new InvalidOperationException(
                    $"Database '{databaseId}' already exists with a different schema. " +
                    $"GetDatabaseAsync was called twice in the same session with conflicting configurations.");
        }
    }

    /// <summary>
    /// Opens a database and adds it to <see cref="_databases"/> once it is fully open. A database
    /// that fails to open, or whose catalog is disposed or deleted while it opens, is disposed and
    /// not kept.
    /// </summary>
    private async Task<LottaDB> OpenDatabaseAsync(string databaseId, Action<ILottaConfiguration>? configure, CancellationToken cancellationToken)
    {
        // Before anything is written: a DeleteAsync from here on may delete what this open writes.
        int deletionEvents;
        lock (_lifetime)
            deletionEvents = _deleting == 0 ? _deletionEvents : -1;

        // Opened by a caller whose open finished just before this one began.
        if (_databases.TryGetValue(databaseId, out var existing))
        {
            CheckSameSchema(existing, databaseId, configure);
            return existing;
        }

        var config = new LottaConfiguration();
        configure?.Invoke(config);
        config.CheckRules();
        config.Opened = true;
        // Before the database is built: building it creates an index that would then be left behind.
        if (config.QueuedRules.Count > 0 && QueueServiceClientFactory == null)
            throw new NotSupportedException(
                $"Queued rules (.DoQueued, .ModifyQueued, .Materialize, .After, .At) need queue storage, which {StorageProvider ?? "this catalog's"} storage does not provide. " +
                $"Use .Do for inline rules, or give the catalog queue storage: every built-in Use…() provider " +
                $"has it, and UseStorage has an overload that takes a queue client factory.");
        var db = new LottaDB(this, databaseId, config);
        try
        {
            await InitializeDatabaseAsync(db, databaseId, config, cancellationToken);
            await db.PrepareRulesAsync(cancellationToken);
        }
        catch (Exception e)
        {
            // Kept, it would be a database that saves but queues no handler work, with no error.
            db.Dispose();
            if (_disposed)
                throw new ObjectDisposedException($"Catalog '{Name}' was disposed while database '{databaseId}' was being opened.", e);
            throw;
        }

        // Checked and kept under the lock Dispose and DeleteAsync take the databases under, so
        // neither can miss this database, nor run between the check and keeping it.
        lock (_lifetime)
        {
            if (!_disposed && _deleting == 0 && deletionEvents == _deletionEvents)
            {
                _databases[databaseId] = db;
                // Only now, and only once kept: the schemas are loaded and any rebuild is done, so
                // a queued handler sees a ready database; and one not kept runs no handler.
                db.StartRules();
                return db;
            }
        }
        // Kept, it would be a live database nobody disposes (its handlers running on), or one
        // whose manifest, queues or index the deletion removed under it.
        db.Dispose();
        if (_disposed)
            throw new ObjectDisposedException($"Catalog '{Name}' was disposed while database '{databaseId}' was being opened.");
        throw new InvalidOperationException($"Catalog '{Name}' was deleted while database '{databaseId}' was being opened. Open it again.");
    }

    /// <summary>Loads the schemas, and checks the manifest and rebuilds the index if the schema changed.</summary>
    private async Task InitializeDatabaseAsync(LottaDB db, string databaseId, LottaConfiguration config, CancellationToken cancellationToken)
    {
        // Load dynamic schemas from Table Storage before computing the schema hash
        await db.InitializeJsonSchemasAsync(cancellationToken);

        // Compute current schema and compare with stored manifest
        var currentSchema = TypeMetadata.ComputeSchemaJson(db._metadata.Values, db._schemas.Values);

        // A read-only replica writes nothing at open — no manifest, no index rebuild.
        if (config.ReadOnly)
            return;

        var table = GetTableClient();
        string? storedSchema = null;
        ETag manifestETag = default;

        try
        {
            var response = await table.GetEntityAsync<TableEntity>(ManifestPartitionKey, databaseId, cancellationToken: cancellationToken);
            manifestETag = response.Value.ETag;
            if (response.Value.TryGetValue(SchemaColumn, out var schemaObj))
                storedSchema = schemaObj as string;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // No manifest row yet
        }

        var manifestEntity = new TableEntity(ManifestPartitionKey, databaseId)
        {
            { SchemaColumn, currentSchema }
        };

        if (storedSchema == null)
        {
            // First ever open of this database.
            await table.UpsertEntityAsync(manifestEntity, TableUpdateMode.Replace, cancellationToken);
        }
        else if (storedSchema != currentSchema)
        {
            // The schema changed. Several servers may be rolling out the new build at once,
            // and they would all try to rebuild — with all but one failing to get the write
            // lock at startup. Use the manifest ETag to elect exactly one rebuilder.
            bool ownsRebuild;
            try
            {
                await table.UpdateEntityAsync(manifestEntity, manifestETag, TableUpdateMode.Replace, cancellationToken);
                ownsRebuild = true;
            }
            catch (RequestFailedException ex) when (ex.Status == 412)
            {
                ownsRebuild = false;   // another server won the race and is rebuilding
            }

            if (ownsRebuild)
            {
                try
                {
                    await db.RebuildSearchIndex(cancellationToken);
                }
                catch
                {
                    // The manifest already advertises the new schema, so without a rollback a
                    // failed rebuild (cancelled, lost the writer, crashed mid-pass) would leave
                    // a stale or half-cleared index permanently associated with it — every
                    // later open would compare equal and skip the rebuild. Put the old schema
                    // back so the next process retries.
                    try
                    {
                        var rollback = new TableEntity(ManifestPartitionKey, databaseId)
                        {
                            { SchemaColumn, storedSchema }
                        };
                        await table.UpsertEntityAsync(rollback, TableUpdateMode.Replace, cancellationToken);
                    }
                    catch
                    {
                        // Rollback is best-effort — surface the original rebuild failure.
                    }
                    throw;
                }
            }
        }
        // storedSchema == currentSchema: nothing to write.
    }

    /// <summary>
    /// List all database IDs registered in this catalog.
    /// </summary>
    public async Task<IReadOnlyList<string>> ListAsync(CancellationToken cancellationToken = default)
    {
        var table = GetTableClient();
        var databases = new List<string>();
        try
        {
            await foreach (var entity in table.QueryAsync<TableEntity>(
                filter: $"PartitionKey eq '{ManifestPartitionKey}'",
                select: new[] { "RowKey" },
                cancellationToken: cancellationToken))
            {
                databases.Add(entity.RowKey);
            }
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Table doesn't exist yet
        }
        return databases;
    }

    /// <summary>
    /// Remove a database's manifest entry. Called by <see cref="LottaDB.DeleteDatabaseAsync"/>.
    /// </summary>
    internal async Task RemoveDatabaseManifestAsync(string databaseId, CancellationToken cancellationToken = default)
    {
        var table = GetTableClient();
        try
        {
            await table.DeleteEntityAsync(ManifestPartitionKey, databaseId, ETag.All, cancellationToken);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // Already gone
        }
        _databases.TryRemove(databaseId, out _);
    }

    /// <summary>
    /// Delete the entire catalog: drops the Azure Table (affecting ALL databases) and disposes all database instances.
    /// </summary>
    public async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        // An open in progress, or one starting before the deletion is done, keeps nothing:
        // what it writes may be deleted under it.
        LottaDB[] open;
        lock (_lifetime)
        {
            _deleting++;
            _deletionEvents++;
            open = [.. _databases.Values];
            _databases.Clear();
        }
        try
        {
            // Dispose databases first to release Lucene locks before deleting storage
            foreach (var db in open)
                db.Dispose();

            var table = GetTableClient();
            await table.DeleteAsync(cancellationToken);

            var blob = GetBlobServiceClient().GetBlobContainerClient(this.Name);
            await blob.DeleteIfExistsAsync(cancellationToken: cancellationToken);

            // Every database's handler queues, opened in this session or not, share the catalog's prefix.
            if (GetQueueServiceClient() is { } queues)
                await Internal.RuleQueueNames.DeleteQueuesAsync(queues, Internal.RuleQueueNames.CatalogPrefix(Name), cancellationToken);

            // Subsequent operations recreate the clients
            ReleaseStorageClients();
        }
        finally
        {
            lock (_lifetime)
            {
                _deleting--;
                _deletionEvents++;
            }
        }
    }

    /// <summary>
    /// Let go of the storage clients; the next use asks the factories again. A client that holds
    /// resources is disposed: the Lucene ones hold their Lucene <c>Directory</c> objects.
    /// </summary>
    private void ReleaseStorageClients()
    {
        (_tableServiceClient as IDisposable)?.Dispose();
        (_blobServiceClient as IDisposable)?.Dispose();
        (_queueServiceClient as IDisposable)?.Dispose();
        _tableClient = null;
        _tableServiceClient = null;
        _blobServiceClient = null;
        _queueServiceClient = null;
    }

    /// <summary>
    /// <summary>
    /// Sanitizes a catalog name for use as an Azure Table name and blob container name.
    /// Azure Table: alphanumeric only, 3-63 chars. Blob container: lowercase alphanumeric + hyphens, 3-63 chars.
    /// We use the stricter intersection: lowercase alphanumeric, 3-63 chars.
    /// </summary>
    private static string SanitizeName(string name)
    {
        var sanitized = new string(name.Where(char.IsLetterOrDigit).ToArray()).ToLowerInvariant();
        if (sanitized.Length < 3)
            sanitized = sanitized.PadRight(3, 'x');
        if (sanitized.Length > 63)
            sanitized = sanitized[..63];
        return sanitized;
    }

    /// Dispose all database instances managed by this catalog, and its storage clients.
    /// </summary>
    public void Dispose()
    {
        // From here on an open in progress keeps nothing; it disposes its database and throws.
        LottaDB[] open;
        lock (_lifetime)
        {
            _disposed = true;
            open = [.. _databases.Values];
            _databases.Clear();
        }
        _closing.Cancel();
        foreach (var db in open)
            db.Dispose();
        ReleaseStorageClients();
    }

    internal TableServiceClient GetTableServiceClient()
    {
        _tableServiceClient ??= TableServiceClientFactory();
        return _tableServiceClient;
    }

    private QueueServiceClient? _queueServiceClient;

    /// <summary>The queue storage client, or null when the storage provider has none.</summary>
    internal QueueServiceClient? GetQueueServiceClient()
    {
        if (_queueServiceClient == null && QueueServiceClientFactory != null)
            _queueServiceClient = QueueServiceClientFactory();
        return _queueServiceClient;
    }

    internal BlobServiceClient GetBlobServiceClient()
    {
        _blobServiceClient ??= BlobServiceClientFactory();
        return _blobServiceClient;
    }

    internal TableClient GetTableClient()
    {
        if (_tableClient == null)
        {
            _tableClient = GetTableServiceClient().GetTableClient(this.Name);
            _tableClient.CreateIfNotExists();
        }
        return _tableClient;
    }
}
