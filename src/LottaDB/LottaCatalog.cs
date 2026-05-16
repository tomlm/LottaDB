using System.Collections.Concurrent;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Lotta.Internal;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.En;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;
using Microsoft.Extensions.AI;
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
    private readonly ConcurrentDictionary<string, LottaDB> _databases = new();
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
    /// Factory for creating a <see cref="TableServiceClient"/>. Receives the catalog name.
    /// </summary>
    public Func<string, TableServiceClient> TableServiceClientFactory { get; set; }

    /// <summary>
    /// Factory for creating a Lucene Directory. Receives a composite path
    /// (<c>{catalog}/{databaseId}/Search</c>) to create per-database indexes.
    /// </summary>
    public Func<string, LuceneDirectory> LuceneDirectoryFactory { get; set; }

    /// <summary>
    /// Factory for creating a <see cref="BlobServiceClient"/>. Receives the catalog name.
    /// Used for the Blob storage API on each database.
    /// </summary>
    public Func<string, BlobServiceClient> BlobServiceClientFactory { get; set; }

    /// <summary>
    /// Embedding generator for vector similarity search, shared across all databases in this catalog.
    /// </summary>
    public IEmbeddingGenerator<string, Embedding<float>>? EmbeddingGenerator { get; set; }

    /// <summary>
    /// Default Analyzer to use for indexing/querying, shared across all databases in this catalog.
    /// </summary>
    public Analyzer Analyzer { get; set; } = new EnglishAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48);

    /// <summary>
    /// Create a catalog with an Azure Storage connection string.
    /// </summary>
    /// <param name="catalogName">Catalog name. Used as the Azure table name and blob container.</param>
    /// <param name="connectionString">Azure Storage connection string.</param>
    /// <param name="configure">Optional callback to configure catalog-level settings.</param>
    public LottaCatalog(string catalogName, string connectionString, Action<LottaCatalog>? configure = null)
    {
        Name = SanitizeName(catalogName);
        connectionString ??= "UseDevelopmentStorage=true";
        TableServiceClientFactory = name => new TableServiceClient(connectionString);
        LuceneDirectoryFactory = name => new AzureDirectory(connectionString, name, new RAMDirectory());
        BlobServiceClientFactory = name => new BlobServiceClient(connectionString);
        configure?.Invoke(this);
    }

    /// <summary>
    /// Create a catalog without a connection string. Requires setting
    /// <see cref="TableServiceClientFactory"/>, <see cref="LuceneDirectoryFactory"/>,
    /// and <see cref="BlobServiceClientFactory"/> manually.
    /// </summary>
    /// <param name="catalogName">Catalog name. Used as the Azure table name and blob container.</param>
    /// <param name="configure">Optional callback to configure catalog-level settings.</param>
    public LottaCatalog(string catalogName, Action<LottaCatalog>? configure = null)
    {
        Name = SanitizeName(catalogName);
        TableServiceClientFactory = _ => throw new InvalidOperationException("LottaCatalog.TableServiceClientFactory is not configured.");
        LuceneDirectoryFactory = _ => throw new InvalidOperationException("LottaCatalog.LuceneDirectoryFactory is not configured.");
        BlobServiceClientFactory = _ => throw new InvalidOperationException("LottaCatalog.BlobServiceClientFactory is not configured.");
        configure?.Invoke(this);
    }

    /// <summary>
    /// Get or create a database within this catalog.
    /// Returns the same instance for the same database ID.
    /// On first creation, writes a manifest row with the schema definition.
    /// If the schema has changed since the last run, the Lucene index is automatically rebuilt.
    /// </summary>
    /// <param name="databaseId">Database ID within this catalog.</param>
    /// <param name="configure">Configuration callback for registering types and handlers for this database.</param>
    public async Task<LottaDB> GetDatabaseAsync(string databaseId = "default", Action<ILottaConfiguration>? configure = null, CancellationToken cancellationToken = default)
    {
        if (_databases.TryGetValue(databaseId, out var existing))
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
            return existing;
        }

        var config = new LottaConfiguration();
        configure?.Invoke(config);
        var db = new LottaDB(this, databaseId, config);

        if (!_databases.TryAdd(databaseId, db))
        {
            db.Dispose();
            return _databases[databaseId];
        }

        // Load dynamic schemas from Table Storage before computing the schema hash
        await db.InitializeJsonDocumentTypesAsync(cancellationToken);

        // Compute current schema and compare with stored manifest
        var currentSchema = TypeMetadata.ComputeSchemaJson(db._metadata.Values, db._schemas.Values);
        var table = GetTableClient();
        string? storedSchema = null;

        try
        {
            var response = await table.GetEntityAsync<TableEntity>(ManifestPartitionKey, databaseId, cancellationToken: cancellationToken);
            if (response.Value.TryGetValue(SchemaColumn, out var schemaObj))
                storedSchema = schemaObj as string;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // No manifest row yet
        }

        // Write/update manifest with current schema
        var manifestEntity = new TableEntity(ManifestPartitionKey, databaseId)
        {
            { SchemaColumn, currentSchema }
        };
        await table.UpsertEntityAsync(manifestEntity, TableUpdateMode.Replace, cancellationToken);

        // If schema changed, rebuild the index
        if (storedSchema != null && storedSchema != currentSchema)
        {
            await db.RebuildSearchIndex(cancellationToken);
        }

        return db;
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
        var table = GetTableClient();
        await table.DeleteAsync(cancellationToken);

        // Clear cached clients so subsequent operations recreate them
        _tableClient = null;
        _tableServiceClient = null;
        _blobServiceClient = null;

        foreach (var db in _databases.Values)
            db.Dispose();
        _databases.Clear();
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

    /// Dispose all database instances managed by this catalog.
    /// </summary>
    public void Dispose()
    {
        foreach (var db in _databases.Values)
            db.Dispose();
        _databases.Clear();
    }

    internal TableServiceClient GetTableServiceClient()
    {
        _tableServiceClient ??= TableServiceClientFactory(Name);
        return _tableServiceClient;
    }

    internal BlobServiceClient GetBlobServiceClient()
    {
        _blobServiceClient ??= BlobServiceClientFactory(Name);
        return _blobServiceClient;
    }

    internal TableClient GetTableClient()
    {
        if (_tableClient == null)
        {
            _tableClient = GetTableServiceClient().GetTableClient(Name);
            _tableClient.CreateIfNotExists();
        }
        return _tableClient;
    }
}
