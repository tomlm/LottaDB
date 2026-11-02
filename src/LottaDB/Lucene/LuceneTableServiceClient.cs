using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using System.Collections.Concurrent;

namespace Lotta;

/// <summary>
/// Table storage served from the Lucene search index. Each database partition is stored in that
/// database's own index: writes go through the database's indexing pipeline (so LottaDB does not
/// register its post-write indexing handlers), and reads come back out of the index. Rows that
/// belong to no database — the catalog manifest — live in a small catalog-level index.
/// Configure with <see cref="LottaLuceneExtensions.UseLucene"/>.
/// </summary>
public sealed class LuceneTableServiceClient : TableServiceClient
{
    private readonly ConcurrentDictionary<string, LottaDB> _databases = new();
    private readonly ConcurrentDictionary<string, LuceneTableClient> _tables = new();

    internal LuceneTableServiceClient(LottaCatalog catalog) : base()
    {
        Catalog = catalog;
    }

    internal LottaCatalog Catalog { get; }

    /// <summary>
    /// Route a partition to the database whose index stores it. The first live database for a
    /// partition owns it; a duplicate instance (which the catalog disposes straight away) does not.
    /// </summary>
    internal void Attach(string partitionKey, LottaDB database) => _databases.TryAdd(partitionKey, database);

    internal void Detach(string partitionKey, LottaDB database)
        => _databases.TryRemove(KeyValuePair.Create(partitionKey, database));

    internal LottaDB? GetDatabase(string partitionKey) => _databases.GetValueOrDefault(partitionKey);

    internal IEnumerable<KeyValuePair<string, LottaDB>> Databases => _databases;

    /// <inheritdoc/>
    public override string AccountName => "lucene";

    /// <inheritdoc/>
    public override TableClient GetTableClient(string tableName)
        => _tables.GetOrAdd(tableName, name => new LuceneTableClient(this, name));

    /// <inheritdoc/>
    public override Response<TableItem> CreateTableIfNotExists(string tableName, CancellationToken cancellationToken = default)
        => GetTableClient(tableName).CreateIfNotExists(cancellationToken);

    /// <inheritdoc/>
    public override Task<Response<TableItem>> CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
        => GetTableClient(tableName).CreateIfNotExistsAsync(cancellationToken);

    /// <inheritdoc/>
    public override Response<TableItem> CreateTable(string tableName, CancellationToken cancellationToken = default)
        => GetTableClient(tableName).Create(cancellationToken);

    /// <inheritdoc/>
    public override Task<Response<TableItem>> CreateTableAsync(string tableName, CancellationToken cancellationToken = default)
        => GetTableClient(tableName).CreateAsync(cancellationToken);

    /// <inheritdoc/>
    public override Response DeleteTable(string tableName, CancellationToken cancellationToken = default)
        => GetTableClient(tableName).Delete(cancellationToken);

    /// <inheritdoc/>
    public override Task<Response> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
        => GetTableClient(tableName).DeleteAsync(cancellationToken);
}
