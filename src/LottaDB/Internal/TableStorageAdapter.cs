using Azure;
using Azure.Data.Tables;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// Adapter over Azure.Data.Tables TableClient for storing POCOs as JSON + tags.
/// Works with both real Azure Table Storage and Spotflow in-memory fakes.
/// </summary>
internal class TableStorageAdapter
{
    private readonly TableServiceClient _serviceClient;
    private readonly Dictionary<string, TableClient> _tables = new();
    private readonly string _partitionKey;

    public TableStorageAdapter(TableServiceClient serviceClient, string partitionKey)
    {
        _serviceClient = serviceClient;
        _partitionKey = partitionKey;
    }

    private TableClient GetTable(string tableName)
    {
        if (tableName.Any(ch => !Char.IsAsciiLetterOrDigit(ch)) || tableName.Length > 62)
            throw new ArgumentException($"{tableName} is not a valid table name");
        if (!_tables.TryGetValue(tableName, out var client))
        {
            client = _serviceClient.GetTableClient(tableName);
            client.CreateIfNotExists();

            _tables[tableName] = client;
        }
        return client;
    }

    public async Task UpsertAsync(string tableName, string key, object obj, TypeMetadata meta, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entity = BuildEntity(key, obj, meta);
        await table.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Conditional replace using the supplied ETag. Returns false if another writer
    /// changed the row since it was read (HTTP 412 Precondition Failed) so the caller
    /// can re-read and retry. A missing row (404) bubbles up as an exception.
    /// </summary>
    public async Task<bool> TryReplaceAsync(string tableName, string key, object obj, TypeMetadata meta, string etag, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entity = BuildEntity(key, obj, meta);
        try
        {
            await table.UpdateEntityAsync(entity, new ETag(etag), TableUpdateMode.Replace, cancellationToken);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 412)
        {
            return false;
        }
    }

    private ITableEntity BuildEntity(string key, object obj, TypeMetadata meta)
    {
        var entity = new TableEntity(_partitionKey, key);
        entity[nameof(LottaTableEntity.Ty2pe)] = obj.GetType().FullName!;

        // Serialize as UTF-8 JSON bytes, split across properties if >64KB
        var bytes = JsonSerializer.SerializeToUtf8Bytes(obj, obj.GetType());
        LottaTableEntity.SetObjectBytes(entity, bytes);

        // Promote tags
        foreach (var tag in meta.Tags)
        {
            var value = tag.GetValue(obj);
            if (value != null)
                entity[tag.Name] = ConvertToTableValue(value);
        }
        return entity;
    }

    public async Task<(T? obj, string? etag)> GetAsync<T>(string tableName, string key, CancellationToken cancellationToken = default) where T : class, new()
    {
        var (entity, etag) = await GetAsync(tableName, key, cancellationToken);
        return (entity as T, etag);
    }

    internal async Task<(object? obj, string? etag)> GetAsync(string tableName, string key, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        try
        {
            var response = await table.GetEntityAsync<TableEntity>(_partitionKey, key, cancellationToken: cancellationToken);
            return (DeserializeEntity<object>(response.Value), response.Value.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return (null, null);
        }
    }

    public string? GetETag(string tableName, string key)
    {
        var table = GetTable(tableName);
        try
        {
            var response = table.GetEntity<TableEntity>(_partitionKey, key);
            return response.Value.ETag.ToString();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<bool> DeleteAsync(string tableName, string key)
    {
        var table = GetTable(tableName);
        try
        {
            await table.DeleteEntityAsync(_partitionKey, key);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
    }

    public IAsyncEnumerable<object> GetManyAsync(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.QueryAsync<TableEntity>(e => e.PartitionKey == _partitionKey,
                maxPerPage: maxPerPage,
                cancellationToken: cancellationToken)
            .Select(entity => DeserializeEntity<object>(entity)!);
    }

    public async IAsyncEnumerable<object> GetManyAsync(string tableName,
        IEnumerable<string> keys,
        int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var keyList = keys as IList<string> ?? keys.ToList();
        if (keyList.Count == 0)
            yield break;

        var table = GetTable(tableName);

        for (int offset = 0; offset < keyList.Count; offset += 100)
        {
            var batch = keyList.Skip(offset).Take(100);
            var keyFilter = string.Join(" or ", batch.Select(k => TableClient.CreateQueryFilter<TableEntity>(e => e.RowKey == k)));
            var query = $"PartitionKey eq '{_partitionKey}' and ({keyFilter})";
            await foreach (var entity in table.QueryAsync<TableEntity>(query,
                    maxPerPage: maxPerPage,
                    cancellationToken: cancellationToken))
            {
                yield return DeserializeEntity<object>(entity)!;
            }
        }
    }

    public IAsyncEnumerable<T> GetManyAsync<T>(string tableName,
        Expression<Func<T, bool>>? predicate = null,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        var table = GetTable(tableName);
        var query = GetODataQuery<T>(predicate);
        return table.QueryAsync<TableEntity>(query, maxPerPage: maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializeEntity<T>(entity)!);
    }

    public IAsyncEnumerable<object> GetAllAsync(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.QueryAsync<TableEntity>(e => e.PartitionKey == _partitionKey, maxPerPage: maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializeEntity<object>(entity)!);
    }

    private static T? DeserializeEntity<T>(TableEntity entity) where T : class
    {
        var bytes = LottaTableEntity.GetObjectBytes(entity);
        var typeName = entity.GetString("Type");
        var concreteType = TypeUtils.ResolveType(typeName);
        if (concreteType != null)
            return JsonSerializer.Deserialize(bytes, concreteType) as T;
        return JsonSerializer.Deserialize<T>(bytes);
    }

    private string GetODataQuery<T>(Expression<Func<T, bool>>? predicate = null) where T : class, new()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"PartitionKey eq '{_partitionKey}'");

        var derivedTypes = TypeUtils.GetDerivedTypes(typeof(T));
        if (derivedTypes.Any())
        {
            sb.Append($" and ({String.Join(" or ", derivedTypes.Select(t => $"Type eq '{t.FullName}'"))})");
        }
        if (predicate != null)
        {
            sb.Append($" and ({TableClient.CreateQueryFilter(predicate)})");
        }
        return sb.ToString();
    }

    public async Task DeleteTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        await table.DeleteAsync(cancellationToken);
    }

    public async Task ResetTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        await table.DeleteAsync(cancellationToken);
        _tables.Remove(tableName);
        GetTable(tableName); // re-creates via CreateIfNotExists
    }

    /// <summary>
    /// Deletes all rows in this adapter's partition without dropping the table.
    /// </summary>
    public async Task ResetPartitionAsync(string tableName, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entities = table.QueryAsync<TableEntity>(
            filter: $"PartitionKey eq '{_partitionKey}'",
            select: new[] { "PartitionKey", "RowKey" },
            cancellationToken: cancellationToken);

        var batch = new List<TableTransactionAction>();
        await foreach (var entity in entities)
        {
            batch.Add(new TableTransactionAction(TableTransactionActionType.Delete,
                new TableEntity(entity.PartitionKey, entity.RowKey) { ETag = ETag.All }));

            if (batch.Count >= 100)
            {
                await SubmitTransactionAsync(tableName, batch);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await SubmitTransactionAsync(tableName, batch);
    }

    /// <summary>
    /// Deletes all rows in this adapter's partition without dropping the table.
    /// Alias for <see cref="ResetPartitionAsync"/> for semantic clarity.
    /// </summary>
    public Task DeletePartitionAsync(string tableName, CancellationToken cancellationToken = default)
        => ResetPartitionAsync(tableName, cancellationToken);

    internal TableTransactionAction CreateUpsertAction(string key, object obj, TypeMetadata meta)
    {
        var entity = BuildEntity(key, obj, meta);
        return new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity);
    }

    internal TableTransactionAction CreateDeleteAction(string key)
    {
        return new TableTransactionAction(TableTransactionActionType.Delete,
            new TableEntity(_partitionKey, key) { ETag = ETag.All });
    }

    internal async Task SubmitTransactionAsync(string tableName, IReadOnlyList<TableTransactionAction> actions)
    {
        if (actions.Count == 0) return;
        var table = GetTable(tableName);
        try
        {
            await table.SubmitTransactionAsync(actions);
        }
        catch (Exception)
        {
            // Fallback for providers that don't support transactions (e.g., Spotflow in-memory)
            foreach (var action in actions)
            {
                switch (action.ActionType)
                {
                    case TableTransactionActionType.UpsertReplace:
                        await table.UpsertEntityAsync(action.Entity, TableUpdateMode.Replace);
                        break;
                    case TableTransactionActionType.Delete:
                        try { await table.DeleteEntityAsync(action.Entity.PartitionKey, action.Entity.RowKey); }
                        catch (RequestFailedException ex) when (ex.Status == 404) { }
                        break;
                }
            }
        }
    }

    private static object ConvertToTableValue(object value)
    {
        return value switch
        {
            string s => s,
            int i => i,
            long l => l,
            double d => d,
            bool b => b,
            Guid g => g,
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(dt),
            _ => value.ToString() ?? ""
        };
    }
}
