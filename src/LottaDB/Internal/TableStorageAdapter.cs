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

    public const string PK = "O";

    public TableStorageAdapter(TableServiceClient serviceClient)
    {
        _serviceClient = serviceClient;
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

    public async Task UpsertAsync(string tableName, string key, object obj, TypeMetadata meta)
    {
        var table = GetTable(tableName);
        var entity = BuildEntity(key, obj, meta);
        await table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
    }

    /// <summary>
    /// Conditional replace using the supplied ETag. Returns false if another writer
    /// changed the row since it was read (HTTP 412 Precondition Failed) so the caller
    /// can re-read and retry. A missing row (404) bubbles up as an exception.
    /// </summary>
    public async Task<bool> TryReplaceAsync(string tableName, string key, object obj, TypeMetadata meta, string etag)
    {
        var table = GetTable(tableName);
        var entity = BuildEntity(key, obj, meta);
        try
        {
            await table.UpdateEntityAsync(entity, new ETag(etag), TableUpdateMode.Replace);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 412)
        {
            return false;
        }
    }

    private static ITableEntity BuildEntity(string key, object obj, TypeMetadata meta)
    {
        var entity = new TableEntity(TableStorageAdapter.PK, key);
        entity[nameof(LottaTableEntity.Type)] = obj.GetType().FullName!;
        entity[nameof(LottaTableEntity.Json)] = JsonSerializer.Serialize(obj, obj.GetType());

        // Promote tags
        foreach (var tag in meta.Tags)
        {
            var value = tag.GetValue(obj);
            if (value != null)
                entity[tag.Name] = ConvertToTableValue(value);
        }
        return entity;
    }

    public async Task<(T? obj, string? etag)> GetAsync<T>(string tableName, string key) where T : class, new()
    {
        var table = GetTable(tableName);
        try
        {
            var response = await table.GetEntityAsync<LottaTableEntity>(PK, key);
            var entity = response.Value;
            var entityType = TypeUtils.ResolveType(entity.Type);
            if (!entityType.IsAssignableTo(typeof(T)))
                return (null, null);
            var obj = JsonSerializer.Deserialize<T>(entity.Json);
            // Prefer the entity's ETag (Azure SDK assigns it via the ITableEntity.ETag setter);
            // fall back to the raw HTTP ETag header if the property round-trip produced nothing.
            var etag = entity.ETag == default ? response.GetRawResponse().Headers.ETag?.ToString() : entity.ETag.ToString();
            return (obj, etag);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return (null, null);
        }
    }

    internal async Task<(object? obj, Type? type)> GetAsync(string tableName, string key)
    {
        var table = GetTable(tableName);
        try
        {
            var response = await table.GetEntityAsync<TableEntity>(PK, key, select: new[] { "Type", "Json" });
            var entity = response.Value;
            var typeName = entity.GetString("Type");
            var json = entity.GetString("Json");
            var type = TypeUtils.ResolveType(typeName);
            if (type == null) return (null, null);
            var obj = JsonSerializer.Deserialize(json, type);
            return (obj, type);
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
            var response = table.GetEntity<TableEntity>(PK, key);
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
            await table.DeleteEntityAsync(PK, key);
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
        return table.QueryAsync<LottaTableEntity>(e => e.PartitionKey == PK,
                maxPerPage: maxPerPage,
                cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<object>(entity.Json, entity.Type)!);
    }

    public IAsyncEnumerable<object> GetManyAsync(string tableName,
        IEnumerable<string> keys,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var keyList = keys as IList<string> ?? keys.ToList();
        if (keyList.Count == 0)
            return AsyncEnumerable.Empty<object>();

        var table = GetTable(tableName);

        var keyFilter = string.Join(" or ", keyList.Select(k => TableClient.CreateQueryFilter<LottaTableEntity>(e => e.RowKey == k)));
        var query = $"PartitionKey eq '{PK}' and ({keyFilter})";
        return table.QueryAsync<LottaTableEntity>(query,
                maxPerPage: maxPerPage,
                cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<object>(entity.Json, entity.Type)!);
    }

    public IAsyncEnumerable<T> GetManyAsync<T>(string tableName,
        Expression<Func<T, bool>>? predicate = null,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        var table = GetTable(tableName);
        var query = GetODataQuery<T>(predicate);
        return table.QueryAsync<LottaTableEntity>(query, maxPerPage: maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<T>(entity.Json, entity.Type)!);
    }

    public IAsyncEnumerable<string> GetKeysAsync<T>(string tableName,
        Expression<Func<T, bool>>? predicate = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        var table = GetTable(tableName);
        var query = GetODataQuery<T>(predicate);
        return table.QueryAsync<TableEntity>(query, select: new[] { "RowKey" }, cancellationToken: cancellationToken)
            .Select(entity => entity.RowKey);
    }

    public IAsyncEnumerable<object> GetAllAsync(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.QueryAsync<LottaTableEntity>(e => e.PartitionKey == PK, maxPerPage: maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<object>(entity.Json, entity.Type)!);
    }

    private static string GetODataQuery<T>(Expression<Func<T, bool>>? predicate = null) where T : class, new()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"PartitionKey eq '{PK}'");

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

    /// <summary>
    /// Deserialize JSON using the concrete type from _type field.
    /// Falls back to T if the concrete type can't be resolved.
    /// </summary>
    private static T? DeserializePolymorphic<T>(string json, string typeName) where T : class
    {
        // Try to find the concrete type in loaded assemblies
        var concreteType = TypeUtils.ResolveType(typeName);
        if (concreteType != null)
        {
            var obj = JsonSerializer.Deserialize(json, concreteType);
            return obj as T;
        }

        // Fallback: deserialize as T directly
        return JsonSerializer.Deserialize<T>(json);
    }

    public async Task DeleteTableAsync(string tableName, CancellationToken ct = default)
    {
        var table = GetTable(tableName);
        await table.DeleteAsync(ct);
    }

    public async Task ResetTableAsync(string tableName, CancellationToken ct = default)
    {
        var table = GetTable(tableName);
        await table.DeleteAsync(ct);
        _tables.Remove(tableName);
        GetTable(tableName); // re-creates via CreateIfNotExists
    }

    internal static TableTransactionAction CreateUpsertAction(string key, object obj, TypeMetadata meta)
    {
        var entity = BuildEntity(key, obj, meta);
        return new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity);
    }

    internal static TableTransactionAction CreateDeleteAction(string key)
    {
        return new TableTransactionAction(TableTransactionActionType.Delete,
            new TableEntity(PK, key) { ETag = ETag.All });
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
