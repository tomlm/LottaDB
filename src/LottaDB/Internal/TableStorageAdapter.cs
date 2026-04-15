using Azure;
using Azure.Data.Tables;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// Adapter over Azure.Data.Tables TableClient for storing POCOs as JSON + tags.
/// Wokeys with both real Azure Table Storage and Spotflow in-memory fakes.
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
        if (!_tables.TryGetValue(tableName, out var client))
        {
            _serviceClient.CreateTableIfNotExists(tableName);
            client = _serviceClient.GetTableClient(tableName);
            _tables[tableName] = client;
        }
        return client;
    }

    public async Task UpsertAsync(string tableName, string key, object obj, TypeMetadata meta)
    {
        var table = GetTable(tableName);
        var entity = new LottaTableEntity(key, obj);

        // Promote tags
        foreach (var tag in meta.Tags)
        {
            var value = tag.GetValue(obj);
            if (value != null)
                entity[tag.Name] = ConvertToTableValue(value);
        }

        await table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
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
            return (obj, entity.ETag.ToString());
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

    public IAsyncEnumerable<object> QueryAsync(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.QueryAsync<LottaTableEntity>(e => e.PartitionKey == PK,
                maxPerPage: maxPerPage,
                cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<object>(entity.Json, entity.Type)!);
    }

    public IAsyncEnumerable<T> QueryAsync<T>(string tableName,
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

    public IEnumerable<object> Query(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.Query<LottaTableEntity>(e => e.PartitionKey == PK, maxPerPage: maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<object>(entity.Json, entity.Type)!);
    }

    /// <summary>
    /// Query all objects whose _type hierarchy contains the given type name.
    /// Deserializes using the concrete type from _type so derived properties are preserved.
    /// </summary>
    public IEnumerable<T> Query<T>(string tableName,
        Expression<Func<T, bool>>? predicate = null,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)

        where T : class, new()
    {
        var table = GetTable(tableName);

        var query = GetODataQuery<T>(predicate);

        return table.Query<LottaTableEntity>(query, maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializePolymorphic<T>(entity.Json, entity.Type)!);
    }

    private static string GetODataQuery<T>(Expression<Func<T, bool>>? predicate = null) where T : class, new()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"PartitionKey eq '{PK}'");

        var derivedTypes = TypeUtils.GetDerivedTypes(typeof(T));
        if (derivedTypes.Any())
        {
            sb.Append($" AND ({String.Join(" OR ", derivedTypes.Select(t => $"Type eq '{t.FullName}'"))})");
        }
        if (predicate != null)
        {
            sb.Append($" AND ({TableClient.CreateQueryFilter(predicate)})");
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

    public async Task DeleteTableAsync(string tableName)
    {
        var table = GetTable(tableName);
        await table.DeleteAsync();
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
