using Azure;
using Azure.Data.Tables;
using Lucene.Net.Diagnostics;
using Lucene.Net.Linq.Mapping;
using System.Linq.Expressions;
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

    public const string PK = "OBJ";

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

    public async Task<bool> UpsertWithETagAsync(string tableName, string key, object obj, TypeMetadata meta, string expectedETag)
    {
        var table = GetTable(tableName);
        var entity = new LottaTableEntity(key, obj);
        foreach (var tag in meta.Tags)
        {
            var value = tag.GetValue(obj);
            if (value != null)
                entity[tag.Name] = ConvertToTableValue(value);
        }

        try
        {
            await table.UpdateEntityAsync(entity, new ETag(expectedETag), TableUpdateMode.Replace);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 412 || ex.Status == 409)
        {
            return false; // ETag mismatch
        }
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

    public IQueryable<object> Query(string tableName)
    {
        var table = GetTable(tableName);
        return table.Query<LottaTableEntity>()
            .Where(entity => entity.PartitionKey == PK)
            .Select(entity => DeserializePolymorphic<object>(entity.Json, entity.Type))
            .AsQueryable()!;
    }

    /// <summary>
    /// Query all objects whose _type hierarchy contains the given type name.
    /// Deserializes using the concrete type from _type so derived properties are preserved.
    /// </summary>
    public IQueryable<T> Query<T>(string tableName) where T : class, new()
    {
        var table = GetTable(tableName);
        return table.Query<LottaTableEntity>()
            .Where(e => e.PartitionKey == PK && e.Types.Contains(typeof(T).FullName!))
            .Select(entity => DeserializePolymorphic<T>(entity.Json, entity.Type))
            .AsQueryable()!;
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
