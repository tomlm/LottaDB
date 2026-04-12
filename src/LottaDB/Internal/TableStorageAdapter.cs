using System.Text.Json;
using Azure;
using Azure.Data.Tables;

namespace LottaDB.Internal;

/// <summary>
/// Adapter over Azure.Data.Tables TableClient for storing POCOs as JSON + tags.
/// Works with both real Azure Table Storage and Spotflow in-memory fakes.
/// </summary>
internal class TableStorageAdapter
{
    private readonly TableServiceClient _serviceClient;
    private readonly Dictionary<string, TableClient> _tables = new();

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

    public async Task UpsertAsync(string tableName, string pk, string rk, object obj, TypeMetadata meta)
    {
        var table = GetTable(tableName);
        var entity = new TableEntity(pk, rk);

        // Store full POCO as JSON
        entity["_json"] = JsonSerializer.Serialize(obj, obj.GetType());

        // Promote tags
        foreach (var tag in meta.Tags)
        {
            var value = tag.GetValue(obj);
            if (value != null)
                entity[tag.Name] = ConvertToTableValue(value);
        }

        await table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
    }

    public async Task<bool> UpsertWithETagAsync(string tableName, string pk, string rk, object obj, TypeMetadata meta, string expectedETag)
    {
        var table = GetTable(tableName);
        var entity = new TableEntity(pk, rk);
        entity["_json"] = JsonSerializer.Serialize(obj, obj.GetType());

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

    public async Task<(T? obj, string? etag)> GetAsync<T>(string tableName, string pk, string rk) where T : class, new()
    {
        var table = GetTable(tableName);
        try
        {
            var response = await table.GetEntityAsync<TableEntity>(pk, rk);
            var entity = response.Value;
            var json = entity.GetString("_json");
            if (json == null) return (null, null);

            var obj = JsonSerializer.Deserialize<T>(json);
            return (obj, entity.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return (null, null);
        }
    }

    public string? GetETag(string tableName, string pk, string rk)
    {
        var table = GetTable(tableName);
        try
        {
            var response = table.GetEntity<TableEntity>(pk, rk);
            return response.Value.ETag.ToString();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<bool> DeleteAsync(string tableName, string pk, string rk)
    {
        var table = GetTable(tableName);
        try
        {
            await table.DeleteEntityAsync(pk, rk);
            return true;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return false;
        }
    }

    public async IAsyncEnumerable<T> QueryAllAsync<T>(string tableName) where T : class, new()
    {
        var table = GetTable(tableName);
        await foreach (var entity in table.QueryAsync<TableEntity>())
        {
            var json = entity.GetString("_json");
            if (json != null)
            {
                var obj = JsonSerializer.Deserialize<T>(json);
                if (obj != null)
                    yield return obj;
            }
        }
    }

    public List<T> QueryAll<T>(string tableName) where T : class, new()
    {
        var table = GetTable(tableName);
        var results = new List<T>();
        foreach (var entity in table.Query<TableEntity>())
        {
            var json = entity.GetString("_json");
            if (json != null)
            {
                var obj = JsonSerializer.Deserialize<T>(json);
                if (obj != null)
                    results.Add(obj);
            }
        }
        return results;
    }

    public List<T> QueryAll<T>(string tableName, string partitionKey) where T : class, new()
    {
        var table = GetTable(tableName);
        var results = new List<T>();
        foreach (var entity in table.Query<TableEntity>(e => e.PartitionKey == partitionKey))
        {
            var json = entity.GetString("_json");
            if (json != null)
            {
                var obj = JsonSerializer.Deserialize<T>(json);
                if (obj != null)
                    results.Add(obj);
            }
        }
        return results;
    }

    public void ClearTable(string tableName)
    {
        var table = GetTable(tableName);
        foreach (var entity in table.Query<TableEntity>())
        {
            table.DeleteEntity(entity.PartitionKey, entity.RowKey);
        }
    }

    public void ClearTable(string tableName, string partitionKey)
    {
        var table = GetTable(tableName);
        foreach (var entity in table.Query<TableEntity>(e => e.PartitionKey == partitionKey))
        {
            table.DeleteEntity(entity.PartitionKey, entity.RowKey);
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
