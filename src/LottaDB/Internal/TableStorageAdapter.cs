using Azure;
using Azure.Data.Tables;
using Lotta;
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

    internal string PartitionKey => _partitionKey;

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

    /// <summary>Upsert an entity and return the new ETag.</summary>
    public async Task<string> UpsertAsync(string tableName, string key, object obj, TypeMetadata meta, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entity = EntityMapper.ToTableEntity(_partitionKey, key, obj, meta);
        var response = await table.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken: cancellationToken);
        return response.Headers.ETag.ToString()!;
    }

    /// <summary>
    /// Conditional replace using the supplied ETag. Returns the new ETag on success.
    /// Throws <see cref="RequestFailedException"/> (412) if another writer changed the row.
    /// </summary>
    public async Task<string> ReplaceAsync(string tableName, string key, object obj, TypeMetadata meta, string etag, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entity = EntityMapper.ToTableEntity(_partitionKey, key, obj, meta);
        var response = await table.UpdateEntityAsync(entity, new ETag(etag), TableUpdateMode.Replace, cancellationToken);
        return response.Headers.ETag.ToString()!;
    }

    /// <summary>
    /// Encode a key for use as Azure Table Storage RowKey.
    /// Replaces disallowed characters (/ \ # ?) with URL-encoded equivalents.
    /// </summary>
    internal static string EncodeKey(string key)
    {
        if (key.IndexOfAny(['/', '\\', '#', '?']) < 0)
            return key;
        return key.Replace("/", "%2F").Replace("\\", "%5C").Replace("#", "%23").Replace("?", "%3F");
    }

    /// <summary>
    /// Decode a RowKey back to the original key.
    /// </summary>
    internal static string DecodeKey(string rowKey)
    {
        if (rowKey.IndexOf('%') < 0)
            return rowKey;
        return rowKey.Replace("%2F", "/").Replace("%5C", "\\").Replace("%23", "#").Replace("%3F", "?");
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
            var response = await table.GetEntityAsync<TableEntity>(_partitionKey, EncodeKey(key), cancellationToken: cancellationToken);
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
            var response = table.GetEntity<TableEntity>(_partitionKey, EncodeKey(key));
            return response.Value.ETag.ToString();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<bool> DeleteAsync(string tableName, string key, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        key = EncodeKey(key);
        try
        {
            await table.DeleteEntityAsync(_partitionKey, key, cancellationToken: cancellationToken);
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
            var keyFilter = string.Join(" or ", batch.Select(k => TableClient.CreateQueryFilter<TableEntity>(e => e.RowKey == EncodeKey(k))));
            var query = $"PartitionKey eq '{_partitionKey}' and ({keyFilter})";
            await foreach (var entity in table.QueryAsync<TableEntity>(query,
                    maxPerPage: maxPerPage,
                    cancellationToken: cancellationToken))
            {
                yield return DeserializeEntity<object>(entity)!;
            }
        }
    }

    /// <summary>
    /// GetMany with ETag annotation. Each returned object has its ETag set via <see cref="ObjectExtensions.SetETag{T}"/>.
    /// </summary>
    public async IAsyncEnumerable<T> GetManyAsync<T>(string tableName,
        Expression<Func<T, bool>>? predicate = null,
        int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        var table = GetTable(tableName);
        var query = GetODataQuery<T>(predicate);
        await foreach (var entity in table.QueryAsync<TableEntity>(query, maxPerPage: maxPerPage, cancellationToken: cancellationToken))
        {
            var obj = DeserializeEntity<T>(entity);
            if (obj != null)
            {
                obj.SetETag(entity.ETag.ToString());
                yield return obj;
            }
        }
    }

    public IAsyncEnumerable<object> GetAllAsync(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.QueryAsync<TableEntity>(e => e.PartitionKey == _partitionKey, maxPerPage: maxPerPage, cancellationToken: cancellationToken)
            .Select(entity => DeserializeEntity<object>(entity)!);
    }

    /// <summary>
    /// Enumerates all raw <see cref="TableEntity"/> rows in the partition.
    /// Used by <see cref="LottaDB.RebuildSearchIndex"/> to iterate once and dispatch
    /// each entity to typed or dynamic mappers based on the Type column.
    /// </summary>
    internal IAsyncEnumerable<TableEntity> GetAllRawAsync(string tableName,
        int? maxPerPage = null,
        CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        return table.QueryAsync<TableEntity>(e => e.PartitionKey == _partitionKey, maxPerPage: maxPerPage, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Deserializes a <see cref="TableEntity"/> into a CLR object using the stored Type column
    /// to resolve the concrete type. Returns null if the type cannot be resolved.
    /// </summary>
    internal static object? DeserializeEntity(TableEntity entity)
        => EntityMapper.FromTableEntity(entity);

    private static T? DeserializeEntity<T>(TableEntity entity) where T : class
    {
        var obj = DeserializeEntity(entity);
        if (obj is T typed) return typed;
        if (obj != null) return null;
        // Fallback: try deserializing as T directly
        var bytes = entity.GetObjectBytes();
        if (bytes.Length == 0) return null;
        return JsonSerializer.Deserialize<T>(bytes);
    }

    private string GetODataQuery<T>(Expression<Func<T, bool>>? predicate = null) where T : class, new()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append($"PartitionKey eq '{_partitionKey}'");

        var derivedTypes = TypeUtils.GetDerivedTypes(typeof(T));
        if (derivedTypes.Any())
        {
            sb.Append($" and ({String.Join(" or ", derivedTypes.Select(t => $"{StorageFields.Type} eq '{t.FullName}'"))})");
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
                await SubmitTransactionAsync(tableName, batch, cancellationToken);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await SubmitTransactionAsync(tableName, batch, cancellationToken);
    }

    /// <summary>
    /// Deletes all rows in this adapter's partition without dropping the table.
    /// Alias for <see cref="ResetPartitionAsync"/> for semantic clarity.
    /// </summary>
    public Task DeletePartitionAsync(string tableName, CancellationToken cancellationToken = default)
        => ResetPartitionAsync(tableName, cancellationToken);

    internal TableTransactionAction CreateUpsertAction(string key, object obj, TypeMetadata meta)
    {
        var entity = EntityMapper.ToTableEntity(_partitionKey, key, obj, meta);
        return new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity);
    }

    internal TableTransactionAction CreateJsonDocumentUpsertAction(string key, string schemaName, JsonDocument json, JsonMetadata schema)
    {
        var entity = EntityMapper.ToTableEntity(_partitionKey, key, json, schema);
        return new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity);
    }

    internal TableTransactionAction CreateDeleteAction(string key)
    {
        return new TableTransactionAction(TableTransactionActionType.Delete,
            new TableEntity(_partitionKey, EncodeKey(key)) { ETag = ETag.All });
    }

    /// <summary>
    /// Submit a batch of table operations. Returns per-item ETags (null for deletes).
    /// </summary>
    internal async Task<string?[]> SubmitTransactionAsync(string tableName, IReadOnlyList<TableTransactionAction> actions, CancellationToken cancellationToken = default)
    {
        var etags = new string?[actions.Count];
        if (actions.Count == 0) return etags;
        var table = GetTable(tableName);
        try
        {
            var responses = await table.SubmitTransactionAsync(actions, cancellationToken);
            for (int i = 0; i < responses.Value.Count; i++)
                etags[i] = responses.Value[i].Headers.ETag?.ToString();
        }
        catch (Exception)
        {
            // Fallback for providers that don't support transactions (e.g., Spotflow in-memory)
            for (int i = 0; i < actions.Count; i++)
            {
                var action = actions[i];
                switch (action.ActionType)
                {
                    case TableTransactionActionType.UpsertReplace:
                        var response = await table.UpsertEntityAsync(action.Entity, TableUpdateMode.Replace, cancellationToken);
                        etags[i] = response.Headers.ETag?.ToString();
                        break;
                    case TableTransactionActionType.Delete:
                        try { await table.DeleteEntityAsync(action.Entity.PartitionKey, action.Entity.RowKey, cancellationToken: cancellationToken); }
                        catch (RequestFailedException ex) when (ex.Status == 404) { }
                        break;
                }
            }
        }
        return etags;
    }

    // === Dynamic (JSON Schema) operations ===

    /// <summary>
    /// Upsert a dynamic JSON document. Stores the full JSON in the Object column
    /// and extracts queryable property values as tag columns for OData filtering.
    /// </summary>
    /// <param name="tableName">The Azure Table Storage table name.</param>
    /// <param name="key">The document key (used as RowKey).</param>
    /// <param name="schemaName">The schema type name (stored in the Type column).</param>
    /// <param name="json">The full JSON document to store.</param>
    /// <param name="schema">The schema definition used to extract tag values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new ETag after upsert.</returns>
    public async Task<string> UpsertJsonDocumentAsync(string tableName, string key, string schemaName,
        JsonDocument json, JsonMetadata schema, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entity = EntityMapper.ToTableEntity(_partitionKey, key, json, schema);
        var response = await table.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken: cancellationToken);
        return response.Headers.ETag.ToString()!;
    }

    /// <summary>
    /// Promotes all top-level simple-type JSON properties to table entity columns.
    /// Used when AutoQueryable is enabled.
    /// </summary>
    internal static void PromoteAutoQueryableProperties(TableEntity entity, JsonElement root, string keyProperty, HashSet<string>? skip = null)
    {
        foreach (var prop in root.EnumerateObject())
        {
            if (prop.Name.Equals(keyProperty, StringComparison.OrdinalIgnoreCase))
                continue;
            if (skip != null && skip.Contains(prop.Name))
                continue;

            switch (prop.Value.ValueKind)
            {
                case JsonValueKind.String:
                    entity[prop.Name] = prop.Value.GetString() ?? "";
                    break;
                case JsonValueKind.Number:
                    if (prop.Value.TryGetInt64(out var l))
                        entity[prop.Name] = l;
                    else if (prop.Value.TryGetDouble(out var d))
                        entity[prop.Name] = d;
                    break;
                case JsonValueKind.True:
                case JsonValueKind.False:
                    entity[prop.Name] = prop.Value.GetBoolean();
                    break;
                case JsonValueKind.Array:
                    // String arrays → comma-delimited
                    var elements = new List<string>();
                    foreach (var element in prop.Value.EnumerateArray())
                    {
                        if (element.ValueKind == JsonValueKind.String)
                            elements.Add(element.GetString() ?? "");
                        else
                            break; // Not a pure string array, skip
                    }
                    if (elements.Count > 0)
                        entity[prop.Name] = string.Join(",", elements);
                    break;
            }
        }
    }

    /// <summary>
    /// Point-read a dynamic JSON document by key.
    /// Returns a <see cref="JsonDocument"/> with ETag annotated via <see cref="ObjectExtensions.SetETag{T}"/>.
    /// </summary>
    public async Task<JsonDocument?> GetJsonDocumentAsync(string tableName, string key, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        try
        {
            var response = await table.GetEntityAsync<TableEntity>(_partitionKey, EncodeKey(key), cancellationToken: cancellationToken);
            var bytes = response.Value.GetObjectBytes();
            if (bytes.Length == 0) return null;
            var doc = JsonDocument.Parse(bytes);
            doc.SetKey(DecodeKey(response.Value.RowKey));
            doc.SetETag(response.Value.ETag.ToString());
            if (response.Value.TryGetValue(StorageFields.Schema, out var schemaVal) && schemaVal is string schemaStr)
                doc.SetSchema(schemaStr);
            return doc;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    /// <summary>
    /// Query dynamic JSON documents by schema name with an optional OData filter.
    /// Each returned <see cref="JsonDocument"/> has its ETag annotated.
    /// </summary>
    public async IAsyncEnumerable<JsonDocument> GetManyJsonDocumentsAsync(string tableName, string schemaName,
        string? filter = null, int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        if (!string.IsNullOrEmpty(filter))
            ValidateODataFilter(filter);
        var query = $"PartitionKey eq '{_partitionKey}' and {StorageFields.Type} eq '{typeof(JsonDocument).FullName}'";
        if (schemaName != StorageFields.DefaultSchema)
            query += $" and {StorageFields.Schema} eq '{schemaName}'";
        if (!string.IsNullOrEmpty(filter))
            query += $" and ({filter})";

        await foreach (var entity in table.QueryAsync<TableEntity>(query, maxPerPage: maxPerPage, cancellationToken: cancellationToken))
        {
            var bytes = entity.GetObjectBytes();
            if (bytes.Length > 0)
            {
                var doc = JsonDocument.Parse(bytes);
                doc.SetKey(DecodeKey(entity.RowKey));
                doc.SetETag(entity.ETag.ToString());
                if (entity.TryGetValue(StorageFields.Schema, out var schemaVal) && schemaVal is string schemaStr)
                    doc.SetSchema(schemaStr);
                yield return doc;
            }
        }
    }

    /// <summary>
    /// Query JSON documents using a raw OData filter (pre-built, includes PartitionKey and Type).
    /// </summary>
    internal async IAsyncEnumerable<JsonDocument> QueryJsonDocumentsRawAsync(string tableName, string filter,
        int? maxPerPage = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        await foreach (var entity in table.QueryAsync<TableEntity>(filter, maxPerPage: maxPerPage, cancellationToken: cancellationToken))
        {
            var bytes = entity.GetObjectBytes();
            if (bytes.Length > 0)
            {
                var doc = JsonDocument.Parse(bytes);
                doc.SetKey(DecodeKey(entity.RowKey));
                doc.SetETag(entity.ETag.ToString());
                if (entity.TryGetValue(StorageFields.Schema, out var schema) && schema is string schemaStr)
                    doc.SetSchema(schemaStr);
                yield return doc;
            }
        }
    }

    /// <summary>Conditional replace for a dynamic JSON document. Returns false on ETag mismatch (412).</summary>
    /// <summary>
    /// Conditional replace for a dynamic JSON document. Returns the new ETag on success,
    /// or null on ETag mismatch (412).
    /// Throws <see cref="RequestFailedException"/> (412) if another writer changed the row.
    /// </summary>
    public async Task<string> ReplaceJsonDocumentAsync(string tableName, string key, string schemaName,
        JsonDocument json, JsonMetadata schema, string etag, CancellationToken cancellationToken = default)
    {
        var table = GetTable(tableName);
        var entity = EntityMapper.ToTableEntity(_partitionKey, key, json, schema);
        var response = await table.UpdateEntityAsync(entity, new ETag(etag), TableUpdateMode.Replace, cancellationToken);
        return response.Headers.ETag.ToString()!;
    }

    /// <summary>
    /// Validates that a user-supplied OData filter does not attempt to override
    /// the fixed PartitionKey or Type constraints, preventing cross-database reads.
    /// </summary>
    /// <summary>
    /// Validates that a user-supplied OData filter does not attempt to override
    /// the fixed PartitionKey, RowKey, or Type constraints, preventing cross-database reads.
    /// </summary>
    internal static void ValidateODataFilter(string filter)
    {
        if (filter.Contains("PartitionKey", StringComparison.OrdinalIgnoreCase) ||
            filter.Contains("RowKey", StringComparison.OrdinalIgnoreCase) ||
            System.Text.RegularExpressions.Regex.IsMatch(filter, $@"\b{StorageFields.Type}\s+(eq|ne|gt|ge|lt|le)\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
        {
            throw new ArgumentException(
                $"OData filter must not reference 'PartitionKey', 'RowKey', or '{StorageFields.Type}' columns. " +
                "These are managed internally by LottaDB for isolation.", nameof(filter));
        }
    }

    internal static object ConvertJsonElementToTableValue(JsonElement val, Type clrType)
    {
        if (clrType == typeof(string)) return val.GetString() ?? "";
        if (clrType == typeof(int)) return val.TryGetInt32(out var i) ? i : 0;
        if (clrType == typeof(double)) return val.TryGetDouble(out var d) ? d : 0.0;
        if (clrType == typeof(bool)) return val.GetBoolean();
        if (clrType == typeof(long)) return val.TryGetInt64(out var l) ? l : 0L;
        return val.ToString();
    }

    internal static object ConvertToTableValue(object value)
    {
        return value switch
        {
            string s => s,
            string[] arr => string.Join(",", arr),
            IEnumerable<string> strings => string.Join(",", strings),
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
