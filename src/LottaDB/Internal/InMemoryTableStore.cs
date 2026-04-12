using System.Collections.Concurrent;
using System.Text.Json;

namespace LottaDB.Internal;

/// <summary>
/// In-memory table storage. Stores objects as JSON, keyed by (PartitionKey, RowKey).
/// Supports ETags for optimistic concurrency.
/// </summary>
internal class InMemoryTableStore
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, TableEntry>> _tables = new();

    public void Upsert(string tableName, string pk, string rk, object obj, out string etag)
    {
        var table = GetOrCreateTable(tableName);
        var key = MakeKey(pk, rk);
        var json = JsonSerializer.Serialize(obj, obj.GetType());
        etag = Guid.NewGuid().ToString();
        table[key] = new TableEntry(pk, rk, json, obj.GetType(), etag);
    }

    public bool TryUpsertWithETag(string tableName, string pk, string rk, object obj, string expectedETag, out string newETag)
    {
        var table = GetOrCreateTable(tableName);
        var key = MakeKey(pk, rk);

        if (table.TryGetValue(key, out var existing))
        {
            if (existing.ETag != expectedETag)
            {
                newETag = "";
                return false; // ETag mismatch
            }
        }

        var json = JsonSerializer.Serialize(obj, obj.GetType());
        newETag = Guid.NewGuid().ToString();
        table[key] = new TableEntry(pk, rk, json, obj.GetType(), newETag);
        return true;
    }

    public T? Get<T>(string tableName, string pk, string rk, out string? etag) where T : class
    {
        var table = GetOrCreateTable(tableName);
        var key = MakeKey(pk, rk);

        if (table.TryGetValue(key, out var entry))
        {
            etag = entry.ETag;
            return JsonSerializer.Deserialize<T>(entry.Json);
        }

        etag = null;
        return null;
    }

    public bool Delete(string tableName, string pk, string rk, out TableEntry? deleted)
    {
        var table = GetOrCreateTable(tableName);
        var key = MakeKey(pk, rk);
        return table.TryRemove(key, out deleted);
    }

    public IEnumerable<T> QueryAll<T>(string tableName) where T : class
    {
        var table = GetOrCreateTable(tableName);
        // Return in RowKey order (lexicographic — descending time keys sort newest first)
        return table.Values
            .OrderBy(e => e.RowKey)
            .Select(e => JsonSerializer.Deserialize<T>(e.Json)!)
            .ToList();
    }

    public string? GetETag(string tableName, string pk, string rk)
    {
        var table = GetOrCreateTable(tableName);
        var key = MakeKey(pk, rk);
        return table.TryGetValue(key, out var entry) ? entry.ETag : null;
    }

    public void Clear(string tableName)
    {
        if (_tables.TryGetValue(tableName, out var table))
            table.Clear();
    }

    private ConcurrentDictionary<string, TableEntry> GetOrCreateTable(string tableName)
    {
        return _tables.GetOrAdd(tableName, _ => new());
    }

    private static string MakeKey(string pk, string rk) => $"{pk}||{rk}";
}

internal record TableEntry(string PartitionKey, string RowKey, string Json, Type ObjectType, string ETag);
