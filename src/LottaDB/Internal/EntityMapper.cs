using Azure.Data.Tables;
using Lucene.Net.Documents;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// Unified mapper for converting between POCO/POJO objects and their storage representations
/// (Azure Table Storage entities and Lucene documents). All metadata (Key, ETag, Schema, Type)
/// is handled in one place.
/// </summary>
internal static class EntityMapper
{
    // ── To Table Storage ──

    /// <summary>
    /// Build a TableEntity from a POCO, setting all metadata columns and promoting queryable properties.
    /// </summary>
    public static TableEntity ToTableEntity(string partitionKey, string key, object obj, TypeMetadata meta)
    {
        var entity = new TableEntity(partitionKey, TableStorageAdapter.EncodeKey(key));
        entity[StorageFields.Type] = obj.GetType().FullName!;
        entity[StorageFields.Schema] = obj.GetType().Name;

        var bytes = JsonSerializer.SerializeToUtf8Bytes(obj, obj.GetType());
        entity.SetObjectBytes(bytes);

        foreach (var tag in meta.Tags)
        {
            var value = tag.GetValue(obj);
            if (value == null) continue;
            if (value is DateTime dt && dt == DateTime.MinValue) continue;
            if (value is DateTimeOffset dto && dto == DateTimeOffset.MinValue) continue;
            entity[tag.Name] = TableStorageAdapter.ConvertToTableValue(value);
        }
        return entity;
    }

    /// <summary>
    /// Build a TableEntity from a JsonDocument, setting all metadata columns and promoting properties.
    /// </summary>
    public static TableEntity ToTableEntity(string partitionKey, string key, JsonDocument json, JsonMetadata schema)
    {
        var entity = new TableEntity(partitionKey, TableStorageAdapter.EncodeKey(key));
        entity[StorageFields.Type] = typeof(JsonDocument).FullName!;
        if (schema.TypeName != StorageFields.DefaultSchema)
            entity[StorageFields.Schema] = schema.TypeName;

        entity.SetObjectBytes(JsonSerializer.SerializeToUtf8Bytes(json.RootElement));

        // Explicit properties first
        foreach (var prop in schema.Properties)
        {
            if (JsonMetadata.GetValue(json.RootElement, prop) is JsonElement val && val.ValueKind != JsonValueKind.Null)
                entity[prop.Name] = TableStorageAdapter.ConvertJsonElementToTableValue(val, prop.ClrType);
        }

        // AutoQueryable: promote remaining simple-type properties
        if (schema.AutoQueryable)
        {
            var explicitNames = new HashSet<string>(schema.Properties.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);
            TableStorageAdapter.PromoteAutoQueryableProperties(entity, json.RootElement, schema.KeyProperty, explicitNames);
        }

        return entity;
    }

    // ── From Table Storage ──

    /// <summary>
    /// Deserialize a TableEntity back to a POCO or JsonDocument, with all metadata set.
    /// </summary>
    public static object? FromTableEntity(TableEntity entity)
    {
        var bytes = entity.GetObjectBytes();
        if (bytes.Length == 0) return null;

        var typeName = entity.GetString(StorageFields.Type);
        var key = TableStorageAdapter.DecodeKey(entity.RowKey);
        var etag = entity.ETag.ToString();
        var schema = entity.TryGetValue(StorageFields.Schema, out var schemaObj) && schemaObj is string s ? s : null;

        // JSON document
        if (JsonMetadata.IsJsonTypeName(typeName))
        {
            var doc = JsonDocument.Parse(bytes);
            doc.SetKey(key);
            doc.SetETag(etag);
            if (schema != null) doc.SetSchema(schema);
            return doc;
        }

        // CLR typed entity
        var concreteType = TypeUtils.ResolveType(typeName);
        if (concreteType != null)
        {
            var obj = JsonSerializer.Deserialize(bytes, concreteType);
            if (obj != null)
            {
                obj.SetJson(System.Text.Encoding.UTF8.GetString(bytes));
                obj.SetKey(key);
                obj.SetETag(etag);
                obj.SetSchema(schema ?? concreteType.Name);
            }
            return obj;
        }
        return null;
    }

    /// <summary>
    /// Deserialize a TableEntity to a specific type, with all metadata set.
    /// </summary>
    public static T? FromTableEntity<T>(TableEntity entity) where T : class
    {
        var obj = FromTableEntity(entity);
        if (obj is T typed) return typed;
        if (obj != null) return null;
        // Fallback: try deserializing as T directly
        var bytes = entity.GetObjectBytes();
        if (bytes.Length == 0) return null;
        var result = JsonSerializer.Deserialize<T>(bytes);
        if (result != null)
        {
            result.SetKey(TableStorageAdapter.DecodeKey(entity.RowKey));
            result.SetETag(entity.ETag.ToString());
            result.SetSchema(typeof(T).Name);
        }
        return result;
    }

    // ── From Lucene Document ──

    /// <summary>
    /// Deserialize a Lucene Document back to a POCO or JsonDocument, with all metadata set.
    /// Used by Search&lt;object&gt;() for untyped results.
    /// </summary>
    public static object FromLuceneDocument(Document source, LottaDB? db = null)
    {
        var json = source.Get(StorageFields.ObjectPrefix)
            ?? throw new InvalidOperationException(
                $"Lucene document missing '{StorageFields.ObjectPrefix}' field. Key: {source.Get(StorageFields.Key) ?? "unknown"}. Index may be corrupted — consider calling RebuildSearchIndex().");

        var typeName = source.Get(StorageFields.Type);
        var etag = source.Get(StorageFields.ETag);
        var keyValue = source.Get(StorageFields.Key);
        var schema = source.Get(StorageFields.Schema);

        // JSON document
        if (typeName == typeof(JsonDocument).FullName)
        {
            var jsonDoc = JsonDocument.Parse(json);
            if (etag != null) jsonDoc.SetETag(etag);
            if (keyValue != null) jsonDoc.SetKey(keyValue);
            if (schema != null) jsonDoc.SetSchema(schema);
            return jsonDoc;
        }

        // CLR typed entity
        var obj = TypeUtils.DeserializeFromTypeName(json, typeName!)
            ?? throw new InvalidOperationException(
                $"Failed to deserialize type '{typeName}' from Lucene document. Key: {keyValue ?? "unknown"}. Index may be corrupted — consider calling RebuildSearchIndex().");
        if (etag != null) obj.SetETag(etag);
        if (keyValue != null) obj.SetKey(keyValue);
        obj.SetSchema(schema ?? obj.GetType().Name);
        if (obj is BlobFile bf && db != null) bf.Database = db;
        return obj;
    }

    /// <summary>
    /// Deserialize a Lucene Document to a specific POCO type, with all metadata set.
    /// </summary>
    public static T FromLuceneDocument<T>(Document source, LottaDB? db = null) where T : class
    {
        var json = source.Get(StorageFields.ObjectPrefix)
            ?? throw new InvalidOperationException(
                $"Lucene document missing '{StorageFields.ObjectPrefix}' field for type {typeof(T).Name}. Key: {source.Get(StorageFields.Key) ?? "unknown"}. Index may be corrupted — consider calling RebuildSearchIndex().");

        // Deserialize to the actual stored type for polymorphism support
        var typeName = source.Get(StorageFields.Type);
        var actualType = typeName != null ? TypeUtils.ResolveType(typeName) : null;
        var targetType = actualType != null && typeof(T).IsAssignableFrom(actualType) ? actualType : typeof(T);

        var obj = (T)JsonSerializer.Deserialize(json, targetType)!;
        obj.SetJson(json);
        var etag = source.Get(StorageFields.ETag);
        if (etag != null) obj.SetETag(etag);
        var keyValue = source.Get(StorageFields.Key);
        if (keyValue != null) obj.SetKey(keyValue);
        var schema = source.Get(StorageFields.Schema);
        obj.SetSchema(schema ?? obj.GetType().Name);
        if (obj is BlobFile bf && db != null) bf.Database = db;
        return obj;
    }

    // ── Metadata helpers for write paths ──

    /// <summary>
    /// Set all metadata on a POCO after a write operation.
    /// </summary>
    public static void AnnotateAfterWrite(object entity, string key, string etag)
    {
        entity.SetKey(key);
        entity.SetETag(etag);
        entity.SetSchema(entity.GetType().Name);
    }

    /// <summary>
    /// Set all metadata on a JsonDocument after a write operation.
    /// </summary>
    public static void AnnotateAfterWrite(JsonDocument doc, string key, string etag, string schemaName)
    {
        doc.SetKey(key);
        doc.SetETag(etag);
        if (schemaName != StorageFields.DefaultSchema)
            doc.SetSchema(schemaName);
    }

    /// <summary>
    /// Add standard metadata fields to a Lucene Document (Type, ETag, Schema).
    /// </summary>
    public static void AddMetadataToLuceneDocument(Document document, string typeName, string etag, string schema)
    {
        // Only add if not already set (JsonDocumentMapper sets these in ToDocument)
        if (document.Get(StorageFields.Type) == null)
            document.Add(new StringField(StorageFields.Type, typeName, Field.Store.YES));
        document.Add(new StoredField(StorageFields.ETag, etag));
        if (document.Get(StorageFields.Schema) == null)
            document.Add(new StringField(StorageFields.Schema, schema, Field.Store.YES));
    }
}
