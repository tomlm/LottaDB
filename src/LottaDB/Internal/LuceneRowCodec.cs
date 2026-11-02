using Azure;
using Azure.Data.Tables;
using Lucene.Net.Documents;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// Converts between table rows (<see cref="TableEntity"/>) and the Lucene documents that hold
/// them when the search index is the storage (<see cref="LuceneTableServiceClient"/>).
/// The row's columns other than the object JSON are kept in a stored-only <see cref="RowField"/>;
/// the object JSON itself is the document's existing <c>_object</c> field, so it is stored once.
/// </summary>
internal static class LuceneRowCodec
{
    /// <summary>Stored-only field holding the row's columns as typed JSON.</summary>
    public const string RowField = "_row";

    /// <summary>Indexed identity for rows in the catalog-level table (partition + row key).</summary>
    public const string IdField = "_id";

    /// <summary>
    /// The row's <c>_type</c> column, indexed (not stored) on every row document so queries by
    /// type are term lookups. Separate from the mapper's <c>_type</c> field, which unmapped rows
    /// lack — they must stay out of search but still be found by a table query.
    /// </summary>
    public const string RowTypeField = "_rowtype";

    private const string PartitionKeyColumn = "PartitionKey";
    private const string RowKeyColumn = "RowKey";
    private const string TimestampColumn = "Timestamp";
    private const string ETagColumn = "odata.etag";

    public static string Id(string partitionKey, string rowKey) => $"{partitionKey}\u0001{rowKey}";

    /// <summary>
    /// A document for a row LottaDB cannot map — an unregistered type, a JSON document whose
    /// schema was deleted, or a catalog-level row. Retrievable by key, invisible to typed search.
    /// </summary>
    public static Document CreateRawDocument(TableEntity row)
    {
        var document = new Document
        {
            new StringField(StorageFields.Key, TableStorageAdapter.DecodeKey(row.RowKey), Field.Store.YES),
            new StoredField(StorageFields.ETag, row.ETag.ToString()),
        };
        var bytes = row.GetObjectBytes();
        if (bytes.Length > 0)
            document.Add(new StoredField(StorageFields.ObjectPrefix, Encoding.UTF8.GetString(bytes)));
        return document;
    }

    /// <summary>Add the row's columns to an index document.</summary>
    public static void AddRowFields(Document document, TableEntity row)
    {
        var columns = new Dictionary<string, TypedValue>
        {
            [PartitionKeyColumn] = TypedValue.FromObject(row.PartitionKey),
            [RowKeyColumn] = TypedValue.FromObject(row.RowKey),
        };
        if (row.Timestamp != null)
            columns[TimestampColumn] = TypedValue.FromObject(row.Timestamp.Value);

        foreach (var (name, value) in row)
        {
            if (name is PartitionKeyColumn or RowKeyColumn or TimestampColumn or ETagColumn) continue;
            if (name.StartsWith(StorageFields.ObjectPrefix, StringComparison.Ordinal)) continue;
            columns[name] = TypedValue.FromObject(value);
        }

        document.RemoveFields(RowField);
        document.Add(new StoredField(RowField, JsonSerializer.Serialize(columns)));

        document.RemoveFields(RowTypeField);
        if (row.GetString(StorageFields.Type) is { } type)
            document.Add(new StringField(RowTypeField, type, Field.Store.NO));
    }

    /// <summary>Rebuild the row from an index document. Returns null for documents that hold no row.</summary>
    public static TableEntity? ToRow(Document document)
    {
        var rowJson = document.Get(RowField);
        if (rowJson == null) return null;

        var columns = JsonSerializer.Deserialize<Dictionary<string, TypedValue>>(rowJson)!;
        var row = new TableEntity(
            (string)columns[PartitionKeyColumn].ToObject()!,
            (string)columns[RowKeyColumn].ToObject()!);

        foreach (var (name, value) in columns)
        {
            if (name is PartitionKeyColumn or RowKeyColumn) continue;
            if (name == TimestampColumn)
                row.Timestamp = (DateTimeOffset?)value.ToObject();
            else
                row[name] = value.ToObject();
        }

        var json = document.Get(StorageFields.ObjectPrefix);
        if (json != null)
            row.SetObjectBytes(Encoding.UTF8.GetBytes(json));

        var etag = document.Get(StorageFields.ETag);
        if (etag != null)
            row.ETag = new ETag(etag);
        return row;
    }

    private sealed class TypedValue
    {
        public string Type { get; set; } = "String";
        public string Value { get; set; } = "";

        public static TypedValue FromObject(object? value) => value switch
        {
            null => new TypedValue { Type = "Null" },
            string s => new TypedValue { Type = "String", Value = s },
            int i => new TypedValue { Type = "Int32", Value = i.ToString(CultureInfo.InvariantCulture) },
            long l => new TypedValue { Type = "Int64", Value = l.ToString(CultureInfo.InvariantCulture) },
            double d => new TypedValue { Type = "Double", Value = d.ToString("R", CultureInfo.InvariantCulture) },
            bool b => new TypedValue { Type = "Boolean", Value = b ? "true" : "false" },
            DateTimeOffset dto => new TypedValue { Type = "DateTime", Value = dto.UtcDateTime.ToString("O", CultureInfo.InvariantCulture) },
            DateTime dt => new TypedValue { Type = "DateTime", Value = dt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture) },
            Guid g => new TypedValue { Type = "Guid", Value = g.ToString() },
            byte[] bytes => new TypedValue { Type = "Binary", Value = Convert.ToBase64String(bytes) },
            BinaryData bd => new TypedValue { Type = "Binary", Value = Convert.ToBase64String(bd.ToArray()) },
            _ => new TypedValue { Type = "String", Value = value.ToString() ?? "" },
        };

        public object? ToObject() => Type switch
        {
            "Null" => null,
            "Int32" => int.Parse(Value, CultureInfo.InvariantCulture),
            "Int64" => long.Parse(Value, CultureInfo.InvariantCulture),
            "Double" => double.Parse(Value, CultureInfo.InvariantCulture),
            "Boolean" => Value == "true",
            "DateTime" => DateTimeOffset.Parse(Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            "Guid" => Guid.Parse(Value),
            "Binary" => Convert.FromBase64String(Value),
            _ => Value,
        };
    }
}
