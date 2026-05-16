using System.Text.Json.Serialization;

namespace Lotta;

/// <summary>
/// Defines a dynamic document type stored as a first-class entity.
/// Specifies which properties to extract from JSON documents for indexing in Lucene
/// and promotion to Table Storage columns for OData filtering.
/// Managed through standard CRUD operations: <c>SaveAsync</c>, <c>GetAsync</c>, <c>DeleteAsync</c>.
/// </summary>
public class JsonDocumentType
{
    /// <summary>The unique name for this document type (e.g. "Person", "Photo").</summary>
    [Key]
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    /// <summary>Optional description of this document type.</summary>
    [JsonPropertyName("description")]
    public string? Description { get; set; }

    /// <summary>
    /// The properties to extract from JSON documents for indexing and querying.
    /// Each property is indexed in Lucene and promoted to a Table Storage column.
    /// </summary>
    [JsonPropertyName("properties")]
    public List<QueryableProperty> Properties { get; set; } = new();

    /// <summary>
    /// The document key — either a property name (e.g. <c>"id"</c>) or a JSONPath expression
    /// (e.g. <c>"$.user.id"</c>). Defaults to <c>"Id"</c>.
    /// </summary>
    [JsonPropertyName("key")]
    public string Key { get; set; } = "Id";

    /// <summary>
    /// Key generation strategy. <see cref="Lotta.KeyMode.Auto"/> generates a ULID
    /// when the key is missing; <see cref="Lotta.KeyMode.Manual"/> requires the caller to supply it.
    /// </summary>
    [JsonPropertyName("keyMode")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public KeyMode KeyMode { get; set; } = KeyMode.Auto;
}

/// <summary>
/// Defines a single property to extract from JSON documents for indexing and querying.
/// </summary>
public class QueryableProperty
{
    /// <summary>
    /// The name of this property. Used as the Lucene field name, Table Storage column name,
    /// and the target for OData filters and Lucene queries.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    /// <summary>
    /// Optional JSONPath expression to extract the value from the document.
    /// Defaults to the property <see cref="Name"/> (top-level property access).
    /// Use for nested values, e.g. <c>"$.address.city"</c>.
    /// </summary>
    [JsonPropertyName("jsonPath")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? JsonPath { get; set; }

    /// <summary>
    /// The data type of this property. Determines Lucene field type and Table Storage column type.
    /// Supported values: <c>"string"</c>, <c>"integer"</c>, <c>"number"</c>, <c>"boolean"</c>.
    /// Defaults to <c>"string"</c>.
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = "string";

    /// <summary>
    /// Controls how the property is indexed.
    /// <see cref="QueryableMode.Auto"/> (default): strings are analyzed (tokenized for full-text search),
    /// other types are not analyzed (exact match).
    /// <see cref="QueryableMode.Analyzed"/>: force full-text analysis.
    /// <see cref="QueryableMode.NotAnalyzed"/>: force exact match (e.g. email addresses, IDs).
    /// </summary>
    [JsonPropertyName("mode")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public QueryableMode Mode { get; set; } = QueryableMode.Auto;

    /// <summary>Enable vector embeddings for similarity search on this property.</summary>
    [JsonPropertyName("vector")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Vector { get; set; }

    /// <summary>
    /// Mark this property as the default target for free-text queries.
    /// Only one property per document type should have this set to true.
    /// </summary>
    [JsonPropertyName("defaultSearch")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool DefaultSearch { get; set; }
}
