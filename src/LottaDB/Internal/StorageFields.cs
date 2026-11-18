using System.Text.Json;

namespace Lotta.Internal;

/// <summary>
/// Central definition of all field/column names used in Azure Table Storage and Lucene.
/// Each field has both a Table Storage column name and a Lucene field name.
/// Some are shared (same name in both), others differ.
/// </summary>
internal static class StorageFields
{
    // ── Shared names (same in Table Storage and Lucene) ──

    /// <summary>The key for the document/entity</summary>
    public const string Key = "RowKey";

    /// <summary>The ETag for the entitity</summary>
    public const string ETag = "ETag";

    /// <summary>The CLR type full name in Lucene (same value as Table Storage Type column).</summary>
    public const string Type = "_type";

    /// <summary>Prefix for serialized JSON object columns (split across Object, Object2, Object3... if >64KB).</summary>
    public const string ObjectPrefix = "_object";

    /// <summary>The schema name. POCO = type name, POJO = JsonSchema name.</summary>
    public const string Schema = "Schema";

    /// <summary>
    /// On an object a <c>Maintain</c> rule produced: the rule and the key of the object it was
    /// produced from, <c>Note/note-view n42</c>. Absent on every other object.
    /// </summary>
    public const string Source = "_source";

    /// <summary>
    /// On an object that <c>Maintain</c> rules produce other objects from: the keys of those
    /// objects, per rule, as JSON (<c>{"Note/tags":["n1#a","n1#b"]}</c>). Split across
    /// <c>_produced</c>, <c>_produced2</c>, ... when long. Absent on every other object.
    /// </summary>
    public const string Produced = "_produced";

    // ── Lucene fields ──

    /// <summary>Composite free-text search field built from all analyzed string properties.</summary>
    public const string Content = "_content";

    // ── Internal names ──

    /// <summary>
    /// The default schema name for schemaless JsonDocument storage.
    /// Uses the CLR type name so it's self-documenting.
    /// </summary>
    public const string DefaultSchema = nameof(JsonDocument);
}
