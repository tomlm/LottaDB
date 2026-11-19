using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Lotta;

/// <summary>
/// Metadata attached to entities via <see cref="ConditionalWeakTable{TKey,TValue}"/>.
/// Tracks storage-level concerns (key, ETag, JSON) separately from domain data.
/// </summary>
public class ObjectMetadata
{
    /// <summary>The entity's storage key.</summary>
    public string? Key { get; set; }

    /// <summary>The Azure Table Storage ETag for optimistic concurrency.</summary>
    public string? ETag { get; set; }

    /// <summary>
    /// The JSON representation from the last read or save. This is a cached snapshot —
    /// it is NOT updated when the object is mutated in memory.
    /// </summary>
    public string? Json { get; set; }

    /// <summary>The matched or assigned schema name for JSON documents.</summary>
    public string? Schema { get; set; }

    /// <summary>
    /// During a save of a type that has <see cref="Rule.Changed"/> rules: the JSON of the version
    /// the save replaces. Null for a new object, and outside a save.
    /// </summary>
    internal string? PreviousJson { get; set; }

    /// <summary><see cref="PreviousJson"/> as an object, made the first time a rule asks.</summary>
    internal object? Previous { get; set; }

    /// <summary>
    /// On an object a <c>Materialize</c> rule produced: the rule and the key of the object it was
    /// produced from (<c>Note/note-view n42</c>). Stored beside the object, not in it.
    /// </summary>
    internal string? Source { get; set; }

    /// <summary>
    /// On an object that <c>Materialize</c> rules produce other objects from: the keys of those
    /// objects, per rule, as its stored row lists them. Null when not known (the object was not
    /// read from its row); <see cref="NothingProduced"/> when the row lists none. Never changed
    /// in place.
    /// </summary>
    internal IReadOnlyDictionary<string, string[]>? Produced { get; set; }

    /// <summary>During a save: what the object's <c>Materialize</c> rules are to write once its row is written, per rule.</summary>
    internal Dictionary<string, LottaDB.PendingViews>? PendingMaterialize { get; set; }

    /// <summary>The row lists nothing produced.</summary>
    internal static readonly IReadOnlyDictionary<string, string[]> NothingProduced = new Dictionary<string, string[]>();
}

/// <summary>
/// Extension methods for annotating objects with storage metadata (key, ETag, JSON).
/// Uses a single <see cref="ConditionalWeakTable{TKey,TValue}"/> keyed by object identity
/// so metadata set via <c>object</c> is readable via <c>T</c> and vice versa.
/// </summary>
public static class ObjectExtensions
{
    private static readonly ConditionalWeakTable<object, ObjectMetadata> _table = new();

    // Cached [Key] property per type for GetKey fallback
    private static readonly ConcurrentDictionary<Type, PropertyInfo?> _keyProperties = new();

    private static PropertyInfo? GetKeyProperty(Type type)
    {
        return _keyProperties.GetOrAdd(type, t =>
            t.GetProperties().FirstOrDefault(p => p.GetCustomAttribute<KeyAttribute>() != null));
    }

    /// <summary>Attach a storage key to an object.</summary>
    public static void SetKey<T>(this T obj, string key) where T : class
    {
        _table.GetOrCreateValue(obj).Key = key;
    }

    /// <summary>
    /// Get the storage key for this object. Checks (in order):
    /// 1. Key from ObjectMetadata (set by LottaDB on read/save operations)
    /// 2. Value of the property marked with <see cref="KeyAttribute"/> (via cached reflection)
    /// Returns null if neither is available.
    /// </summary>
    public static string? GetKey<T>(this T obj) where T : class
    {
        if (_table.TryGetValue(obj, out var meta) && meta.Key != null)
            return meta.Key;

        var keyProp = GetKeyProperty(obj.GetType());
        if (keyProp != null)
            return keyProp.GetValue(obj)?.ToString();

        return null;
    }

    /// <summary>Attach the JSON representation from a read or save.</summary>
    public static void SetJson<T>(this T obj, string json) where T : class
    {
        _table.GetOrCreateValue(obj).Json = json;
    }

    /// <summary>
    /// Get the cached JSON from the last read or save, or null if none.
    /// This is a snapshot — it does not reflect in-memory mutations to the object.
    /// </summary>
    public static string? GetJson<T>(this T obj) where T : class
    {
        return _table.TryGetValue(obj, out var meta) ? meta.Json : null;
    }

    /// <summary>Attach an ETag to an object.</summary>
    public static void SetETag<T>(this T obj, string etag) where T : class
    {
        _table.GetOrCreateValue(obj).ETag = etag;
    }

    /// <summary>
    /// Get the ETag previously attached to this object, or null if none.
    /// </summary>
    public static string? GetETag<T>(this T obj) where T : class
    {
        return _table.TryGetValue(obj, out var meta) ? meta.ETag : null;
    }

    /// <summary>Attach a schema name to a JSON document.</summary>
    public static T SetSchema<T>(this T obj, string schema) where T : class
    {
        _table.GetOrCreateValue(obj).Schema = schema;
        return obj;
    }

    /// <summary>Get the schema name previously attached to this object, or null if none.</summary>
    public static string? GetSchema<T>(this T obj) where T : class
    {
        return _table.TryGetValue(obj, out var meta) ? meta.Schema : null;
    }

    /// <summary>Marks an object as produced by a <c>Materialize</c> rule from one source object; null for none.</summary>
    internal static void SetSource(this object obj, string? source)
    {
        if (source == null && !_table.TryGetValue(obj, out _))
            return;
        _table.GetOrCreateValue(obj).Source = source;
    }

    /// <summary>The rule and source object that produced this object (<c>Note/note-view n42</c>); null when no rule did.</summary>
    internal static string? GetSource(this object obj) =>
        _table.TryGetValue(obj, out var meta) ? meta.Source : null;

    /// <summary>Notes the keys of the objects made from this one, per rule, as its row lists them (or is about to).</summary>
    internal static void SetProduced(this object obj, IReadOnlyDictionary<string, string[]>? produced)
    {
        if (produced == null && !_table.TryGetValue(obj, out _))
            return;
        _table.GetOrCreateValue(obj).Produced = produced;
    }

    /// <summary>The keys of the objects made from this one, per rule; null when not known.</summary>
    internal static IReadOnlyDictionary<string, string[]>? GetProduced(this object obj) =>
        _table.TryGetValue(obj, out var meta) ? meta.Produced : null;

    internal static void SetPendingMaterialize(this object obj, Dictionary<string, LottaDB.PendingViews>? pending)
    {
        if (pending == null && !_table.TryGetValue(obj, out _))
            return;
        _table.GetOrCreateValue(obj).PendingMaterialize = pending;
    }

    internal static Dictionary<string, LottaDB.PendingViews>? GetPendingMaterialize(this object obj) =>
        _table.TryGetValue(obj, out var meta) ? meta.PendingMaterialize : null;

    /// <summary>
    /// Notes, for the length of a save, the JSON of the version the save replaces (null for a new
    /// object), so that <see cref="Rule.Changed"/> can compare the two.
    /// </summary>
    internal static void SetPrevious(this object obj, string? json)
    {
        // Nothing to forget on an object nothing was noted for.
        if (json == null && !_table.TryGetValue(obj, out _))
            return;
        var meta = _table.GetOrCreateValue(obj);
        meta.PreviousJson = json;
        meta.Previous = null;
    }

    /// <summary>The version the save in progress replaces, as an object of the same type; null when there is none.</summary>
    internal static object? GetPrevious(this object obj)
    {
        if (!_table.TryGetValue(obj, out var meta) || meta.PreviousJson == null)
            return null;
        if (meta.Previous == null)
        {
            try
            {
                meta.Previous = System.Text.Json.JsonSerializer.Deserialize(meta.PreviousJson, obj.GetType());
            }
            catch (System.Text.Json.JsonException)
            {
                return null;   // stored as something this type cannot read: nothing to compare with
            }
        }
        return meta.Previous;
    }
}
