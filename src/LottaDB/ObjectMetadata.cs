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

        var keyProp = GetKeyProperty(typeof(T));
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
}
