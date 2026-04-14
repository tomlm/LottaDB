using System.Linq.Expressions;
using System.Reflection;

namespace Lotta.Internal;

/// <summary>
/// Parsed metadata for a registered type. Key, tags, type hierarchy.
/// </summary>
internal class TypeMetadata
{
    public Type Type { get; }
    public Func<object, string> GetKey { get; set; } = null!;
    public Action<object, string>? SetKey { get; set; }
    public KeyMode KeyMode { get; set; } = KeyMode.Manual;
    public PropertyInfo? KeyProperty { get; set; }
    public List<TagInfo> Tags { get; } = new();
    public List<IndexedPropertyInfo> IndexedProperties { get; } = new();

    public TypeMetadata(Type type)
    {
        Type = type;
    }

    public static TypeMetadata Build<T>(StorageConfiguration<T>? fluentConfig) where T : class, new()
    {
        var type = typeof(T);
        var meta = new TypeMetadata(type);

        var (getKey, keymode, keyProp) = ResolveKey<T>(fluentConfig);
        meta.GetKey = getKey;
        meta.KeyMode = keymode;
        meta.KeyProperty = keyProp;

        // Provide a setter for Auto key mode
        if (keyProp != null && keyProp.CanWrite)
            meta.SetKey = (obj, key) => keyProp.SetValue(obj, key);

        ResolveTags<T>(meta, fluentConfig);
        ResolveIndexedProperties<T>(meta, fluentConfig);

        return meta;
    }

    private static (Func<object, string> getter, KeyMode keymode, PropertyInfo? prop) ResolveKey<T>(StorageConfiguration<T>? fluent) where T : class, new()
    {
        // Fluent override with expression
        if (fluent?.KeyExpression is Expression<Func<T, string>> keyExpr)
        {
            var compiled = keyExpr.Compile();
            var keyProp = ExtractPropertyInfo(keyExpr);
            return (obj => compiled((T)obj), KeyMode.Manual, keyProp);
        }

        // Fluent override with strategy
        if (fluent?.KeyModeValue != null)
        {
            var strategy = fluent.KeyModeValue.Value;
            var attrProp = typeof(T).GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<KeyAttribute>() != null);
            return (obj => GenerateKey(obj, strategy, attrProp), strategy, attrProp);
        }

        // Attribute-based
        var prop = typeof(T).GetProperties()
            .FirstOrDefault(p => p.GetCustomAttribute<KeyAttribute>() != null);

        if (prop == null)
            throw new InvalidOperationException($"Type {typeof(T).Name} has no [Key] attribute and no fluent SetKey was configured.");

        var attrStrategy = prop.GetCustomAttribute<KeyAttribute>()!.Mode;
        return (obj => GenerateKey(obj, attrStrategy, prop), attrStrategy, prop);
    }

    internal static string GenerateKey(object obj, KeyMode mode, PropertyInfo? prop)
    {
        switch (mode)
        {
            case KeyMode.Manual:
                if (prop == null) throw new InvalidOperationException("Manual key mode requires a property.");
                return prop.GetValue(obj)?.ToString() ?? "";

            case KeyMode.Auto:
                if (prop != null)
                {
                    var existing = prop.GetValue(obj)?.ToString();
                    if (!string.IsNullOrEmpty(existing))
                        return existing;
                }
                return Ulid.NewUlid().ToString();

            default:
                throw new ArgumentOutOfRangeException(nameof(mode));
        }
    }

    private static void ResolveTags<T>(TypeMetadata meta, StorageConfiguration<T>? fluent) where T : class, new()
    {
        foreach (var prop in typeof(T).GetProperties())
        {
            var tagAttr = prop.GetCustomAttribute<TagAttribute>();
            if (tagAttr != null)
            {
                meta.Tags.Add(new TagInfo
                {
                    Name = tagAttr.Name ?? prop.Name,
                    Property = prop,
                    GetValue = obj => prop.GetValue(obj)
                });
            }
        }

        if (fluent != null)
        {
            foreach (var tagExpr in fluent.Tags)
            {
                var propInfo = ExtractPropertyInfo(tagExpr);
                if (propInfo != null && !meta.Tags.Any(t => t.Property == propInfo))
                {
                    meta.Tags.Add(new TagInfo
                    {
                        Name = propInfo.Name,
                        Property = propInfo,
                        GetValue = obj => propInfo.GetValue(obj)
                    });
                }
            }
        }
    }

    private static void ResolveIndexedProperties<T>(TypeMetadata meta, StorageConfiguration<T>? fluent) where T : class, new()
    {
        if (fluent == null) return;

        foreach (var config in fluent.IndexedProperties)
        {
            var propInfo = ExtractPropertyInfo(config.Expression);
            if (propInfo != null)
            {
                meta.IndexedProperties.Add(new IndexedPropertyInfo
                {
                    Property = propInfo,
                    IsKey = config.IsKey,
                    IsNotAnalyzed = config.IsNotAnalyzed,
                    IsNumeric = config.IsNumeric,
                    HasDocValues = config.HasDocValues,
                    AnalyzerType = config.AnalyzerType,
                });
            }
        }
    }

    private static PropertyInfo? ExtractPropertyInfo(LambdaExpression expr)
    {
        var body = expr.Body;
        if (body is UnaryExpression unary)
            body = unary.Operand;
        if (body is MemberExpression member && member.Member is PropertyInfo prop)
            return prop;
        return null;
    }
}

internal class TagInfo
{
    public required string Name { get; init; }
    public required PropertyInfo Property { get; init; }
    public required Func<object, object?> GetValue { get; init; }
}

internal class IndexedPropertyInfo
{
    public required PropertyInfo Property { get; init; }
    public bool IsKey { get; init; }
    public bool IsNotAnalyzed { get; init; }
    public bool IsNumeric { get; init; }
    public bool HasDocValues { get; init; }
    public Type? AnalyzerType { get; init; }
}
