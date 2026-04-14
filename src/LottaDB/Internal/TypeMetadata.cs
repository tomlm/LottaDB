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

        ResolveTags<T>(meta, fluentConfig);

        return meta;
    }

    private static (Func<object, string> getter, KeyMode keymode, PropertyInfo? prop) ResolveKey<T>(StorageConfiguration<T>? fluent) where T : class, new()
    {
        // Fluent override with expression
        if (fluent?.KeyExpression is Expression<Func<T, string>> keyExpr)
        {
            var compiled = keyExpr.Compile();
            return (obj => compiled((T)obj), KeyMode.Manual, null);
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

    internal static string GenerateKey(object obj, KeyMode strategy, PropertyInfo? prop)
    {
        switch (strategy)
        {
            case KeyMode.Manual:
                if (prop == null) throw new InvalidOperationException("Natural key strategy requires a property.");
                return prop.GetValue(obj)?.ToString() ?? "";

            case KeyMode.DescendingTime:
                var descTs = GetTimestamp(obj, prop);
                var descTicks = (DateTimeOffset.MaxValue.Ticks - descTs.Ticks).ToString("D19");
                return $"{descTicks}_{Guid.NewGuid():N}";

            case KeyMode.AscendingTime:
                var ascTs = GetTimestamp(obj, prop);
                var ascTicks = ascTs.Ticks.ToString("D19");
                return $"{ascTicks}_{Guid.NewGuid():N}";

            case KeyMode.Auto:
                // If property already has a value, use it (upsert). Otherwise generate.
                if (prop != null)
                {
                    var existing = prop.GetValue(obj)?.ToString();
                    if (!string.IsNullOrEmpty(existing))
                        return existing;
                }
                return Ulid.NewUlid().ToString();

            default:
                throw new ArgumentOutOfRangeException(nameof(strategy));
        }
    }

    private static DateTimeOffset GetTimestamp(object obj, PropertyInfo? prop)
    {
        if (prop == null) return DateTimeOffset.UtcNow;
        var value = prop.GetValue(obj);
        return value switch
        {
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(dt),
            _ => DateTimeOffset.UtcNow
        };
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
