using System.Linq.Expressions;
using System.Reflection;

namespace LottaDB.Internal;

/// <summary>
/// Parsed storage and indexing metadata for a registered type.
/// Built from [PartitionKey]/[RowKey]/[Tag] attributes and fluent Store&lt;T&gt; overrides.
/// </summary>
internal class TypeMetadata
{
    public Type Type { get; }
    public string TableName { get; set; }
    public Func<object, string> GetPartitionKey { get; set; } = null!;
    public Func<object, string> GetRowKey { get; set; } = null!;
    public RowKeyStrategy RowKeyStrategy { get; set; } = RowKeyStrategy.Natural;
    public PropertyInfo? RowKeyProperty { get; set; }
    public List<TagInfo> Tags { get; } = new();

    public TypeMetadata(Type type)
    {
        Type = type;
        TableName = type.Name.ToLowerInvariant() + "s";
    }

    public static TypeMetadata Build<T>(StoreConfiguration<T>? fluentConfig) where T : class, new()
    {
        var type = typeof(T);
        var meta = new TypeMetadata(type);

        // Apply fluent table name override
        if (fluentConfig?.TableName != null)
            meta.TableName = fluentConfig.TableName;

        // Resolve partition key
        meta.GetPartitionKey = ResolvePartitionKey<T>(fluentConfig);

        // Resolve row key
        var (getRowKey, strategy, rowKeyProp) = ResolveRowKey<T>(fluentConfig);
        meta.GetRowKey = getRowKey;
        meta.RowKeyStrategy = strategy;
        meta.RowKeyProperty = rowKeyProp;

        // Resolve tags
        ResolveTags<T>(meta, fluentConfig);

        return meta;
    }

    private static Func<object, string> ResolvePartitionKey<T>(StoreConfiguration<T>? fluent) where T : class, new()
    {
        // Fluent override
        if (fluent?.PartitionKeyExpression is Expression<Func<T, string>> pkExpr)
        {
            var compiled = pkExpr.Compile();
            return obj => compiled((T)obj);
        }

        // Attribute-based
        var prop = typeof(T).GetProperties()
            .FirstOrDefault(p => p.GetCustomAttribute<PartitionKeyAttribute>() != null);

        if (prop == null)
            throw new InvalidOperationException($"Type {typeof(T).Name} has no [PartitionKey] attribute and no fluent SetPartitionKey was configured.");

        return obj => prop.GetValue(obj)?.ToString() ?? "";
    }

    private static (Func<object, string> getter, RowKeyStrategy strategy, PropertyInfo? prop) ResolveRowKey<T>(StoreConfiguration<T>? fluent) where T : class, new()
    {
        // Fluent override with expression
        if (fluent?.RowKeyExpression is Expression<Func<T, string>> rkExpr)
        {
            var compiled = rkExpr.Compile();
            return (obj => compiled((T)obj), RowKeyStrategy.Natural, null);
        }

        // Fluent override with strategy
        if (fluent?.RowKeyStrategyValue != null)
        {
            var strategy = fluent.RowKeyStrategyValue.Value;
            // Find the property with [RowKey] for the time source (if strategy needs it)
            var attrProp = typeof(T).GetProperties()
                .FirstOrDefault(p => p.GetCustomAttribute<RowKeyAttribute>() != null);
            return (obj => GenerateRowKey(obj, strategy, attrProp), strategy, attrProp);
        }

        // Attribute-based
        var prop = typeof(T).GetProperties()
            .FirstOrDefault(p => p.GetCustomAttribute<RowKeyAttribute>() != null);

        if (prop == null)
            throw new InvalidOperationException($"Type {typeof(T).Name} has no [RowKey] attribute and no fluent SetRowKey was configured.");

        var attrStrategy = prop.GetCustomAttribute<RowKeyAttribute>()!.Strategy;
        return (obj => GenerateRowKey(obj, attrStrategy, prop), attrStrategy, prop);
    }

    internal static string GenerateRowKey(object obj, RowKeyStrategy strategy, PropertyInfo? prop)
    {
        switch (strategy)
        {
            case RowKeyStrategy.Natural:
                if (prop == null) throw new InvalidOperationException("Natural RowKey strategy requires a property.");
                return prop.GetValue(obj)?.ToString() ?? "";

            case RowKeyStrategy.DescendingTime:
                var descTs = GetTimestamp(obj, prop);
                var descTicks = (DateTimeOffset.MaxValue.Ticks - descTs.Ticks).ToString("D19");
                return $"{descTicks}_{Guid.NewGuid():N}";

            case RowKeyStrategy.AscendingTime:
                var ascTs = GetTimestamp(obj, prop);
                var ascTicks = ascTs.Ticks.ToString("D19");
                return $"{ascTicks}_{Guid.NewGuid():N}";

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

    private static void ResolveTags<T>(TypeMetadata meta, StoreConfiguration<T>? fluent) where T : class, new()
    {
        // Attribute-based tags
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

        // Fluent tags
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
