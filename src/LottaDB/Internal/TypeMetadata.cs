using System.Linq.Expressions;
using System.Reflection;

namespace Lotta.Internal;

/// <summary>
/// Parsed metadata for a registered type. Key, queryable properties, type hierarchy.
/// </summary>
internal class TypeMetadata
{
    public Type Type { get; }
    public Func<object, string> GetKey { get; set; } = null!;
    public Action<object, string>? SetKey { get; set; }
    public KeyMode KeyMode { get; set; } = KeyMode.Manual;
    public PropertyInfo? KeyProperty { get; set; }

    /// <summary>Properties promoted to Table Storage columns for server-side filtering.</summary>
    public List<TagInfo> Tags { get; } = new();

    /// <summary>Properties indexed in Lucene for search.</summary>
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

        ResolveQueryableProperties<T>(meta, fluentConfig);

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

    /// <summary>
    /// Resolves [Queryable], [Tag], and [Field] attributes plus fluent
    /// AddQueryable/AddTag/AddField calls into Tags and IndexedProperties.
    /// </summary>
    private static void ResolveQueryableProperties<T>(TypeMetadata meta, StorageConfiguration<T>? fluent) where T : class, new()
    {
        foreach (var prop in typeof(T).GetProperties())
        {
            // [Queryable] → both Tag + Lucene index
            var queryableAttr = prop.GetCustomAttribute<QueryableAttribute>();
            if (queryableAttr != null)
            {
                AddQueryable(meta, prop, queryableAttr.Mode, queryableAttr.Vector);
                continue;
            }

            // [Tag] → Table Storage column only
            var tagAttr = prop.GetCustomAttribute<TagAttribute>();
            if (tagAttr != null)
            {
                AddTag(meta, prop, tagAttr.Name);
            }

            // [Field] from Lucene.Net.Linq → Lucene index only (skip [Key] which inherits from [Field])
            if (prop.GetCustomAttribute<KeyAttribute>() == null)
            {
                var fieldAttr = prop.GetCustomAttribute<Lucene.Net.Linq.Mapping.FieldAttribute>(true);
                if (fieldAttr != null)
                {
                    var vectorAttr = prop.GetCustomAttribute<Lucene.Net.Linq.Mapping.VectorFieldAttribute>();
                    meta.IndexedProperties.Add(new IndexedPropertyInfo
                    {
                        Property = prop,
                        IsNotAnalyzed = fieldAttr.IndexMode == Lucene.Net.Linq.Mapping.IndexMode.NotAnalyzed
                                     || fieldAttr.IndexMode == Lucene.Net.Linq.Mapping.IndexMode.NotAnalyzedNoNorms,
                        IsVectorField = vectorAttr != null,
                    });
                }
            }
        }

        if (fluent == null) return;

        // Fluent AddQueryable → both Tag + Lucene index
        foreach (var config in fluent.QueryableProperties)
        {
            var propInfo = ExtractPropertyInfo(config.Expression);
            if (propInfo != null && !meta.Tags.Any(t => t.Property == propInfo))
                AddQueryable(meta, propInfo, config.Mode, config.IsVectorField);
        }

        // Fluent AddTag → Table Storage column only
        foreach (var tagExpr in fluent.TagProperties)
        {
            var propInfo = ExtractPropertyInfo(tagExpr);
            if (propInfo != null && !meta.Tags.Any(t => t.Property == propInfo))
                AddTag(meta, propInfo, name: null);
        }

        // Fluent AddField → Lucene index only (added to IndexedProperties for ApplyFluentConfig)
        foreach (var config in fluent.FieldProperties)
        {
            var propInfo = ExtractPropertyInfo(config.Expression);
            if (propInfo != null && !meta.IndexedProperties.Any(i => i.Property == propInfo))
            {
                meta.IndexedProperties.Add(new IndexedPropertyInfo
                {
                    Property = propInfo,
                    IsNotAnalyzed = config.IsNotAnalyzed,
                    IsVectorField = config.IsVectorField,
                });
            }
        }
    }

    private static void AddTag(TypeMetadata meta, PropertyInfo prop, string? name)
    {
        meta.Tags.Add(new TagInfo
        {
            Name = name ?? prop.Name,
            Property = prop,
            GetValue = obj => prop.GetValue(obj)
        });
    }

    private static void AddQueryable(TypeMetadata meta, PropertyInfo prop, QueryableMode mode, bool vector = false)
    {
        // Table Storage tag
        meta.Tags.Add(new TagInfo
        {
            Name = prop.Name,
            Property = prop,
            GetValue = obj => prop.GetValue(obj)
        });

        // Lucene index
        var isNotAnalyzed = mode switch
        {
            QueryableMode.NotAnalyzed => true,
            QueryableMode.Analyzed => false,
            // Auto: strings are analyzed, everything else is not
            _ => prop.PropertyType != typeof(string)
        };

        meta.IndexedProperties.Add(new IndexedPropertyInfo
        {
            Property = prop,
            IsNotAnalyzed = isNotAnalyzed,
            IsVectorField = vector,
        });
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
    public bool IsNotAnalyzed { get; init; }
    public bool IsVectorField { get; init; }
}
