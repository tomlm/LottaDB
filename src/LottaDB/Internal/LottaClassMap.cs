using System.Linq.Expressions;
using System.Reflection;
using Lucene.Net.Linq.Fluent;
using Lucene.Net.Linq.Mapping;
using Lucene.Net.Util;

namespace Lotta.Internal;

/// <summary>
/// Builds a ClassMap for type T by reading [Field]/[NumericField] attributes,
/// then adds _type DocumentKey entries for the type hierarchy.
/// The resulting IDocumentMapper handles _type discrimination automatically.
/// </summary>
internal static class LottaClassMap
{
    public static IDocumentMapper<T> Build<T>(string[] typeHierarchy) where T : class, new()
    {
        var map = new ClassMap<T>(LuceneVersion.LUCENE_48);

        foreach (var prop in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!prop.CanRead || !prop.CanWrite) continue;

            var fieldAttr = prop.GetCustomAttribute<FieldAttribute>();
            var numericAttr = prop.GetCustomAttribute<NumericFieldAttribute>();
            var ignoreAttr = prop.GetCustomAttribute<IgnoreFieldAttribute>();

            if (ignoreAttr != null) continue;

            // Skip complex types that Lucene can't index
            if (!IsIndexableType(prop.PropertyType)) continue;

            var paramExpr = Expression.Parameter(typeof(T), "x");
            var propExpr = Expression.Property(paramExpr, prop);
            var convertExpr = Expression.Convert(propExpr, typeof(object));
            var lambda = Expression.Lambda<Func<T, object>>(convertExpr, paramExpr);

            if (numericAttr != null)
            {
                map.Property(lambda).AsNumericField();
            }
            else if (fieldAttr != null)
            {
                if (fieldAttr.Key)
                {
                    // Key fields are mapped via Key() which handles indexing
                    map.Key(lambda);
                }
                else
                {
                    var propMap = map.Property(lambda);
                    switch (fieldAttr.IndexMode)
                    {
                        case IndexMode.NotAnalyzed:
                            propMap.NotAnalyzed();
                            break;
                        case IndexMode.Analyzed:
                            propMap.Analyzed();
                            break;
                        case IndexMode.NotIndexed:
                            propMap.NotIndexed();
                            break;
                    }
                }
            }
            else
            {
                // Convention: map all indexable properties by default (analyzed, stored)
                map.Property(lambda);
            }
        }

        return map.ToDocumentMapper();
    }

    private static bool IsIndexableType(Type type)
    {
        // Unwrap nullable
        var t = Nullable.GetUnderlyingType(type) ?? type;

        if (t == typeof(string)) return true;
        if (t == typeof(int) || t == typeof(long) || t == typeof(float) || t == typeof(double)) return true;
        if (t == typeof(bool)) return true;
        if (t == typeof(DateTime) || t == typeof(DateTimeOffset)) return true;
        if (t == typeof(Guid)) return true;

        // IList<string> / IEnumerable<string> for multi-valued fields
        if (typeof(IEnumerable<string>).IsAssignableFrom(t)) return true;

        return false;
    }
}
