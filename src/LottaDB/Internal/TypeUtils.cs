using System.Text.Json;

namespace Lotta.Internal
{
    internal static class TypeUtils
    {
        /// <summary>
        /// True for string collections indexed as multi-valued fields: string[], List&lt;string&gt;,
        /// IEnumerable&lt;string&gt; and anything else assignable to IEnumerable&lt;string&gt; (except string).
        /// </summary>
        internal static bool IsStringCollection(Type type)
            => type != typeof(string) && typeof(IEnumerable<string>).IsAssignableFrom(type);

        /// <summary>
        /// Walks the BaseType chain from the given type up to (but not including) object.
        /// </summary>
        internal static IEnumerable<Type> GetTypeHierarchy(Type type)
        {
            for (var current = type; current != null && current != typeof(object); current = current.BaseType)
            {
                yield return current;
            }
        }

        static readonly Dictionary<string, Type> _fullNameToType = new();
        static readonly Dictionary<Type, List<Type>> _derivedTypes = new();

        /// <summary>How many assemblies were loaded when the types were last read; a name not found is looked for again only once more have loaded.</summary>
        static int _assembliesRead;
        static readonly HashSet<string> _notFound = new(StringComparer.Ordinal);

        /// <summary>
        /// Resolve Type.FullName => Type; null when no loaded assembly defines it. A stored row
        /// can name a type that no longer exists (a class removed since the row was saved):
        /// reading it gives nothing rather than failing, so rebuilding the index and listing the
        /// queues pass it by.
        /// </summary>
        internal static Type? ResolveType(string? fullName)
        {
            if (fullName == null)
                return null;
            lock (_fullNameToType)
            {
                if (_fullNameToType.TryGetValue(fullName, out var t))
                    return t;

                var assemblies = AppDomain.CurrentDomain.GetAssemblies();
                if (_notFound.Contains(fullName) && assemblies.Length == _assembliesRead)
                    return null;
                foreach (var type in assemblies.SelectMany(a => a.DefinedTypes))
                {
                    _fullNameToType[type.FullName!] = type;
                }
                _assembliesRead = assemblies.Length;
                _notFound.Clear();

                if (_fullNameToType.TryGetValue(fullName, out t))
                    return t;
                _notFound.Add(fullName);
                return null;
            }
        }

        internal static object? DeserializeFromTypeName(string json, string typeName)
        {
            var type = ResolveType(typeName);
            if (type == null) return null;
            try
            {
                return System.Text.Json.JsonSerializer.Deserialize(json, type);
            }
            catch
            {
                return null;
            }
        }

        internal static List<Type> GetDerivedTypes(Type type)
        {
            lock (_derivedTypes)
            {
                if (_derivedTypes.TryGetValue(type, out var derivedTypes))
                    return derivedTypes;
                derivedTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(a => a.DefinedTypes)
                    .Where(t => t != typeof(object) && t.IsClass && !t.IsAbstract && type.IsAssignableFrom(t))
                    .Select(t => t.AsType())
                    .ToList();
                _derivedTypes[type] = derivedTypes;
                return derivedTypes;
            }
        }
    }
}
