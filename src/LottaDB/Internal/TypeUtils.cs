using System.Text.Json;

namespace Lotta.Internal
{
    internal static class TypeUtils
    {
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

        /// <summary>
        /// Resolve Type.FullName => Type
        /// </summary>
        internal static Type? ResolveType(string? fullName)
        {
            if (fullName == null)
                return null;
            lock (_fullNameToType)
            {
                if (_fullNameToType.TryGetValue(fullName, out var t))
                    return t;

                foreach (var type in AppDomain.CurrentDomain.GetAssemblies().SelectMany(a => a.DefinedTypes))
                {
                    _fullNameToType[type.FullName!] = type;
                }

                return _fullNameToType[fullName];
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
