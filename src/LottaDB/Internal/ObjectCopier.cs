using Lucene.Net.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Lotta.Internal;

internal static class ObjectCopier<T> where T : class
{
    /// <summary>
    /// ShallowCopy(source, target) copies all public properties from source to target.
    /// </summary>
    public static readonly Action<T, T> ShallowCopy = CreateCopyAction();

    private static Action<T, T> CreateCopyAction()
    {
        var source = Expression.Parameter(typeof(T), "source");
        var target = Expression.Parameter(typeof(T), "target");

        var assignments = typeof(T)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p =>
                p.CanRead &&
                p.CanWrite &&
                p.GetIndexParameters().Length == 0)
            .Select(p =>
                Expression.Assign(
                    Expression.Property(target, p),
                    Expression.Property(source, p)));

        var body = Expression.Block(assignments);
        return Expression.Lambda<Action<T, T>>(body, source, target).Compile();
    }
}
