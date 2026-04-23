namespace Lotta;

/// <summary>
/// Specifies which property is the default search target for free-text queries
/// (<c>Search&lt;T&gt;("text")</c>) and object-level <c>.Query()</c> / <c>.Similar()</c>.
/// When set, LottaDB skips generating the automatic <c>_content_</c> composite field.
/// <para>
/// The referenced property must be indexed via <see cref="QueryableAttribute"/>,
/// <see cref="Lucene.Net.Linq.Mapping.FieldAttribute"/>, or the fluent API.
/// </para>
/// </summary>
/// <example>
/// <code>
/// [DefaultSearch(nameof(Content))]
/// public class Article
/// {
///     [Key] public string Id { get; set; }
///     [Queryable] public string Title { get; set; }
///     [Queryable] public string Body { get; set; }
///     [Queryable(Vector = true)]
///     public string Content { get =&gt; $"{Title} {Body}"; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class DefaultSearchAttribute : Attribute
{
    /// <summary>The name of the property to use as the default search field.</summary>
    public string PropertyName { get; }

    /// <param name="propertyName">
    /// Name of the indexed property. Use <c>nameof(Property)</c> for compile-time safety.
    /// </param>
    public DefaultSearchAttribute(string propertyName)
    {
        PropertyName = propertyName;
    }
}
