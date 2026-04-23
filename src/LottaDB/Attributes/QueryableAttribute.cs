namespace Lotta;

/// <summary>
/// Makes a property queryable: promotes it to a native Azure Table Storage column
/// for server-side filtering AND indexes it in Lucene for full-text or exact-match search.
/// <para>
/// Smart defaults: <c>string</c> properties are analyzed (full-text searchable),
/// value types are not analyzed (exact match). Override with <see cref="QueryableMode"/>.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class QueryableAttribute : Attribute
{
    /// <summary>How the property is indexed in Lucene.</summary>
    public QueryableMode Mode { get; }

    /// <summary>
    /// When <c>true</c>, vector embeddings are generated for this property,
    /// enabling <c>.Similar()</c> queries. Composable with any <see cref="QueryableMode"/>.
    /// </summary>
    public bool Vector { get; set; }

    /// <summary>Queryable with smart defaults based on property type.</summary>
    public QueryableAttribute()
    {
        Mode = QueryableMode.Auto;
    }

    /// <summary>Queryable with explicit index mode.</summary>
    /// <param name="mode">How the property should be indexed.</param>
    public QueryableAttribute(QueryableMode mode)
    {
        Mode = mode;
    }
}
