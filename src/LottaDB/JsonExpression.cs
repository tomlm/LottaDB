namespace Lotta;

/// <summary>
/// Expression-only type for building queries against JSON document properties.
/// Used in <c>Search</c> and <c>GetManyAsync</c> expressions — never instantiated.
/// <para>
/// <c>db.Search(j =&gt; j["name"] == "alice" &amp;&amp; j["age"] &gt; 20);</c>
/// </para>
/// </summary>
public class JsonExpression
{
    /// <summary>
    /// Access a JSON property by name for use in query expressions.
    /// Translates to a Lucene field query or OData column filter.
    /// </summary>
    public JsonField this[string field]
        => throw new NotSupportedException("JsonExpression is for query expressions only — do not invoke directly.");

    /// <summary>Get the schema name for filtering. Translates to the Schema column/field.</summary>
    public string? GetSchema()
        => throw new NotSupportedException("JsonExpression is for query expressions only.");

    /// <summary>Get the document key for filtering. Translates to the key column/field.</summary>
    public string? GetKey()
        => throw new NotSupportedException("JsonExpression is for query expressions only.");

    /// <summary>Get the ETag for filtering.</summary>
    public string? GetETag()
        => throw new NotSupportedException("JsonExpression is for query expressions only.");
}

/// <summary>
/// Represents a JSON document field in a query expression.
/// Supports comparison operators that the expression visitor translates to Lucene/OData queries.
/// Never instantiated — exists purely for expression tree construction.
/// </summary>
public class JsonField
{
    public static bool operator ==(JsonField left, string right) => throw new NotSupportedException();
    public static bool operator !=(JsonField left, string right) => throw new NotSupportedException();
    public static bool operator ==(JsonField left, int right) => throw new NotSupportedException();
    public static bool operator !=(JsonField left, int right) => throw new NotSupportedException();
    public static bool operator >(JsonField left, int right) => throw new NotSupportedException();
    public static bool operator <(JsonField left, int right) => throw new NotSupportedException();
    public static bool operator >=(JsonField left, int right) => throw new NotSupportedException();
    public static bool operator <=(JsonField left, int right) => throw new NotSupportedException();
    public static bool operator ==(JsonField left, long right) => throw new NotSupportedException();
    public static bool operator !=(JsonField left, long right) => throw new NotSupportedException();
    public static bool operator >(JsonField left, long right) => throw new NotSupportedException();
    public static bool operator <(JsonField left, long right) => throw new NotSupportedException();
    public static bool operator >=(JsonField left, long right) => throw new NotSupportedException();
    public static bool operator <=(JsonField left, long right) => throw new NotSupportedException();
    public static bool operator ==(JsonField left, double right) => throw new NotSupportedException();
    public static bool operator !=(JsonField left, double right) => throw new NotSupportedException();
    public static bool operator >(JsonField left, double right) => throw new NotSupportedException();
    public static bool operator <(JsonField left, double right) => throw new NotSupportedException();
    public static bool operator >=(JsonField left, double right) => throw new NotSupportedException();
    public static bool operator <=(JsonField left, double right) => throw new NotSupportedException();
    public static bool operator ==(JsonField left, bool right) => throw new NotSupportedException();
    public static bool operator !=(JsonField left, bool right) => throw new NotSupportedException();

    public override bool Equals(object? obj) => throw new NotSupportedException();
    public override int GetHashCode() => throw new NotSupportedException();
}
