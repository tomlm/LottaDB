namespace LottaDB;

/// <summary>
/// Promotes a property to a native Azure Table Storage column (tag).
/// Tagged properties are server-side filterable via <see cref="LottaDB.Query{T}"/>.
/// The property must be a table-storage-compatible primitive type.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class TagAttribute : Attribute
{
    /// <summary>Optional column name override. Defaults to the property name.</summary>
    public string? Name { get; set; }
}
