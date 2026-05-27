namespace Lotta;

/// <summary>
/// Excludes a property from AutoQueryable promotion. When a type has AutoQueryable enabled,
/// all simple-type properties are automatically promoted to queryable columns. Apply this
/// attribute to opt out specific properties (e.g., large strings that would exceed the
/// Azure Table Storage 64KB property limit).
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class NotQueryableAttribute : Attribute
{
}
