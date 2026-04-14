namespace Lotta;

/// <summary>
/// Marks a property as the unique key for this object type.
/// The key value is used as the RowKey in Azure Table Storage.
/// Use <see cref="Mode"/> to control key generation for time-ordered objects.
/// For composite keys, use <c>SetKey(Func&lt;T, string&gt;)</c> fluently instead.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class KeyAttribute : Attribute
{
    /// <summary>The strategy for generating the key. Defaults to <see cref="KeyMode.Manual"/>.</summary>
    public KeyMode Mode { get; set; } = KeyMode.Manual;
}
