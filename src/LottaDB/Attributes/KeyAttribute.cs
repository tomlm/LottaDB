using Lucene.Net.Linq.Mapping;

namespace Lotta;

/// <summary>
/// Marks a property as the unique key for this object type.
/// The key value is used as the RowKey in Azure Table Storage.
/// Extends <see cref="FieldAttribute"/> so the property is automatically
/// indexed as a Lucene document key field (no separate [Field(Key=true)] needed).
/// Use <see cref="Mode"/> to control key generation.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class KeyAttribute : FieldAttribute
{
    public KeyAttribute() : base(IndexMode.NotAnalyzed)
    {
        Key = true;
    }

    /// <summary>The strategy for generating the key. Defaults to <see cref="KeyMode.Manual"/>.</summary>
    public KeyMode Mode { get; set; } = KeyMode.Manual;
}
