namespace Lotta;

/// <summary>
/// Determines how the object key is generated.
/// </summary>
public enum KeyMode
{
    /// <summary>Use the property value directly as the key.</summary>
    Manual,
    /// <summary>Auto generate a ULID key if the property is empty at save time.</summary>
    Auto
}
