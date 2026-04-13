namespace Lotta;

/// <summary>
/// Determines how the object key is generated.
/// </summary>
public enum KeyStrategy
{
    /// <summary>Use the property value directly as the key. Results in upsert semantics (one row per object).</summary>
    Natural,
    /// <summary>Generate a time-ordered key with newest first. Each write appends a new row.</summary>
    DescendingTime,
    /// <summary>Generate a time-ordered key with oldest first. Each write appends a new row.</summary>
    AscendingTime
}
