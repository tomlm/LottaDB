namespace Lotta;

/// <summary>
/// Indicates the kind of change that occurred to an object.
/// </summary>
public enum ChangeKind
{
    /// <summary>The object was saved (created or updated).</summary>
    Saved,
    /// <summary>The object was deleted.</summary>
    Deleted
}
