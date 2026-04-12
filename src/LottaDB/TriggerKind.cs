namespace LottaDB;

/// <summary>
/// Indicates the operation that triggered a builder invocation.
/// </summary>
public enum TriggerKind
{
    /// <summary>The trigger object was saved (created or updated).</summary>
    Saved,
    /// <summary>The trigger object was deleted.</summary>
    Deleted
}
