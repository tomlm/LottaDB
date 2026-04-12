namespace LottaDB;

/// <summary>
/// Describes a failure that occurred in a builder during a write operation.
/// The source object's save is not affected — builder errors are captured, not thrown.
/// </summary>
public record BuilderError
{
    /// <summary>The name of the builder that failed.</summary>
    public required string BuilderName { get; init; }
    /// <summary>The CLR type name of the trigger object.</summary>
    public required string TriggerTypeName { get; init; }
    /// <summary>The entity key of the trigger object.</summary>
    public required string TriggerKey { get; init; }
    /// <summary>The exception thrown by the builder.</summary>
    public required Exception Exception { get; init; }
}
