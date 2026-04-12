namespace LottaDB;

/// <summary>
/// Determines how the Azure Table Storage RowKey is generated for an object.
/// </summary>
public enum RowKeyStrategy
{
    /// <summary>Use the property value directly as the RowKey. Results in upsert semantics (one row per entity).</summary>
    Natural,
    /// <summary>Generate a time-ordered RowKey with newest first. Each write appends a new row.</summary>
    DescendingTime,
    /// <summary>Generate a time-ordered RowKey with oldest first. Each write appends a new row.</summary>
    AscendingTime
}
