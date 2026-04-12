namespace LottaDB;

/// <summary>
/// Marks a property as the row key for Azure Table Storage.
/// Use <see cref="Strategy"/> to control how the RowKey is generated:
/// <see cref="RowKeyStrategy.Natural"/> uses the property value directly (upsert semantics),
/// <see cref="RowKeyStrategy.DescendingTime"/> generates a time-ordered key (newest first),
/// <see cref="RowKeyStrategy.AscendingTime"/> generates a time-ordered key (oldest first).
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class RowKeyAttribute : Attribute
{
    /// <summary>The strategy for generating the RowKey. Defaults to <see cref="RowKeyStrategy.Natural"/>.</summary>
    public RowKeyStrategy Strategy { get; set; } = RowKeyStrategy.Natural;
}
