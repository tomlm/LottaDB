namespace LottaDB;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class RowKeyAttribute : Attribute
{
    public RowKeyStrategy Strategy { get; set; } = RowKeyStrategy.Natural;
}
