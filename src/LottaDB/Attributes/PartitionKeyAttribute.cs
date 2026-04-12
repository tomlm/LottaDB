namespace LottaDB;

/// <summary>
/// Marks a property as the partition key for Azure Table Storage.
/// The property value is used as the PartitionKey when storing the object.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class PartitionKeyAttribute : Attribute
{
}
