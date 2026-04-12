namespace LottaDB;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class TagAttribute : Attribute
{
    public string? Name { get; set; }
}
