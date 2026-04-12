namespace LottaDB;

public record ObjectChange
{
    public required string TypeName { get; init; }
    public required string Key { get; init; }
    public required ChangeKind Kind { get; init; }
    public object? Object { get; init; }
}

public record ObjectChange<T>
{
    public required string Key { get; init; }
    public T? Object { get; init; }
    public required ChangeKind Kind { get; init; }
}
