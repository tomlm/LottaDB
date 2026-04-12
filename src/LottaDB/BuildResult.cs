namespace LottaDB;

public record BuildResult<T>
{
    public T? Object { get; init; }
    public string? Key { get; init; }
}
