namespace LottaDB;

public record BuilderError
{
    public required string BuilderName { get; init; }
    public required string TriggerTypeName { get; init; }
    public required string TriggerKey { get; init; }
    public required Exception Exception { get; init; }
}
