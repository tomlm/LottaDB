namespace LottaDB;

public record ObjectResult
{
    public IReadOnlyList<ObjectChange> Changes { get; init; } = [];
    public IReadOnlyList<BuilderError> Errors { get; init; } = [];
}
