namespace Lotta;

/// <summary>
/// The result of a <see cref="LottaDB.SaveAsync{T}"/>, <see cref="LottaDB.ChangeAsync{T}"/>,
/// or <see cref="LottaDB.DeleteAsync{T}"/> operation. Contains all object changes (including
/// derived objects produced by builders) and any builder errors.
/// </summary>
public record ObjectResult
{
    /// <summary>All objects that were saved or deleted, including derived objects from builders.</summary>
    public IReadOnlyList<ObjectChange> Changes { get; init; } = [];
    /// <summary>Builder failures, if any. Empty on success. The source save is never affected by builder errors.</summary>
    public IReadOnlyList<BuilderError> Errors { get; init; } = [];
}
