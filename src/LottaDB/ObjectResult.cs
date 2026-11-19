namespace Lotta;

/// <summary>
/// The result of a <see cref="LottaDB.SaveAsync{T}"/>, <see cref="LottaDB.ChangeAsync{T}"/>,
/// or <see cref="LottaDB.DeleteAsync{T}"/> operation. Contains all object changes (including
/// changes made by inline rules) and any handler errors.
/// </summary>
public record ObjectResult
{
    /// <summary>All objects that were saved or deleted, including side effects from inline rules (<c>.Do</c>).</summary>
    public IReadOnlyList<ObjectChange> Changes { get; init; } = [];

    /// <summary>Exceptions from inline rules and their filters, and from queuing work. The source save/delete is never affected by handler errors.</summary>
    public IReadOnlyList<Exception> Errors { get; init; } = [];
}
