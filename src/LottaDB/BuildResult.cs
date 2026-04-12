namespace LottaDB;

/// <summary>
/// A result yielded by an <see cref="IBuilder{TTrigger, TDerived}"/>.
/// Set <see cref="Object"/> to save a derived object, or set it to null
/// and provide <see cref="Key"/> to delete a derived object by key.
/// </summary>
public record BuildResult<T>
{
    /// <summary>The derived object to save. Null signals a delete (use <see cref="Key"/>).</summary>
    public T? Object { get; init; }
    /// <summary>The key of the derived object to delete. Used when <see cref="Object"/> is null.</summary>
    public string? Key { get; init; }
}
