namespace Lotta;

/// <summary>
/// Describes a change to an object (non-generic). Contained in <see cref="ObjectResult.Changes"/>.
/// </summary>
public record ObjectChange
{
    /// <summary>The CLR type name of the changed object (e.g. "NoteView").</summary>
    public required Type Type { get; init; }

    /// <summary>The entity key (partition key + row key) of the changed object.</summary>
    public required string Key { get; init; }

    /// <summary>Whether the object was saved or deleted.</summary>
    public required ChangeKind Kind { get; init; }

    /// <summary>The full typed object, or null if deleted.</summary>
    public object? Object { get; init; }
}

/// <summary>
/// Describes a change to an object of type <typeparamref name="T"/>.
/// Passed to <see cref="LottaDB.Observe{T}"/> callbacks.
/// </summary>
public record ObjectChange<T>
{
    /// <summary>The entity key of the changed object.</summary>
    public required string Key { get; init; }

    /// <summary>The full typed object, or default if deleted.</summary>
    public T? Object { get; init; }

    /// <summary>Whether the object was saved or deleted.</summary>
    public required ChangeKind Kind { get; init; }
}
