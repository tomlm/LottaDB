namespace Lotta;

/// <summary>
/// Thrown when a conditional save fails because the entity was modified by another writer
/// (the supplied ETag no longer matches the stored version).
/// </summary>
public class ConcurrencyException : Exception
{
    /// <summary>The entity key that had the conflict.</summary>
    public string Key { get; }

    /// <summary>The CLR type of the entity.</summary>
    public Type EntityType { get; }

    /// <summary>Create a ConcurrencyException.</summary>
    public ConcurrencyException(string key, Type entityType)
        : base($"Concurrency conflict on {entityType.Name} '{key}': the entity was modified by another writer.")
    {
        Key = key;
        EntityType = entityType;
    }
}
