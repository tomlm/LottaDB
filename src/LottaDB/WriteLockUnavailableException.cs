namespace Lotta;

/// <summary>
/// Thrown when the cross-process Lucene write lock could not be obtained within
/// <see cref="ILottaConfiguration.WriteLockTimeout"/>. Another process currently owns the
/// writer role for this database. No changes were applied — retry, or route the write to
/// the process that holds the lock.
/// </summary>
public class WriteLockUnavailableException : Exception
{
    /// <summary>The database that could not be locked for writing.</summary>
    public string DatabaseId { get; }

    /// <summary>Create a WriteLockUnavailableException.</summary>
    public WriteLockUnavailableException(string databaseId, Exception? innerException = null)
        : base($"Could not obtain the Lucene write lock for database '{databaseId}': " +
               "another process currently owns the writer role. No changes were made.",
               innerException)
    {
        DatabaseId = databaseId;
    }
}
