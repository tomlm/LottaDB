namespace LottaDB;

/// <summary>
/// Provides Lucene Directory instances for each registered type.
/// Use <see cref="RAMDirectoryProvider"/> for tests and <see cref="FSDirectoryProvider"/> for production.
/// </summary>
public interface IDirectoryProvider
{
    /// <summary>Get or create the Lucene Directory for the given type name.</summary>
    Lucene.Net.Store.Directory GetDirectory(string typeName);
}
