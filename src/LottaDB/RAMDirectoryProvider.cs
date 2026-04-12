namespace LottaDB;

/// <summary>
/// In-memory Lucene directory provider. Each type gets its own RAMDirectory.
/// Use for unit tests alongside Spotflow in-memory table storage.
/// </summary>
public class RAMDirectoryProvider : IDirectoryProvider
{
    private readonly Dictionary<string, Lucene.Net.Store.RAMDirectory> _directories = new();

    public Lucene.Net.Store.Directory GetDirectory(string typeName)
    {
        if (!_directories.TryGetValue(typeName, out var dir))
        {
            dir = new Lucene.Net.Store.RAMDirectory();
            dir.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());
            _directories[typeName] = dir;
        }
        return dir;
    }
}
