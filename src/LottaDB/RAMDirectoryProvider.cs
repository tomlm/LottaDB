namespace LottaDB;

public class RAMDirectoryProvider : IDirectoryProvider
{
    private readonly Dictionary<string, Lucene.Net.Store.RAMDirectory> _directories = new();

    public Lucene.Net.Store.Directory GetDirectory(string typeName)
    {
        if (!_directories.TryGetValue(typeName, out var dir))
        {
            dir = new Lucene.Net.Store.RAMDirectory();
            _directories[typeName] = dir;
        }
        return dir;
    }
}
