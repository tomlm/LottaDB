namespace LottaDB;

public class FSDirectoryProvider : IDirectoryProvider
{
    private readonly string _basePath;
    private readonly Dictionary<string, Lucene.Net.Store.FSDirectory> _directories = new();

    public FSDirectoryProvider(string basePath)
    {
        _basePath = basePath;
    }

    public Lucene.Net.Store.Directory GetDirectory(string typeName)
    {
        if (!_directories.TryGetValue(typeName, out var dir))
        {
            var path = Path.Combine(_basePath, typeName);
            System.IO.Directory.CreateDirectory(path);
            dir = Lucene.Net.Store.FSDirectory.Open(path);
            _directories[typeName] = dir;
        }
        return dir;
    }
}
