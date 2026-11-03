using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tables;

namespace Lotta;

/// <summary>
/// Adds <see cref="UseSQLite"/> to <see cref="LottaCatalog"/> for SQLite-backed storage.
/// </summary>
public static class LottaSQLiteExtensions
{
    /// <summary>
    /// Configure the catalog to use SQLite-backed storage. All table and blob data is stored
    /// in a single <c>.db</c> file under <paramref name="rootPath"/> -- portable, atomic, and easy to manage.
    /// The Lucene index is stored as an FSDirectory alongside the database file.
    /// </summary>
    /// <param name="catalog">The catalog to configure.</param>
    /// <param name="rootPath">Root directory for storage. The database file is created as <c>{catalogName}.db</c> within this directory.</param>
    public static LottaCatalog UseSQLite(this LottaCatalog catalog, string rootPath)
    {
        var fullPath = Path.GetFullPath(rootPath);
        System.IO.Directory.CreateDirectory(fullPath);
        var dbPath = Path.Combine(fullPath, $"{catalog.Name}.db");
        return catalog.UseStorage("SQLite",
            () => new SqliteTableServiceClient(dbPath),
            () => new SqliteBlobServiceClient(dbPath),
            LuceneDirectories.FileSystem(fullPath));
    }
}
