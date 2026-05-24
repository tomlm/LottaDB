using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tables;
using Lucene.Net.Store;

namespace Lotta.Tests
{
    public static class Extensions
    {
        private static readonly string _runRoot;

        public static LottaCatalog ConfigureTestStorage(this LottaCatalog catalog)
        {
            //UseMemoryClient(catalog);
            UseSQLite(catalog);
            // UseFileSystemClient(catalog);
            // UseAzuriteClient(catalog);
            return catalog;
        }

        static Extensions()
        {
            var baseDir = Path.Combine(Path.GetTempPath(), "LottaTests");
            _runRoot = Path.Combine(baseDir, $"run-{DateTime.Now:yyyyMMdd-HHmmss}");
            System.IO.Directory.CreateDirectory(_runRoot);

            // Keep only the 2 most recent runs
            if (System.IO.Directory.Exists(baseDir))
            {
                var oldRuns = System.IO.Directory.GetDirectories(baseDir, "run-*")
                    .OrderByDescending(d => d)
                    .Skip(2)
                    .ToList();
                foreach (var old in oldRuns)
                    try { System.IO.Directory.Delete(old, true); } catch { }
            }
        }

        public static void UseAzuriteClient(LottaCatalog catalog)
        {
            var tableClient = new TableServiceClient("UseDevelopmentStorage=true");
            var blobClient = new MemoryBlobServiceClient();
            catalog.TableServiceClientFactory = () => tableClient;
            catalog.BlobServiceClientFactory = () => blobClient;
            // Azurite: AzureDirectory persists to blob storage with default FSDirectory cache
        }

        public static void UseMemoryClient(LottaCatalog catalog)
        {
            var tableClient = new MemoryTableServiceClient();
            var blobClient = new MemoryBlobServiceClient();
            catalog.TableServiceClientFactory = () => tableClient;
            catalog.BlobServiceClientFactory = () => blobClient;
            // Memory: FSDirectory for Lucene to avoid RAMDirectory concurrency issues
            catalog.LuceneDirectoryFactory = path => new RAMDirectory();
        }

        public static void UseFileSystemClient(LottaCatalog catalog)
        {
            catalog.TableServiceClientFactory = () => new FileTableServiceClient(_runRoot);
            catalog.BlobServiceClientFactory = () => new FileBlobServiceClient(_runRoot);
            // FileSystem: FSDirectory alongside the data
            catalog.LuceneDirectoryFactory = path =>
            {
                var dir = Path.Combine(_runRoot, path.Replace('/', Path.DirectorySeparatorChar));
                System.IO.Directory.CreateDirectory(dir);
                return FSDirectory.Open(dir);
            };
        }

        public static void UseSQLite(LottaCatalog catalog)
        {
            var dbPath = Path.Combine(_runRoot, $"{catalog.Name}.db");
            catalog.TableServiceClientFactory = () => new SqliteTableServiceClient(dbPath);
            catalog.BlobServiceClientFactory = () => new SqliteBlobServiceClient(dbPath);
            // SQLite: FSDirectory alongside the .db file
            catalog.LuceneDirectoryFactory = path =>
            {
                var dir = Path.Combine(_runRoot, path.Replace('/', Path.DirectorySeparatorChar));
                System.IO.Directory.CreateDirectory(dir);
                return FSDirectory.Open(dir);
            };
        }

    }
}
