using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tables;

namespace Lotta.Tests
{
    public static class Extensions
    {
        private static readonly string _runRoot;

        public static LottaCatalog ConfigureTestStorage(this LottaCatalog catalog)
        {
            // UseMemoryClient(catalog);
            UseFileSystemClient(catalog);
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
        }

        public static void UseMemoryClient(LottaCatalog catalog)
        {
            var tableClient = new MemoryTableServiceClient();
            var blobClient = new MemoryBlobServiceClient();
            catalog.TableServiceClientFactory = () => tableClient;
            catalog.BlobServiceClientFactory = () => blobClient;
        }

        public static void UseFileSystemClient(LottaCatalog catalog)
        {
            catalog.TableServiceClientFactory = () => new FileTableServiceClient(_runRoot);
            catalog.BlobServiceClientFactory = () => new FileBlobServiceClient(_runRoot);
        }

        public static void UseSQLite(LottaCatalog catalog)
        {
            var dbPath = Path.Combine(_runRoot, $"{catalog.Name}.db");
            catalog.TableServiceClientFactory = () => new SqliteTableServiceClient(dbPath);
            catalog.BlobServiceClientFactory = () => new SqliteBlobServiceClient(dbPath);
        }

    }
}
