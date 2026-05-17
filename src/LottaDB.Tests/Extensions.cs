using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Memory;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Lucene.Net.Store;

namespace Lotta.Tests
{
    public static class Extensions
    {
        public static LottaCatalog ConfigureTestStorage(this LottaCatalog catalog)
        {
            var provider = new MemoryStorageProvider();
            var account = provider.AddAccount(catalog.Name);

            catalog.TableServiceClientFactory = _ => new MemoryTableServiceClient(account);
            catalog.BlobServiceClientFactory = _ => MemoryBlobServiceClient.FromAccount(account);
            catalog.LuceneDirectoryFactory = CreateMockDirectory;
            return catalog;
        }

        public static TableServiceClient CreateMockTableServiceClient(string name)
        {
            var provider = new MemoryStorageProvider();
            var account = provider.AddAccount(name);
            return new MemoryTableServiceClient(account);
        }

        public static Lucene.Net.Store.Directory CreateMockDirectory(string name)
        {
            var directory = new RAMDirectory();
            directory.SetLockFactory(NoLockFactory.GetNoLockFactory());
            return directory;
        }
    }
}
