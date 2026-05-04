using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Lucene.Net.Store;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Blobs;
using Spotflow.InMemory.Azure.Storage.Tables;

namespace Lotta.Tests
{
    public static class Extensions
    {
        public static LottaCatalog ConfigureTestStorage(this LottaCatalog catalog)
        {
            // Use a shared provider so table + blob storage share the same in-memory account
            var provider = new InMemoryStorageProvider();
            var account = provider.AddAccount(catalog.Name);

            catalog.TableServiceClientFactory = _ => InMemoryTableServiceClient.FromAccount(account);
            catalog.BlobServiceClientFactory = _ => InMemoryBlobServiceClient.FromAccount(account);
            catalog.LuceneDirectoryFactory = CreateMockDirectory;
            return catalog;
        }

        public static TableServiceClient CreateMockTableServiceClient(string name)
        {
            var provider = new InMemoryStorageProvider();
            var account = provider.AddAccount(name);
            return InMemoryTableServiceClient.FromAccount(account);
        }

        public static Lucene.Net.Store.Directory CreateMockDirectory(string name)
        {
            var directory = new RAMDirectory();
            directory.SetLockFactory(NoLockFactory.GetNoLockFactory());
            return directory;
        }
    }
}
