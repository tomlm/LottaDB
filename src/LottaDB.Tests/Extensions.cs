using Azure.Data.Tables;
using Lucene.Net.Store;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Tables;

namespace Lotta.Tests
{
    public static class Extensions
    {
        public static LottaCatalog ConfigureTestStorage(this LottaCatalog catalog)
        {
            catalog.TableServiceClientFactory = CreateMockTableServiceClient;
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
