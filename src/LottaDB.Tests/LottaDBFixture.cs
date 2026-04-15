using Azure.Data.Tables;
using Lotta;
using Microsoft.Extensions.DependencyInjection;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Tables;

namespace Lotta.Tests;

public class LottaDBFixture : IDisposable
{
    public LottaDB Db { get; }

    public LottaDBFixture()
    {
        Db = CreateDb();
    }

    public void Dispose() { }

    public static TableServiceClient CreateInMemoryTableServiceClient()
    {
        var provider = new InMemoryStorageProvider();
        var account = provider.AddAccount($"test{Guid.NewGuid():N}");
        return InMemoryTableServiceClient.FromAccount(account);
    }

    public static LottaDB CreateDb(Action<ILottaConfiguration>? configure = null)
    {
        var tableClient = CreateInMemoryTableServiceClient();
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());

        var options = new LottaConfiguration();
        options.Store<Actor>();
        options.Store<Note>();
        options.Store<NoteView>();
        options.Store<ModerationView>();
        options.Store<OrderWithLines>();
        options.Store<CycleA>();
        options.Store<CycleB>();
        options.Store<FeedEntry>();
        options.Store<LogEntry>();
        options.Store<BaseEntity>();
        options.Store<Person>();
        options.Store<Employee>();

        configure?.Invoke(options);

        return new LottaDB($"test{Guid.NewGuid():N}", tableClient, directory, options);
    }
}
