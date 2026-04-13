using Azure.Data.Tables;
using LottaDB;
using Microsoft.Extensions.DependencyInjection;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Tables;

namespace LottaDB.Tests;

public class LottaDBFixture : IDisposable
{
    public ILottaDB Db { get; }

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

    public static ILottaDB CreateDb(Action<ILottaDBOptions>? configure = null)
    {
        var tableClient = CreateInMemoryTableServiceClient();
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());

        var options = new LottaDBOptions();
        options.Store<Actor>();
        options.Store<Note>();
        options.Store<NoteView>();
        options.Store<ModerationView>();
        options.Store<OrderWithLines>();
        options.Store<CycleA>();
        options.Store<CycleB>();
        options.Store<FeedEntry>();
        options.Store<LogEntry>();

        configure?.Invoke(options);

        return new LottaDB($"test{Guid.NewGuid():N}", tableClient, directory, options);
    }
}

/// <summary>
/// Creates a fresh LottaDB instance per test.
/// </summary>
public static class TestLottaDBFactory
{
    public static ILottaDB CreateWithBuilders(Action<ILottaDBOptions>? configure = null)
    {
        return LottaDBFixture.CreateDb(configure);
    }
}
