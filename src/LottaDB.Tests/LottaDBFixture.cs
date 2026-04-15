using Azure;
using Azure.Data.Tables;
using Lotta;
using Lucene.Net.Util;
using Microsoft.Extensions.DependencyInjection;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Tables;
using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class LottaDBFixture : IDisposable
{
    public LottaDB Db { get; }

    public LottaDBFixture()
    {
        Db = CreateDb();
    }

    public void Dispose() { }

    public static TableServiceClient CreateTableServiceClient()
    {
        //var provider = new InMemoryStorageProvider();
        //var account = provider.AddAccount($"test{Guid.NewGuid():N}");
        //return InMemoryTableServiceClient.FromAccount(account);
        return new TableServiceClient("UseDevelopmentStorage=true");
    }

    public static Lucene.Net.Store.Directory CreateLuceneDirectory()
    {
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());
        return directory;
    }

    public static LottaDB CreateDb(Action<ILottaConfiguration>? configure = null,
        [CallerMemberName] string? testName = null)
    {
        var tableClient = CreateTableServiceClient();
        var directory = CreateLuceneDirectory();
        //foreach(var table in tableClient.Query().Where(t => t.Name.StartsWith("test")))
        //{
        //    tableClient.DeleteTable(table.Name);
        //}
        testName = String.Join("", testName.Where(c => char.IsLetterOrDigit(c)));
        tableClient.DeleteTable(testName);

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

        return new LottaDB(testName, tableClient, directory, options);
    }
}
