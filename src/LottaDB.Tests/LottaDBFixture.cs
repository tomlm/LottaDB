using Azure;
using Azure.Data.Tables;
using Lotta;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;
using Lucene.Net.Util;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Tables;
using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class LottaDBFixture : IDisposable
{
    private const string DefaultTestName = "LottaTests";

    public LottaDBFixture()
    {
    }

    public void Dispose() { }

    public static TableServiceClient CreateMockTableServiceClient(string name)
    {
        var provider = new InMemoryStorageProvider();
        var account = provider.AddAccount(name);
        return InMemoryTableServiceClient.FromAccount(account);
    }

    public static Lucene.Net.Store.Directory CreateMockDirectory(string name)
    {
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());
        return directory;
    }

    public static LottaDB CreateDb(Action<ILottaConfiguration>? configure = null,
        [CallerMemberName] string? testName = null)
    {
        testName = string.Join("", testName.Where(char.IsLetterOrDigit).Take(60));
        return new LottaDB(testName!, null, options =>
        {
            options.CreateTableServiceClient = CreateMockTableServiceClient;
            options.CreateLuceneDirectory = CreateMockDirectory;
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
        });

    }
}
