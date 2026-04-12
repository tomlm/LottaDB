using LottaDB;
using Microsoft.Extensions.DependencyInjection;

namespace LottaDB.Tests;

public class LottaDBFixture : IDisposable
{
    public ILottaDB Db { get; }
    public ServiceProvider ServiceProvider { get; }

    public LottaDBFixture()
    {
        var services = new ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
            opts.UseLuceneDirectory(new RAMDirectoryProvider());

            opts.Store<Actor>();
            opts.Store<Note>();
            opts.Store<NoteView>();
            opts.Store<ModerationView>();
            opts.Store<OrderWithLines>();
            opts.Store<CycleA>();
            opts.Store<CycleB>();
            opts.Store<FeedEntry>();
            opts.Store<LogEntry>();
        });

        ServiceProvider = services.BuildServiceProvider();
        Db = ServiceProvider.GetRequiredService<ILottaDB>();
    }

    public void Dispose()
    {
        ServiceProvider.Dispose();
    }
}

/// <summary>
/// Creates a fresh LottaDB instance with explicit builders registered.
/// </summary>
public static class TestLottaDBFactory
{
    public static ILottaDB CreateWithBuilders(Action<ILottaDBOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
            opts.UseLuceneDirectory(new RAMDirectoryProvider());

            opts.Store<Actor>();
            opts.Store<Note>();
            opts.Store<NoteView>();
            opts.Store<ModerationView>();
            opts.Store<OrderWithLines>();
            opts.Store<CycleA>();
            opts.Store<CycleB>();
            opts.Store<FeedEntry>();
            opts.Store<LogEntry>();

            configure?.Invoke(opts);
        });

        var sp = services.BuildServiceProvider();
        return sp.GetRequiredService<ILottaDB>();
    }
}
