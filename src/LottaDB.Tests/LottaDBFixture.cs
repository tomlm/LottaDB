using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class LottaDBFixture : IDisposable
{
    public LottaDBFixture()
    {
    }

    public void Dispose() { }

    public static async Task<LottaDB> CreateDbAsync(Action<ILottaConfiguration>? configureAction = null,
        bool reset = true,
        [CallerMemberName] string? testName = null)
    {
        testName = string.Join("", testName!.Where(char.IsLetterOrDigit).Take(60));
        var db = new LottaDB(testName!, "UseDevelopmentStorage=true", config =>
        {
            config.ConfigureTestStorage();

            config.Store<Actor>();
            config.Store<Note>();
            config.Store<NoteView>();
            config.Store<ModerationView>();
            config.Store<OrderWithLines>();
            config.Store<CycleA>();
            config.Store<CycleB>();
            config.Store<FeedEntry>();
            config.Store<LogEntry>();
            config.Store<BaseEntity>();
            config.Store<Person>();
            config.Store<Employee>();

            configureAction?.Invoke(config);
        });
        if (reset)
            await db.ResetAsync();
        return db;
    }
}
