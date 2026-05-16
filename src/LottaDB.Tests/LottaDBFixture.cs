using System.Runtime.CompilerServices;
using Xunit.Sdk;

namespace Lotta.Tests;

public class LottaDBFixture : IDisposable
{
    public LottaDBFixture()
    {
    }

    public void Dispose() { }

    public static async Task<LottaDB> CreateDbAsync(Action<ILottaConfiguration>? configureAction = null,
        Action<LottaCatalog>? configureCatalog = null,
        bool reset = true,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null)
    {
        testName = string.Join("", testName!.Where(char.IsLetterOrDigit).Take(60));
        var catalog = new LottaCatalog(testName!);
        catalog.ConfigureTestStorage();
        configureCatalog?.Invoke(catalog);

        var db = await catalog.GetDatabaseAsync("default", config =>
        {
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
            config.Store<VectorNote>();
            config.Store<Article>();
            config.Store<Product>();

            configureAction?.Invoke(config);
        }, cancellationToken);
        if (reset)
            await db.ResetDatabaseAsync(cancellationToken);
        return db;
    }

    public static string GetTestName([CallerMemberName] string? testName = null)
        => string.Join("", testName!.Where(char.IsLetterOrDigit).Take(60));
}
