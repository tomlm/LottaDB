using System.Runtime.CompilerServices;

namespace Lotta.Tests;

/// <summary>
/// Storage provider constants for parameterized tests.
/// </summary>
public static class Provider
{
    public const string Memory = "Memory";
    public const string FileSystem = "FileSystem";
    public const string SQLite = "SQLite";
    public const string Azurite = "Azurite";
}

/// <summary>
/// Base class for all LottaDB tests. Creates a catalog per test instance
/// and disposes it (including all databases) when the test completes.
/// Derive per-provider subclasses to run all tests against every storage provider.
/// </summary>
public abstract class LottaTestBase : IDisposable
{
    private readonly LottaCatalog _catalog;

    protected LottaTestBase(string provider = Provider.Memory)
    {
        var sanitized = string.Join("", GetType().Name.Where(char.IsLetterOrDigit).Take(60));
        _catalog = new LottaCatalog(sanitized);
        _catalog.ConfigureTestStorage(provider);
    }

    /// <summary>The catalog for this test instance.</summary>
    protected LottaCatalog Catalog => _catalog;

    /// <summary>
    /// Create a database with the standard test types registered.
    /// Each call with a different databaseId creates a separate isolated database.
    /// </summary>
    protected virtual async Task<LottaDB> CreateDbAsync(
        Action<ILottaConfiguration>? configureAction = null,
        bool reset = true,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null)
    {
        var databaseId = string.Join("", testName!.Where(char.IsLetterOrDigit).Take(60));
        var db = await _catalog.GetDatabaseAsync(databaseId, config =>
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

    /// <summary>
    /// Create a database within a specific catalog (for tests that need custom catalog config).
    /// </summary>
    protected static async Task<LottaDB> CreateDbAsync(
        LottaCatalog catalog,
        string databaseId,
        CancellationToken ct = default)
    {
        return await catalog.GetDatabaseAsync(databaseId, config =>
        {
            config.Store<Actor>();
            config.Store<Note>();
        }, ct);
    }

    /// <summary>
    /// Create a catalog with custom configuration (for tests that need multiple catalogs
    /// or provider-specific settings).
    /// </summary>
    protected static LottaCatalog CreateCatalog(
        Action<LottaCatalog>? configure = null,
        [CallerMemberName] string? testName = null)
    {
        var sanitized = string.Join("", testName!.Where(char.IsLetterOrDigit).Take(60));
        var catalog = new LottaCatalog(sanitized);
        catalog.ConfigureTestStorage();
        configure?.Invoke(catalog);
        return catalog;
    }

    public void Dispose()
    {
        _catalog.Dispose();
    }
}
