using Lucene.Net.Analysis;
using System.Text.Json;

namespace Lotta;

/// <summary>
/// Per-database configuration for type registrations and handlers.
/// Infrastructure settings (storage factories, embedding generator, analyzer) live on <see cref="LottaCatalog"/>.
/// </summary>
public class LottaConfiguration : ILottaConfiguration
{
    internal Dictionary<Type, object> StorageConfigurations { get; } = new();
    internal List<OnRegistration> OnRegistrations { get; } = new();
    internal BlobUploadHandler? UploadHandler { get; private set; }
    internal List<AsyncRegistration> AsyncRegistrations { get; } = new();
    internal List<RuleRegistration> RuleRegistrations { get; } = new();

    /// <summary>Creates an empty configuration.</summary>
    public LottaConfiguration() => Rules = new RuleSet(this);

    /// <inheritdoc/>
    public RuleSet Rules { get; }

    /// <inheritdoc/>
    public QueueMode QueueMode { get; set; } = QueueMode.Auto;

    /// <inheritdoc/>
    public int MaxQueueHops { get; set; } = 16;

    /// <summary>
    /// Gets or sets the delay, in milliseconds, before an automatic commit is performed after a change.
    /// </summary>
    public int AutoCommitDelay { get; set; } = 50;

    /// <inheritdoc/>
    public string[] AutoKeyProperties { get; set; } = ["id", "_id", "key", "_key", "pk", "primaryKey", "uuid", "guid"];

    /// <inheritdoc/>
    public int WriteLockTimeout { get; set; } = 10000;

    /// <inheritdoc/>
    public int WriteLockReleaseDelay { get; set; } = 30000;

    /// <inheritdoc/>
    public int WriteLockMaxHoldTime { get; set; } = 0;

    /// <inheritdoc/>
    public int MaxSearchStaleness { get; set; } = 1000;

    /// <inheritdoc/>
    public bool ReadOnly { get; set; } = false;

    /// <summary>
    /// Defines a storage configuration for a specific type. This is where you can specify how a type should be stored in the database,
    /// including table name, partition key, row key, etc. If not configured, Lotta will use default conventions to determine these values.
    /// </summary>
    public ILottaConfiguration Store<T>(Action<IStorageConfiguration<T>>? configure = null) where T : class, new()
    {
        var config = new StorageConfiguration<T>();
        configure?.Invoke(config);
        StorageConfigurations[typeof(T)] = config;
        return this;
    }

    /// <summary>Throws if a rule was started with <c>When</c> or <c>WhenDeleted</c> and given no action.</summary>
    internal void CheckRules()
    {
        if (RuleRegistrations.FirstOrDefault(r => r.Actions == 0) is { } rule)
        {
            var name = rule.ObjectType.Name;
            throw new InvalidOperationException(rule.Deleted
                ? $"A rule for deletes of {name} has no action: follow Rules.WhenDeleted<{name}>(...) with .Do(handler)."
                : $"A rule for saves of {name} has no action: follow Rules.When<{name}>(...) with .Do(handler) or .Queue(name, handler).");
        }
    }

    /// <summary>
    /// Set once a database has been opened with this configuration: its rules have been read, so
    /// adding to them or changing them now would do nothing.
    /// </summary>
    internal bool Opened { get; set; }

    /// <summary>Throws if a database has already been opened with this configuration.</summary>
    internal void ThrowIfOpened()
    {
        if (Opened)
            throw new InvalidOperationException(
                "The database has already been opened with this configuration, so its rules can no longer change. " +
                "Complete each rule inside the configure callback, or add an inline rule to the open database with db.Rules.When<T>().Do(...).");
    }

    /// <summary>Registers a queued action of a rule, checking its name. The modifiers set its options afterwards.</summary>
    internal AsyncRegistration AddQueuedRule<T>(string name, System.Linq.Expressions.Expression<Func<T, bool>>? filterExpression,
        Func<T, bool>? filter, RuleHandler<T> handler)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(handler);
        ThrowIfOpened();
        if (name is null || !Internal.AsyncQueueNames.IsValidHandlerName(name))
            throw new ArgumentException(
                $"'{name}' is not a queue name. Use 3 to 40 lower-case letters and digits, with single hyphens between them (\"describe-mail\").", nameof(name));
        if (AsyncRegistrations.Any(r => r.Name == name))
            throw new ArgumentException($"A queue named '{name}' is already registered.", nameof(name));

        var registration = new AsyncRegistration(
            name, typeof(T), new QueueOptions(),
            (entity, db, cancellationToken) => handler((T)entity, db, cancellationToken),
            filter == null ? null : entity => filter((T)entity),
            filterExpression,
            // Captured while T is known: the objects the index says the filter is true for.
            filterExpression == null ? null : db => db.Search<T>().Where(filterExpression).ToList());
        AsyncRegistrations.Add(registration);
        return registration;
    }

    /// <summary>
    /// Set the blob upload handler. Replacement semantics: last one wins.
    /// Call with no arguments to use the default handler (extension-based mime type detection,
    /// text content extraction for known text formats).
    /// Use <c>Rules.When&lt;BlobFile&gt;().Do(...)</c> or <c>.Queue(...)</c> for additional processing after
    /// upload (CSAM scanning, thumbnails, etc.): uploading saves the blob's record, which fires them.
    /// Automatically registers all BlobFile types for storage.
    /// </summary>
    public ILottaConfiguration OnUpload(BlobUploadHandler? handler = null)
    {
        UploadHandler = handler ?? DefaultBlobHandler.HandleAsync;

        // Auto-register all BlobFile types if not already registered
        StoreBlobFileTypes();

        return this;
    }

    private void StoreBlobFileTypes()
    {
        RegisterIfMissing<BlobFile>();
        RegisterIfMissing<BlobPhoto>();
        RegisterIfMissing<BlobDocument>();
        RegisterIfMissing<BlobSpreadsheet>();
        RegisterIfMissing<BlobPresentation>();
        RegisterIfMissing<BlobMedia>();
        RegisterIfMissing<BlobMusic>();
        RegisterIfMissing<BlobVideo>();
        RegisterIfMissing<BlobMessage>();
        RegisterIfMissing<BlobWebPage>();
        RegisterIfMissing<BlobOfficeDocument>();
    }

    private void RegisterIfMissing<T>() where T : class, new()
    {
        if (!StorageConfigurations.ContainsKey(typeof(T)))
            Store<T>();
    }
}

internal record OnRegistration(Type ObjectType, object Handler);

/// <summary>
/// A queued action as registered: what it handles, in what order, and how to call it. When
/// <paramref name="When"/> (the rule's filter, compiled once) is set, only objects it is true for
/// are queued and run. <paramref name="Filter"/> is the filter as written, and
/// <paramref name="FindIndexed"/> runs it against the search index, for <c>RequeueAsync</c>; it
/// throws when the index cannot express the filter.
/// </summary>
internal sealed record AsyncRegistration(
    string Name, Type ObjectType, QueueOptions Options,
    Func<object, LottaDB, CancellationToken, Task> Invoke,
    Func<object, bool>? When = null,
    System.Linq.Expressions.LambdaExpression? Filter = null,
    Func<LottaDB, System.Collections.IEnumerable>? FindIndexed = null)
{
    /// <summary>Higher runs first.</summary>
    public int Priority => Options.Priority;

    /// <summary>Whether only an object's latest state matters.</summary>
    public bool Latest => Options.Latest;
}
