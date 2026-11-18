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
    /// <summary>The queued rules (<c>Update</c>, <c>Then</c>), in the order they were registered.</summary>
    internal List<QueuedRule> QueuedRules { get; } = new();
    internal List<RuleRegistration> RuleRegistrations { get; } = new();
    /// <summary>The types that have a rule whose filter uses <c>Changed(...)</c>: their saves note the version they replace.</summary>
    internal HashSet<Type> ChangedTypes { get; } = new();

    /// <summary>Creates an empty configuration.</summary>
    public LottaConfiguration() => Rules = new RuleSet(this);

    /// <inheritdoc/>
    public RuleSet Rules { get; }

    /// <inheritdoc/>
    public QueueMode QueueMode { get; set; } = QueueMode.Auto;

    /// <inheritdoc/>
    public int MaxQueueHops { get; set; } = 16;

    /// <inheritdoc/>
    public int MaxConcurrency
    {
        get => _maxConcurrency;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _maxConcurrency = value;
        }
    }
    private int _maxConcurrency = 4;

    /// <inheritdoc/>
    public int RuleMaxAttempts
    {
        get => _ruleMaxAttempts;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _ruleMaxAttempts = value;
        }
    }
    private int _ruleMaxAttempts = 5;

    /// <inheritdoc/>
    public TimeSpan RuleRetryDelay
    {
        get => _ruleRetryDelay;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            _ruleRetryDelay = value;
        }
    }
    private TimeSpan _ruleRetryDelay = TimeSpan.FromSeconds(2);

    /// <inheritdoc/>
    public TimeSpan RuleTimeout
    {
        get => _ruleTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            _ruleTimeout = value;
        }
    }
    private TimeSpan _ruleTimeout = TimeSpan.FromMinutes(5);

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
                : $"A rule for saves of {name} has no action: follow Rules.When<{name}>(...) with .Do(handler), .Update(handler) or .Then(handler).");
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

    /// <summary>
    /// Registers a queued action of a rule (<c>Update</c> or <c>Then</c>), checking its name. The
    /// rule's identity is the type and the name, <c>Note/summarize</c>, and must be unique.
    /// </summary>
    internal void AddQueuedRule<T>(RuleVerb verb, string name, Internal.RuleFilter<T> filter, RuleHandler<T> handler)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(handler);
        ThrowIfOpened();
        if (name is null || !Internal.RuleQueueNames.IsValidRuleName(name))
            throw new ArgumentException(
                $"'{name}' is not a rule name. Use 3 to 40 lower-case letters and digits, with single hyphens between them (\"describe-mail\").", nameof(name));
        var id = $"{typeof(T).Name}/{name}";
        if (QueuedRules.Any(r => r.Id == id))
            throw new ArgumentException($"A rule '{id}' is already registered. Give one of the two another name.", nameof(name));

        var onSave = filter.OnSave;
        var rest = filter.Rest;
        var restExpression = filter.RestExpression;
        QueuedRules.Add(new QueuedRule
        {
            Id = id,
            Name = name,
            ObjectType = typeof(T),
            Verb = verb,
            Order = QueuedRules.Count,
            Invoke = (entity, context, cancellationToken) => handler((T)entity, context, cancellationToken),
            OnSave = onSave == null ? null : entity => onSave((T)entity),
            Rest = rest == null ? null : entity => rest((T)entity),
            // Captured while T is known: the objects the index says the filter is true for.
            FindIndexed = restExpression == null ? null : db => db.Search<T>().Where(restExpression).ToList(),
            Inputs = [.. filter.Inputs.Select(input => (Func<object, object?>)(entity => input((T)entity)))],
            UsesChanged = filter.UsesChanged,
        });
        if (filter.UsesChanged)
            ChangedTypes.Add(typeof(T));
    }

    /// <summary>
    /// Set the blob upload handler. Replacement semantics: last one wins.
    /// Call with no arguments to use the default handler (extension-based mime type detection,
    /// text content extraction for known text formats).
    /// Use <c>Rules.When&lt;BlobFile&gt;().Do(...)</c>, <c>.Update(...)</c> or <c>.Then(...)</c> for additional processing after
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
