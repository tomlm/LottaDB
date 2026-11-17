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

    /// <inheritdoc/>
    public AsyncHandlerMode AsyncHandlers { get; set; } = AsyncHandlerMode.Auto;

    /// <inheritdoc/>
    public int AsyncHandlerMaxHops { get; set; } = 16;

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

    /// <summary>
    /// Registers an asynchronous handler to be invoked when a trigger occurs for entities of the specified type.
    /// </summary>
    public ILottaConfiguration On<T>(EntityHandler<T> handler) where T : class, new()
    {
        OnRegistrations.Add(new OnRegistration(typeof(T), handler));
        return this;
    }

    /// <inheritdoc/>
    public ILottaConfiguration OnAsync<T>(string name, EntityHandler<T> handler, int priority = 0, bool latest = true, AsyncHandlerOptions? options = null)
        where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (name is null || !Internal.AsyncQueueNames.IsValidHandlerName(name))
            throw new ArgumentException(
                $"'{name}' is not a handler name. Use 3 to 40 lower-case letters and digits, with single hyphens between them (\"describe-mail\").", nameof(name));
        if (AsyncRegistrations.Any(r => r.Name == name))
            throw new ArgumentException($"A queued handler named '{name}' is already registered.", nameof(name));
        options ??= new AsyncHandlerOptions();
        if (options.MaxAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxAttempts must be at least 1.");
        if (options.Timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "Timeout must be positive.");
        if (options.RetryDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "RetryDelay cannot be negative.");

        AsyncRegistrations.Add(new AsyncRegistration(
            name, typeof(T), priority, latest, options,
            (entity, db, cancellationToken) => handler((T)entity, TriggerKind.Saved, db, cancellationToken)));
        return this;
    }

    /// <summary>
    /// Set the blob upload handler. Replacement semantics: last one wins.
    /// Call with no arguments to use the default handler (extension-based mime type detection,
    /// text content extraction for known text formats).
    /// Use <c>On&lt;BlobFile&gt;</c> for additional processing after upload (CSAM scanning, thumbnails, etc.).
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

/// <summary>A queued handler as registered: what it handles, in what order, and how to call it.</summary>
internal sealed record AsyncRegistration(
    string Name, Type ObjectType, int Priority, bool Latest, AsyncHandlerOptions Options,
    Func<object, LottaDB, CancellationToken, Task> Invoke);
