using Lucene.Net.Analysis;

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

    /// <summary>
    /// Gets or sets the delay, in milliseconds, before an automatic commit is performed after a change.
    /// </summary>
    public int AutoCommitDelay { get; set; } = 1000;

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
