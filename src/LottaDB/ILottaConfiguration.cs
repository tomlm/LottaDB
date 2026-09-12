using System.Linq.Expressions;
using System.Text.Json;

namespace Lotta;

/// <summary>
/// Handler invoked after an entity is saved or deleted.
/// </summary>
/// <typeparam name="T">The entity type.</typeparam>
/// <param name="entity">The entity that was saved or deleted.</param>
/// <param name="kind">Whether the entity was saved or deleted.</param>
/// <param name="db">The database instance, for querying or saving related entities.</param>
public delegate Task EntityHandler<in T>(T entity, TriggerKind kind, LottaDB db, CancellationToken cancellationToken);

/// <summary>
/// Handler invoked when a blob is uploaded. Receives the blob content and returns metadata to store.
/// </summary>
/// <param name="path">Blob path relative to the database (e.g. "photos/vacation.jpg").</param>
/// <param name="contentType">MIME type (explicit or detected from file extension).</param>
/// <param name="content">A readable stream of the blob content.</param>
/// <param name="db">The database instance, for querying or saving related entities.</param>
/// <returns>A <see cref="BlobFile"/> (or subclass) to save as metadata, or null to skip.</returns>
public delegate Task<BlobFile?> BlobUploadHandler(string path, string? contentType, Stream content, LottaDB db, CancellationToken cancellationToken);

/// <summary>
/// Per-database configuration for type registrations and handlers.
/// Infrastructure settings (storage factories, embedding generator, analyzer) live on <see cref="LottaCatalog"/>.
/// </summary>
public interface ILottaConfiguration
{
    /// <summary>
    /// Gets or sets the interval, in milliseconds, at which automatic search commits are performed.
    /// </summary>
    public int AutoCommitDelay { get; set; }

    /// <summary>
    /// Convention-based key property names for auto-detecting document keys.
    /// Used by both POCOs (property name matching) and POJOs (JSON property name matching).
    /// First match wins (case-insensitive). Set to override the defaults.
    /// Default: id, _id, key, _key, pk, primaryKey, uuid, guid.
    /// </summary>
    public string[] AutoKeyProperties { get; set; }

    /// <summary>
    /// Milliseconds to wait for the cross-process Lucene write lock before throwing
    /// <see cref="WriteLockUnavailableException"/>. Lucene polls at roughly one-second
    /// granularity. 0 fails immediately. Default: 10000.
    /// </summary>
    public int WriteLockTimeout { get; set; }

    /// <summary>
    /// Milliseconds of write inactivity after which the Lucene write lock is committed and
    /// released, so another process can take over the writer role. The lock is re-acquired
    /// transparently on the next write. -1 holds it until <see cref="LottaDB.Dispose()"/> or
    /// <see cref="LottaDB.ReleaseWriteLockAsync"/>. Default: 5000.
    /// </summary>
    public int WriteLockReleaseDelay { get; set; }

    /// <summary>
    /// Maximum milliseconds the write lock may be held continuously before it is voluntarily
    /// released to give other processes a turn. 0 disables. Only useful when several processes
    /// write continuously — otherwise <see cref="WriteLockReleaseDelay"/> already hands the
    /// writer role over whenever writes go idle. Default: 0.
    /// <para>
    /// After yielding, this process will not re-acquire the writer for about 1.5 seconds. That
    /// back-off is required rather than incidental: Lucene's lock acquisition polls once per
    /// second, so re-taking the lock on the next write — potentially milliseconds later — would
    /// beat any waiting process back to it and the yield would accomplish nothing. Expect writes
    /// on this instance to stall for that back-off each time the limit is hit.
    /// </para>
    /// </summary>
    public int WriteLockMaxHoldTime { get; set; }

    /// <summary>
    /// Maximum milliseconds a <c>Search</c> may serve results without re-checking the Lucene
    /// directory for commits made by other processes. 0 checks on every search; -1 never polls.
    /// This process's own writes are always immediately visible regardless of this setting.
    /// Default: 1000.
    /// </summary>
    public int MaxSearchStaleness { get; set; }

    /// <summary>
    /// Open the database for reads only. Any write throws immediately instead of attempting to
    /// acquire the cross-process write lock, and no schema manifest or index rebuild is written
    /// at open. Default: false.
    /// </summary>
    public bool ReadOnly { get; set; }

    /// <summary>Register an object type. Config from [Key]/[Queryable] attributes, or fluent override.</summary>
    /// <typeparam name="T">The object type to register.</typeparam>
    /// <param name="configure">Optional fluent configuration for key strategy, queryable properties, etc.</param>
    ILottaConfiguration Store<T>(Action<IStorageConfiguration<T>>? configure = null) where T : class, new();

    /// <summary>
    /// Register a handler that runs inline after every save or delete of type <typeparamref name="T"/>.
    /// The handler has full DB access — it can save, delete, query, or search any type.
    /// Multiple handlers per type are allowed and run in registration order.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="handler">Async handler receiving the object, trigger kind, and DB instance.</param>
    ILottaConfiguration On<T>(EntityHandler<T> handler) where T : class, new();

    /// <summary>
    /// Set the blob upload handler (replacement semantics: last one wins).
    /// Call with no arguments to use the default handler (extension-based MIME detection, text extraction).
    /// Use <c>On&lt;BlobFile&gt;</c> for additional post-upload processing (CSAM scanning, thumbnails, etc.).
    /// </summary>
    /// <param name="handler">Handler that processes blob content and returns metadata to store.</param>
    ILottaConfiguration OnUpload(BlobUploadHandler? handler = null);

}

/// <summary>
/// Fluent configuration for how an object type is stored in Azure Table Storage and indexed in Lucene.
/// Used inside <c>opts.Store&lt;T&gt;(s =&gt; ...)</c>.
/// </summary>
/// <typeparam name="T">The object type being configured.</typeparam>
public interface IStorageConfiguration<T> where T : class, new()
{
    /// <summary>Set the key using a custom expression. For composite keys (e.g. <c>s.SetKey(x =&gt; $"{x.Domain}/{x.Id}")</c>).</summary>
    IStorageConfiguration<T> SetKey(Expression<Func<T, string>> resolver);

    /// <summary>Set the key strategy for time-ordered objects.</summary>
    IStorageConfiguration<T> SetKey(KeyMode strategy);

    /// <summary>
    /// Make a property queryable: promotes it to a Table Storage column for server-side
    /// filtering AND indexes it in Lucene for search.
    /// </summary>
    IQueryableConfiguration AddQueryable<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Promote a property to a Table Storage column only (not indexed in Lucene).</summary>
    IStorageConfiguration<T> AddTag<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Index a property in Lucene only (not promoted to a Table Storage column).</summary>
    IFieldConfiguration AddField<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>Exclude a property from both table storage and Lucene indexing.</summary>
    IStorageConfiguration<T> Ignore<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>
    /// Set the default search property for free-text queries.
    /// </summary>
    IStorageConfiguration<T> DefaultSearch<TProp>(Expression<Func<T, TProp>> property);

    /// <summary>
    /// Control AutoQueryable behavior. When enabled (default), all top-level simple-type
    /// properties are auto-promoted as queryable (Table Storage columns + Lucene fields)
    /// and a _content_ field is built for full-text search.
    /// Pass false to disable and only index explicitly configured properties.
    /// </summary>
    IStorageConfiguration<T> AutoQueryable(bool enabled = true);
}

/// <summary>
/// Fluent options for a queryable property.
/// </summary>
public interface IQueryableConfiguration
{
    /// <summary>Index with full-text analysis (tokenized, searchable by terms).</summary>
    IQueryableConfiguration Analyzed();
    /// <summary>Index as-is for exact match filtering only.</summary>
    IQueryableConfiguration NotAnalyzed();
    /// <summary>Enable vector embeddings for similarity search.</summary>
    IQueryableConfiguration Vector();
}

/// <summary>
/// Fluent options for a Lucene-only indexed field.
/// </summary>
public interface IFieldConfiguration
{
    /// <summary>Index with full-text analysis (tokenized, searchable by terms).</summary>
    IFieldConfiguration Analyzed();
    /// <summary>Index as-is for exact match filtering only.</summary>
    IFieldConfiguration NotAnalyzed();
    /// <summary>Enable vector embeddings for similarity search.</summary>
    IFieldConfiguration Vector();
}
