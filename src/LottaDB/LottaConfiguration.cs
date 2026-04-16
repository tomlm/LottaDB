using Azure.Data.Tables;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.En;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;

namespace Lotta;

/// <summary>
/// Configuration for a Lotta DB.
/// </summary>
public class LottaConfiguration : ILottaConfiguration
{
    internal Dictionary<Type, object> StorageConfigurations { get; } = new();
    internal List<OnRegistration> OnRegistrations { get; } = new();

    public LottaConfiguration()
    {
        CreateTableServiceClient = name => throw new InvalidOperationException("LottaConfiguration.CreateTableServiceClient is not configured.");
        CreateLuceneDirectory = name => throw new InvalidOperationException("LottaConfiguration.CreateLuceneDirectory is not configured.");
    }

    public LottaConfiguration(string connectionString)
        : this()
    {
        if (!String.IsNullOrEmpty(connectionString))
        {
            CreateTableServiceClient = name => new TableServiceClient(connectionString);
            CreateLuceneDirectory = name => new AzureDirectory(connectionString, name, new RAMDirectory());
        }
    }

    /// <summary>
    /// Factory for creating tableservice client. Default if there is a connectionstring is Azure TableServiceClient
    /// otherwise throws an exception. You can override this to provide your own implementation of TableServiceClient
    /// </summary>
    public Func<string, TableServiceClient> CreateTableServiceClient { get; set; }

    /// <summary>
    /// Factory for creating tableservice client. Default if there is a connectionstring is AzureDirectory
    /// otherwise throws an exception. You can override this to provide your own implementation of Directory
    /// </summary>
    public Func<string, Lucene.Net.Store.Directory> CreateLuceneDirectory { get; set; }

    /// <summary>
    /// Default Analyzer to use for indexing/querying
    /// </summary>
    public Analyzer Analyzer{ get; set; } = new EnglishAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48);

    /// <summary>
    /// Defines a storage configuration for a specific type. This is where you can specify how a type should be stored in the database, 
    /// including table name, partition key, row key, etc. If not configured, Lotta will use default conventions to determine these values.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="configure"></param>
    /// <returns></returns>
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
    /// <remarks>The handler will be called for each trigger event associated with the specified entity type.
    /// Multiple handlers can be registered for different entity types or trigger kinds.</remarks>
    /// <typeparam name="T">The type of entity for which the handler will be registered. Must be a reference type with a parameterless
    /// constructor.</typeparam>
    /// <param name="handler">A function to execute when a trigger occurs. The function receives the entity instance, the kind of trigger, and
    /// the database context, and returns a task that represents the asynchronous operation.</param>
    /// <returns>The current configuration instance, enabling method chaining.</returns>
    public ILottaConfiguration On<T>(Func<T, TriggerKind, LottaDB, Task> handler) where T : class, new()
    {
        OnRegistrations.Add(new OnRegistration(typeof(T), handler));
        return this;
    }
}

internal record OnRegistration(Type ObjectType, object Handler);
