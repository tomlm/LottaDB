using Azure.Data.Tables;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;

namespace Lotta;

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
    {
        CreateTableServiceClient = name => new TableServiceClient(connectionString);
        CreateLuceneDirectory = name => new AzureDirectory(connectionString, name, new RAMDirectory());
    }

    public Func<string, TableServiceClient> CreateTableServiceClient { get; set; }

    public Func<string, Lucene.Net.Store.Directory> CreateLuceneDirectory { get; set; }

    public ILottaConfiguration Store<T>(Action<IStorageConfiguration<T>>? configure = null) where T : class, new()
    {
        var config = new StorageConfiguration<T>();
        configure?.Invoke(config);
        StorageConfigurations[typeof(T)] = config;
        return this;
    }

    public ILottaConfiguration On<T>(Func<T, TriggerKind, LottaDB, Task> handler) where T : class, new()
    {
        OnRegistrations.Add(new OnRegistration(typeof(T), handler));
        return this;
    }
}

internal record OnRegistration(Type ObjectType, object Handler);
