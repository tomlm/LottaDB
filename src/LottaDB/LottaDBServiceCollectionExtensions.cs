using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace LottaDB;

/// <summary>
/// Optional DI convenience extension. LottaDB can also be constructed directly.
/// </summary>
public static class LottaDBServiceCollectionExtensions
{
    /// <summary>
    /// Register a LottaDB database instance as a singleton.
    /// </summary>
    public static IServiceCollection AddLottaDB(this IServiceCollection services,
        string name, TableServiceClient tableServiceClient, LuceneDirectory directory,
        Action<ILottaDBOptions> configure)
    {
        var options = new LottaDBOptions();
        configure(options);
        var instance = new LottaDB(name, tableServiceClient, directory, options);
        services.AddSingleton(instance);
        return services;
    }
}
