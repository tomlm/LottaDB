using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;

namespace LottaDB;

/// <summary>
/// Extension methods for registering LottaDB with dependency injection.
/// Requires a <see cref="TableServiceClient"/> to be registered in DI.
/// </summary>
public static class LottaDBServiceCollectionExtensions
{
    /// <summary>
    /// Register LottaDB services. Configure types, views, builders, and observers.
    /// A <see cref="TableServiceClient"/> must be registered in DI before calling this.
    /// </summary>
    public static IServiceCollection AddLottaDB(this IServiceCollection services, Action<ILottaDBOptions> configure)
    {
        var options = new LottaDBOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<ILottaDB>(sp =>
        {
            var tableServiceClient = sp.GetRequiredService<TableServiceClient>();
            var sink = sp.GetService<IBuilderFailureSink>();
            return new LottaDBInstance(options, tableServiceClient, sink);
        });
        return services;
    }
}
