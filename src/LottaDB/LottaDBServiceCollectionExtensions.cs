using Microsoft.Extensions.DependencyInjection;

namespace LottaDB;

/// <summary>
/// Extension methods for registering LottaDB with dependency injection.
/// </summary>
public static class LottaDBServiceCollectionExtensions
{
    /// <summary>
    /// Register LottaDB services. Configure storage, types, views, builders, and observers.
    /// </summary>
    public static IServiceCollection AddLottaDB(this IServiceCollection services, Action<ILottaDBOptions> configure)
    {
        var options = new LottaDBOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<ILottaDB>(sp => new LottaDBInstance(options));
        return services;
    }
}
