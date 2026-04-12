using Microsoft.Extensions.DependencyInjection;

namespace LottaDB;

public static class LottaDBServiceCollectionExtensions
{
    public static IServiceCollection AddLottaDB(this IServiceCollection services, Action<ILottaDBOptions> configure)
    {
        var options = new LottaDBOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<ILottaDB>(sp => new LottaDBInstance(options));
        return services;
    }
}
