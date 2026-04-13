namespace Lotta;

public class LottaConfiguration : ILottaConfiguration
{
    internal Dictionary<Type, object> StoreConfigurations { get; } = new();
    internal List<OnRegistration> OnRegistrations { get; } = new();

    public ILottaConfiguration Store<T>(Action<IStoreConfiguration<T>>? configure = null) where T : class, new()
    {
        var config = new StoreConfiguration<T>();
        configure?.Invoke(config);
        StoreConfigurations[typeof(T)] = config;
        return this;
    }

    public ILottaConfiguration On<T>(Func<T, TriggerKind, LottaDB, Task> handler) where T : class, new()
    {
        OnRegistrations.Add(new OnRegistration(typeof(T), handler));
        return this;
    }
}

internal record OnRegistration(Type ObjectType, object Handler);
