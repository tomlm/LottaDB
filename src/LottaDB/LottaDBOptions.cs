using System.Linq.Expressions;

namespace LottaDB;

public class LottaDBOptions : ILottaDBOptions
{
    internal string? ConnectionString { get; private set; }
    internal bool UseInMemory { get; private set; }
    internal IDirectoryProvider? DirectoryProvider { get; private set; }
    internal Dictionary<Type, object> StoreConfigurations { get; } = new();
    internal List<ViewRegistration> ViewRegistrations { get; } = new();
    internal List<BuilderRegistration> BuilderRegistrations { get; } = new();
    internal List<ObserverRegistration> ObserverRegistrations { get; } = new();

    public ILottaDBOptions UseAzureTables(string connectionString)
    {
        ConnectionString = connectionString;
        UseInMemory = false;
        return this;
    }

    public ILottaDBOptions UseInMemoryTables()
    {
        UseInMemory = true;
        return this;
    }

    public ILottaDBOptions UseLuceneDirectory(IDirectoryProvider provider)
    {
        DirectoryProvider = provider;
        return this;
    }

    public ILottaDBOptions Store<T>(Action<IStoreConfiguration<T>>? configure = null) where T : class
    {
        var config = new StoreConfiguration<T>();
        configure?.Invoke(config);
        StoreConfigurations[typeof(T)] = config;
        return this;
    }

    public ILottaDBOptions CreateView<TView>(Expression<Func<ILottaDB, IQueryable<TView>>> viewExpression) where TView : class
    {
        ViewRegistrations.Add(new ViewRegistration(typeof(TView), viewExpression));
        return this;
    }

    public ILottaDBOptions AddBuilder<TTrigger, TDerived, TBuilder>()
        where TTrigger : class
        where TDerived : class
        where TBuilder : class, IBuilder<TTrigger, TDerived>
    {
        BuilderRegistrations.Add(new BuilderRegistration(typeof(TTrigger), typeof(TDerived), typeof(TBuilder)));
        return this;
    }

    public ILottaDBOptions Observe<T>(Func<ObjectChange<T>, Task> handler) where T : class
    {
        ObserverRegistrations.Add(new ObserverRegistration(typeof(T), handler));
        return this;
    }
}

internal record ViewRegistration(Type ViewType, object Expression);
internal record BuilderRegistration(Type TriggerType, Type DerivedType, Type BuilderType);
internal record ObserverRegistration(Type ObjectType, object Handler);
