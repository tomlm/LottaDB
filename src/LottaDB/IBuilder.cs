namespace LottaDB;

/// <summary>
/// A builder that produces derived objects when a trigger object is saved or deleted.
/// Implement this for custom logic that can't be expressed as a <c>CreateView</c> LINQ join.
/// </summary>
/// <typeparam name="TTrigger">The type of object that triggers this builder.</typeparam>
/// <typeparam name="TDerived">The type of derived object this builder produces.</typeparam>
public interface IBuilder<TTrigger, TDerived>
{
    /// <summary>
    /// Called when a <typeparamref name="TTrigger"/> object is saved or deleted.
    /// Yield <see cref="BuildResult{T}"/> items to save or delete derived objects.
    /// The builder has full access to <see cref="ILottaDB"/> for reading related objects.
    /// </summary>
    IAsyncEnumerable<BuildResult<TDerived>> BuildAsync(
        TTrigger entity,
        TriggerKind trigger,
        ILottaDB db,
        CancellationToken ct);
}
