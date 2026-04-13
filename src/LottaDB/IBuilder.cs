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
    /// <param name="entity">The trigger object that was saved or deleted.</param>
    /// <param name="trigger">Whether the trigger object was saved or deleted.</param>
    /// <param name="db">The LottaDB instance for reading related objects.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Zero or more <see cref="BuildResult{T}"/> items to save or delete derived objects.</returns>
    IAsyncEnumerable<BuildResult<TDerived>> BuildAsync(
        TTrigger entity,
        TriggerKind trigger,
        ILottaDB db,
        CancellationToken ct);
}
