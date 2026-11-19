namespace Lotta;

/// <summary>
/// Whether an inline handler is running for a save or a delete. Internal: rules choose their
/// trigger with <c>When</c> or <c>WhenDeleted</c>; the built-in index handlers take both.
/// </summary>
internal enum TriggerKind
{
    /// <summary>The trigger object was saved (created or updated).</summary>
    Saved,
    /// <summary>The trigger object was deleted.</summary>
    Deleted
}

/// <summary>
/// An inline handler as the handler engine stores and calls it, for saves and deletes alike.
/// <see cref="RuleActions.Inline"/> adapts a <see cref="RuleHandler{T}"/> to it.
/// </summary>
internal delegate Task EntityHandler<in T>(T entity, TriggerKind kind, LottaDB db, CancellationToken cancellationToken);
