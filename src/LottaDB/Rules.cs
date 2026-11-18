using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Lotta;

/// <summary>
/// The work a rule does for one object: given the object, the database (to query, save or delete
/// anything), and a token to stop on.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
/// <param name="item">The object that was saved, or deleted.</param>
/// <param name="db">The database, for querying or saving related objects.</param>
/// <param name="cancellationToken">
/// For an inline action, the save's token. For a queued action, cancelled when the run outlasts
/// its <see cref="QueuedRuleBuilder{T}.Timeout"/> (a failure), or when the database is disposed or
/// the <see cref="LottaDB.DrainAsync"/> running it is cancelled (not a failure: the work stays queued).
/// </param>
public delegate Task RuleHandler<in T>(T item, LottaDB db, CancellationToken cancellationToken);

/// <summary>
/// A database's rules, set in its configuration (<see cref="ILottaConfiguration.Rules"/>): what to
/// do when objects are saved or deleted.
/// <code>
/// config.Rules.When&lt;Note&gt;(n =&gt; n.IsPublished).Do(UpdateFeedView).Queue("notify-followers", Notify);
/// config.Rules.When&lt;Note&gt;(n =&gt; n.SummarizedContent != n.Content).Queue(Summarize).Priority(10);
/// config.Rules.WhenDeleted&lt;Note&gt;().Do(RemoveFeedView);
/// </code>
/// </summary>
public sealed class RuleSet
{
    private readonly LottaConfiguration _config;

    internal RuleSet(LottaConfiguration config) => _config = config;

    /// <summary>
    /// Start a rule for saves of <typeparamref name="T"/> (and of types derived from it). Give it
    /// actions: <c>.Do(handler)</c> runs inside the save, <c>.Queue(name, handler)</c> runs later
    /// from a queue. Several actions may follow one <c>When</c>, and several rules may be made for
    /// one type; they run in the order they were added. A rule with no action makes opening the
    /// database throw <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">
    /// Which saves the rule is for; null for all. It runs inside the save, so it must be quick and
    /// look only at the object. For <c>.Do</c> it is checked before the handler. For <c>.Queue</c>
    /// it is checked when the object is saved (false: nothing is queued) and again on the object as
    /// it is when the run comes up (false: the run is skipped, and not counted). If it throws on a
    /// save, the error is in the save's <see cref="ObjectResult.Errors"/> and the save still
    /// happens; if it throws when a queued run comes up, that counts as a failed run. It is an
    /// expression, compiled once here. <see cref="LottaDB.RequeueAsync"/> asks the search index
    /// for the objects it is true for when the index can express it (<c>[Queryable]</c>
    /// properties compared with constants), and otherwise reads every object; for a condition the
    /// index cannot express, such as comparing two fields, consider storing a flag and filtering on that.
    /// </param>
    public RuleBuilder<T> When<T>(Expression<Func<T, bool>>? filter = null) where T : class, new()
    {
        _config.ThrowIfOpened();
        var rule = new RuleRegistration(typeof(T), deleted: false);
        _config.RuleRegistrations.Add(rule);
        return new RuleBuilder<T>(_config, rule, filter);
    }

    /// <summary>
    /// Start a rule for deletes of <typeparamref name="T"/> (and of types derived from it). Give it
    /// an action with <c>.Do(handler)</c>, which runs inside the delete and is given the object as
    /// it was. A rule with no action makes opening the database throw.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">Which deletes the rule is for, looking at the deleted object; null for all.</param>
    public DeletedRuleBuilder<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null) where T : class, new()
    {
        _config.ThrowIfOpened();
        var rule = new RuleRegistration(typeof(T), deleted: true);
        _config.RuleRegistrations.Add(rule);
        return new DeletedRuleBuilder<T>(_config, rule, filter?.Compile());
    }
}

/// <summary>
/// Rules added to an open database (<see cref="LottaDB.Rules"/>). They are inline only, and each
/// is removed when the handle its <c>.Do</c> returns is disposed.
/// <code>
/// using var rule = db.Rules.When&lt;Note&gt;().Do(Refresh);
/// </code>
/// </summary>
public sealed class LiveRuleSet
{
    private readonly LottaDB _db;

    internal LiveRuleSet(LottaDB db) => _db = db;

    /// <summary>Start an inline rule for saves of <typeparamref name="T"/> (and of types derived from it).</summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">Which saves the rule is for; null for all. See <see cref="RuleSet.When{T}"/>.</param>
    public LiveRule<T> When<T>(Expression<Func<T, bool>>? filter = null) where T : class, new() => new(_db, filter?.Compile());

    /// <summary>Start an inline rule for deletes of <typeparamref name="T"/> (and of types derived from it).</summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">Which deletes the rule is for; null for all.</param>
    public LiveDeletedRule<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null) where T : class, new() => new(_db, filter?.Compile());
}

/// <summary>
/// A rule for saves of <typeparamref name="T"/> (and of types derived from it), made by
/// <see cref="RuleSet.When{T}"/>. Give it one or more actions: <see cref="Do"/> to act
/// inside the save, <see cref="Queue(string, RuleHandler{T})"/> to act later from a queue. A rule
/// with no action makes opening the database throw. Rules must be complete before the database
/// opens; adding to or changing one afterwards throws.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class RuleBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly RuleRegistration _rule;
    private readonly Expression<Func<T, bool>>? _filterExpression;
    private readonly Func<T, bool>? _filter;

    internal RuleBuilder(LottaConfiguration config, RuleRegistration rule, Expression<Func<T, bool>>? filter)
    {
        _config = config;
        _rule = rule;
        _filterExpression = filter;
        // Compiled once, here: every check, on save, before a run and in .Do, uses the delegate.
        _filter = filter?.Compile();
    }

    /// <summary>
    /// Act inside every save the rule's filter is true for, after the object is written and
    /// indexed. The handler has full database access; what it saves or deletes is part of the
    /// save's <see cref="ObjectResult.Changes"/>, and what it throws is in the save's
    /// <see cref="ObjectResult.Errors"/> (the save itself still happens). Actions run in the order
    /// they were added.
    /// </summary>
    /// <param name="handler">The work to do.</param>
    public RuleBuilder<T> Do(RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _config.ThrowIfOpened();
        _config.OnRegistrations.Add(new OnRegistration(typeof(T), RuleActions.Inline(TriggerKind.Saved, _filter, handler)));
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Act later, from a queue, outside the save that caused it. Every save the rule's filter is
    /// true for queues the work; the handler is then given the object as it is when it runs (the
    /// filter is checked again then, and the run skipped if it is false by then), and is retried
    /// until it succeeds or has failed too often (<see cref="QueuedRuleBuilder{T}.Retry"/>). Use it
    /// for work too slow to do inside a save. It may run more than once for one save, so it must be
    /// safe to repeat. Follow it with <see cref="QueuedRuleBuilder{T}.Priority"/>,
    /// <see cref="QueuedRuleBuilder{T}.EverySave"/>, <see cref="QueuedRuleBuilder{T}.Retry"/> or
    /// <see cref="QueuedRuleBuilder{T}.Timeout"/> to schedule it.
    /// </summary>
    /// <param name="name">
    /// Identifies the work across runs of the program; its queue is named after it. 3 to 40
    /// lower-case letters and digits, with single hyphens between them, unique in the configuration.
    /// </param>
    /// <param name="handler">The work to do.</param>
    /// <exception cref="ArgumentException">The name is not a valid queue name, or is already used.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public QueuedRuleBuilder<T> Queue(string name, RuleHandler<T> handler)
    {
        var registration = _config.AddQueuedRule(name, _filterExpression, _filter, handler);
        _rule.Actions++;
        return new QueuedRuleBuilder<T>(this, _config, registration);
    }

    /// <summary>
    /// Act later, from a queue, as <see cref="Queue(string, RuleHandler{T})"/>, with the queue
    /// named after the handler method: its name without an <c>Async</c> suffix, in kebab case, so
    /// <c>Summarize</c> gives <c>summarize</c> and <c>handlers.NotifyFollowersAsync</c> gives
    /// <c>notify-followers</c>. Renaming the method renames the queue; work still in the old queue
    /// is then reported as orphaned by <see cref="LottaDB.GetQueueStatusAsync"/> and can be moved
    /// with <see cref="LottaDB.MoveQueuedAsync"/>.
    /// </summary>
    /// <param name="handler">A method: <c>Summarize</c> or <c>handlers.Summarize</c>. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="handler"/> is not a method name (a lambda, or a call), or the name made from
    /// it is not a valid queue name or is already used.
    /// </exception>
    public QueuedRuleBuilder<T> Queue(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => Queue(Internal.AsyncQueueNames.HandlerNameFromExpression(handlerExpression), handler);
}

/// <summary>
/// A rule whose most recent action is queued, returned by <c>.Queue(...)</c>. The scheduling
/// modifiers (<see cref="Priority"/>, <see cref="EverySave"/>, <see cref="Retry"/>,
/// <see cref="Timeout"/>) apply to that queued action only. <see cref="Do"/> and <c>Queue</c> add
/// further actions to the same rule.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class QueuedRuleBuilder<T> where T : class, new()
{
    private readonly RuleBuilder<T> _rule;
    private readonly LottaConfiguration _config;
    private readonly AsyncRegistration _registration;

    internal QueuedRuleBuilder(RuleBuilder<T> rule, LottaConfiguration config, AsyncRegistration registration)
    {
        _rule = rule;
        _config = config;
        _registration = registration;
    }

    /// <summary>Higher runs first; within one priority, work runs first in, first out. Default: 0.</summary>
    /// <param name="priority">The priority.</param>
    public QueuedRuleBuilder<T> Priority(int priority)
    {
        _config.ThrowIfOpened();
        _registration.Options.Priority = priority;
        return this;
    }

    /// <summary>
    /// Run once for every save. By default only an object's latest state matters: several saves of
    /// one object before the work gets to it give one run, on the object as it then is.
    /// </summary>
    public QueuedRuleBuilder<T> EverySave()
    {
        _config.ThrowIfOpened();
        _registration.Options.Latest = false;
        return this;
    }

    /// <summary>
    /// How a failing run is retried. Defaults: 5 attempts, 2 seconds.
    /// </summary>
    /// <param name="maxAttempts">How many times a failing run is tried before its work is set aside in the poison queue; at least 1.</param>
    /// <param name="delay">
    /// How long to wait after the first failure before trying again; zero or more. The wait doubles
    /// with each further failure, up to five minutes. A run that finds the write lock held by
    /// another process waits the same way, counted separately and not as a failure.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">A value is out of range.</exception>
    public QueuedRuleBuilder<T> Retry(int maxAttempts, TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        _config.ThrowIfOpened();
        _registration.Options.MaxAttempts = maxAttempts;
        _registration.Options.RetryDelay = delay;
        return this;
    }

    /// <summary>How long one run may take before it is cancelled and counted as a failure. Default: 5 minutes.</summary>
    /// <param name="timeout">The time allowed; more than zero.</param>
    /// <exception cref="ArgumentOutOfRangeException">The time is not positive.</exception>
    public QueuedRuleBuilder<T> Timeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        _config.ThrowIfOpened();
        _registration.Options.Timeout = timeout;
        return this;
    }

    /// <summary>Add an inline action to the same rule; see <see cref="RuleBuilder{T}.Do"/>.</summary>
    /// <param name="handler">The work to do.</param>
    public RuleBuilder<T> Do(RuleHandler<T> handler) => _rule.Do(handler);

    /// <summary>Add another queued action to the same rule; see <see cref="RuleBuilder{T}.Queue(string, RuleHandler{T})"/>.</summary>
    /// <param name="name">The new queue's name.</param>
    /// <param name="handler">The work to do.</param>
    public QueuedRuleBuilder<T> Queue(string name, RuleHandler<T> handler) => _rule.Queue(name, handler);

    /// <summary>Add another queued action to the same rule, named after its handler; see <see cref="RuleBuilder{T}.Queue(RuleHandler{T}, string?)"/>.</summary>
    /// <param name="handler">A method.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    public QueuedRuleBuilder<T> Queue(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => _rule.Queue(Internal.AsyncQueueNames.HandlerNameFromExpression(handlerExpression), handler);
}

/// <summary>
/// A rule for deletes of <typeparamref name="T"/> (and of types derived from it), made by
/// <see cref="RuleSet.WhenDeleted{T}"/>. Give it an action with <see cref="Do"/>; a
/// rule with no action makes opening the database throw.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class DeletedRuleBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly RuleRegistration _rule;
    private readonly Func<T, bool>? _filter;

    internal DeletedRuleBuilder(LottaConfiguration config, RuleRegistration rule, Func<T, bool>? filter)
    {
        _config = config;
        _rule = rule;
        _filter = filter;
    }

    /// <summary>
    /// Act inside every delete the rule's filter is true for, after the object is removed. The
    /// handler is given the object as it was. What it saves or deletes is part of the delete's
    /// <see cref="ObjectResult.Changes"/>, and what it throws is in its <see cref="ObjectResult.Errors"/>.
    /// </summary>
    /// <param name="handler">The work to do.</param>
    public DeletedRuleBuilder<T> Do(RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _config.ThrowIfOpened();
        _config.OnRegistrations.Add(new OnRegistration(typeof(T), RuleActions.Inline(TriggerKind.Deleted, _filter, handler)));
        _rule.Actions++;
        return this;
    }
}

/// <summary>
/// A rule for saves of <typeparamref name="T"/> added to an open database by
/// <see cref="LiveRuleSet.When{T}"/>. Its action runs inline, and stops when the handle
/// <see cref="Do"/> returns is disposed.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class LiveRule<T> where T : class, new()
{
    private readonly LottaDB _db;
    private readonly Func<T, bool>? _filter;

    internal LiveRule(LottaDB db, Func<T, bool>? filter)
    {
        _db = db;
        _filter = filter;
    }

    /// <summary>Act inside every save the filter is true for, as <see cref="RuleBuilder{T}.Do"/> does, until the handle is disposed.</summary>
    /// <param name="handler">The work to do.</param>
    /// <returns>A handle; dispose it to remove the rule.</returns>
    public IDisposable Do(RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return _db.AddHandler(RuleActions.Inline(TriggerKind.Saved, _filter, handler));
    }
}

/// <summary>
/// A rule for deletes of <typeparamref name="T"/> added to an open database by
/// <see cref="LiveRuleSet.WhenDeleted{T}"/>. Its action runs inline, and stops when the handle
/// <see cref="Do"/> returns is disposed.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class LiveDeletedRule<T> where T : class, new()
{
    private readonly LottaDB _db;
    private readonly Func<T, bool>? _filter;

    internal LiveDeletedRule(LottaDB db, Func<T, bool>? filter)
    {
        _db = db;
        _filter = filter;
    }

    /// <summary>Act inside every delete the filter is true for, as <see cref="DeletedRuleBuilder{T}.Do"/> does, until the handle is disposed.</summary>
    /// <param name="handler">The work to do.</param>
    /// <returns>A handle; dispose it to remove the rule.</returns>
    public IDisposable Do(RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return _db.AddHandler(RuleActions.Inline(TriggerKind.Deleted, _filter, handler));
    }
}

/// <summary>When queued work runs.</summary>
public enum QueueMode
{
    /// <summary>By itself, in the background, for as long as the database is open.</summary>
    Auto,

    /// <summary>Only when <see cref="LottaDB.DrainAsync"/> is called.</summary>
    Manual,
}

/// <summary>Where one queue stands.</summary>
/// <param name="Name">The queue's name, as given to <c>.Queue</c>.</param>
/// <param name="Priority">Its priority; higher runs first.</param>
/// <param name="Latest">Whether it runs once for an object's latest state instead of once per save.</param>
/// <param name="Waiting">Messages in the queue, including any being worked on or waiting to be retried. Approximate.</param>
/// <param name="Running">Runs in progress in this process.</param>
/// <param name="Poisoned">Messages set aside after failing too often. Approximate.</param>
/// <param name="LastError">The message of the most recent failure in this process; null after a success.</param>
/// <param name="Paused">Whether it is paused in this process.</param>
/// <param name="Orphaned">
/// True for a queue of this database that no rule in the configuration uses, left by a rule that
/// was renamed or removed. Only <paramref name="Name"/>, <paramref name="Waiting"/> and
/// <paramref name="Poisoned"/> mean anything for it. Move its work with
/// <see cref="LottaDB.MoveQueuedAsync"/>, or delete it with <see cref="LottaDB.DeleteQueueAsync"/>.
/// </param>
public sealed record QueueStatus(
    string Name, int Priority, bool Latest, int Waiting, int Running, int Poisoned, string? LastError, bool Paused, bool Orphaned = false);

/// <summary>How one queued action is scheduled and retried; set by the <see cref="QueuedRuleBuilder{T}"/> modifiers.</summary>
internal sealed class QueueOptions
{
    public int Priority { get; set; } = 0;
    public bool Latest { get; set; } = true;
    public int MaxAttempts { get; set; } = 5;
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>A rule made by <c>When</c> or <c>WhenDeleted</c>, counted so a rule with no action is caught at open.</summary>
internal sealed class RuleRegistration(Type objectType, bool deleted)
{
    public Type ObjectType { get; } = objectType;
    public bool Deleted { get; } = deleted;
    public int Actions { get; set; }
}

internal static class RuleActions
{
    /// <summary>
    /// An inline action as the handler engine calls it: run for one trigger kind, and only when
    /// the filter is true. A filter that throws throws inside the engine's call, so its error is
    /// reported in the save's errors like a handler's.
    /// </summary>
    public static EntityHandler<T> Inline<T>(TriggerKind trigger, Func<T, bool>? filter, RuleHandler<T> handler) =>
        (entity, kind, db, cancellationToken) =>
            kind == trigger && (filter?.Invoke(entity) ?? true) ? handler(entity, db, cancellationToken) : Task.CompletedTask;
}
