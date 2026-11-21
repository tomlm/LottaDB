using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Lotta.Internal;

namespace Lotta;

/// <summary>
/// The work a rule does for one object. Every verb (<c>Do</c>, <c>DoQueued</c>, <c>Modify</c>, <c>ModifyQueued</c>) takes
/// this shape.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
/// <param name="item">
/// The object that was saved, or deleted. For <c>Modify</c>, the object about to be written; for <c>ModifyQueued</c>, a copy to edit. It carries its key and
/// ETag like any object LottaDB hands out (<c>item.GetKey()</c>, <c>item.GetETag()</c>).
/// </param>
/// <param name="context">The database, the rule's name, which attempt this is, and a key for dropping duplicates.</param>
/// <param name="cancellationToken">
/// For <c>Do</c>, the save's token. For a queued rule, cancelled when the run outlasts
/// <see cref="ILottaConfiguration.RuleTimeout"/> (a failure), or when the database is disposed or
/// the <see cref="LottaDB.DrainAsync"/> running it is cancelled (not a failure: the work stays queued).
/// </param>
public delegate Task RuleHandler<in T>(T item, RuleContext context, CancellationToken cancellationToken);

/// <summary>What a rule's handler is told about the run it is part of.</summary>
public sealed class RuleContext
{
    private readonly object? _item;
    private readonly string _ruleId;
    private readonly string? _etag;
    private string? _idempotencyKey;

    internal RuleContext(LottaDB db, string rule, string ruleId, int attempt, object? item, string? etag)
    {
        Db = db;
        Rule = rule;
        Attempt = attempt;
        _ruleId = ruleId;
        _item = item;
        _etag = etag;
    }

    /// <summary>The database the rule runs on, to query, save or delete anything.</summary>
    public LottaDB Db { get; }

    /// <summary>The rule's name, such as <c>summarize</c>; empty for an in-save <c>Do</c> or <c>Modify</c>, which have none.</summary>
    public string Rule { get; }

    /// <summary>1 on the first run, 2 on the first retry, and so on. Always 1 for an in-save <c>Do</c> or <c>Modify</c>.</summary>
    public int Attempt { get; }

    /// <summary>
    /// A key made from the object's type, key and ETag at the save that caused the run, and the
    /// rule's name. For a delete of an object that had no ETag, an id made in the delete takes the
    /// ETag's place. It is the same on every retry of that work, so an outside service given it can
    /// drop duplicates.
    /// </summary>
    public string IdempotencyKey => _idempotencyKey ??= MakeIdempotencyKey();

    private string MakeIdempotencyKey()
    {
        var text = $"{_item?.GetType().FullName}\n{_item?.GetKey()}\n{_etag ?? _item?.GetETag()}\n{_ruleId}";
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)), 0, 16).ToLowerInvariant();
    }
}

/// <summary>
/// What a rule's filter can ask besides the object's values. Bring it into scope with
/// <c>using static Lotta.Rule;</c>.
/// </summary>
public static class Rule
{
    /// <summary>
    /// In a rule's filter: true when any of the values differs, in this save, from the version the
    /// save replaces. It makes a rule react to a change rather than to a state, and tells an
    /// <c>ModifyQueued</c> rule what its inputs are.
    /// <code>
    /// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.IsPublished) &amp;&amp; n.IsPublished).DoQueued(NotifyFollowers);
    /// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.Content)).ModifyQueued(Summarize);
    /// </code>
    /// A new object counts as changed. An object saved without having been loaded is compared with
    /// the stored version, which is read first (only for types that have <c>Changed</c> rules).
    /// Later checks of the filter, when a queued run comes up or in a backfill, take
    /// <c>Changed</c> to be true and check the rest.
    /// </summary>
    /// <param name="values">Values of the object the rule is for: <c>Changed(m.Subject, m.Body)</c>.</param>
    /// <exception cref="InvalidOperationException">Always, when called: it is a marker that only means something inside a rule's filter.</exception>
    public static bool Changed(params object?[] values) =>
        throw new InvalidOperationException(
            "Changed(...) only means something inside a rule's filter: Rules.When<Note>(n => Changed(n.Content)). It cannot be called.");

    /// <summary>
    /// In a rule's filter: true when a vector field of the object has text with no vector for it
    /// yet: the object is new, its text changed, it was saved before vectors were kept on the row,
    /// or the embedding model changed. A save works this out; where it has not been worked out
    /// (a queued run coming up, a backfill), it is true, and <see cref="EmbedVectors"/> finds out.
    /// <para>
    /// LottaDB registers, for each type with vector fields, the rule
    /// <c>When&lt;T&gt;(x =&gt; VectorsStale(x)).ModifyQueued("embed", EmbedVectors)</c>, unless the
    /// configuration already has a rule named <c>embed</c> for the type. Register your own under that
    /// name to narrow it: <c>When&lt;Note&gt;(n =&gt; n.IsPublished &amp;&amp; VectorsStale(n)).ModifyQueued("embed", EmbedVectors)</c>.
    /// </para>
    /// </summary>
    /// <param name="item">The object the rule is for.</param>
    public static bool VectorsStale(object item) => item.GetVectorsStale() ?? true;

    /// <summary>
    /// A <c>ModifyQueued</c> handler that embeds the vector fields of the object whose text has no
    /// vector, in one call to the catalog's embedding generator, and keeps the vectors on the
    /// object's row. The search index then gets them, and <c>Similar</c> finds the object.
    /// See <see cref="VectorsStale"/>.
    /// </summary>
    public static Task EmbedVectors<T>(T item, RuleContext context, CancellationToken cancellationToken) where T : class
        => context.Db.EmbedVectorsAsync(item, cancellationToken);
}

/// <summary>
/// A database's rules, set in its configuration (<see cref="ILottaConfiguration.Rules"/>): what to
/// do when objects are saved or deleted.
/// <code>
/// config.Rules.When&lt;Note&gt;(n =&gt; n.IsPublished).Do(UpdateFeedView);
/// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.Content)).ModifyQueued(Summarize);
/// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.IsPublished) &amp;&amp; n.IsPublished).DoQueued(NotifyFollowers);
/// config.Rules.WhenDeleted&lt;Note&gt;().Do(RemoveFeedView);
/// </code>
/// </summary>
public sealed class RuleSet
{
    private readonly LottaConfiguration _config;

    internal RuleSet(LottaConfiguration config) => _config = config;

    /// <summary>
    /// Start a rule for saves of <typeparamref name="T"/> (and of types derived from it). Give it
    /// actions: <c>.Do(handler)</c> runs a task inside the save and <c>.DoQueued(handler)</c>
    /// runs one later, from a queue; <c>.Modify(handler)</c> changes the object as part of the
    /// save and <c>.ModifyQueued(handler)</c> changes it later, from a queue;
    /// <c>.Materialize(projection)</c> keeps other objects in step with it; <c>.After(delay)</c>
    /// and <c>.At(time)</c> start actions that run at a later time. Several actions may follow one <c>When</c>, and several rules may be made for
    /// one type. A rule with no action makes opening the database throw
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">
    /// Which saves the rule is for; null for all. It runs inside the save, so it must be quick and
    /// look only at the object. Use <see cref="Rule.Changed"/> in it to react to a change rather
    /// than a state. For <c>.Do</c> it is checked before the handler. For a queued action it is
    /// checked when the object is saved (false: nothing is queued) and again, without its
    /// <c>Changed</c> parts, on the object as it is when the run comes up (false: the rule is
    /// skipped, and not counted as a run). For <c>.Materialize</c> it says which objects have a
    /// projection: false deletes what the object produced. If it throws on a save, the error is in the save's
    /// <see cref="ObjectResult.Errors"/> and the save still happens; if it throws when a queued run
    /// comes up, that counts as a failed run. It is an expression, taken apart and compiled once
    /// here. <see cref="LiveRuleSet.BackfillAsync"/> asks the search index for the objects it is
    /// true for when the index can express it (<c>[Queryable]</c> properties compared with
    /// constants), and otherwise reads every object.
    /// </param>
    /// <exception cref="ArgumentException">The filter uses <c>Changed</c> in a way that cannot be evaluated.</exception>
    public RuleBuilder<T> When<T>(Expression<Func<T, bool>>? filter = null) where T : class, new()
    {
        _config.ThrowIfOpened();
        var parsed = RuleFilter.Parse(filter);
        var rule = new RuleRegistration(typeof(T), deleted: false);
        _config.RuleRegistrations.Add(rule);
        if (parsed.UsesChanged)
            _config.ChangedTypes.Add(typeof(T));
        return new RuleBuilder<T>(_config, rule, parsed);
    }

    /// <summary>
    /// Start a schedule: work done every so often, for no object.
    /// <code>
    /// config.Rules.Every(TimeSpan.FromHours(24)).Do(SyncWithGraph);
    /// </code>
    /// The first run is one interval after the schedule is first seen, and each later one is one
    /// interval after the run before it was started.
    /// </summary>
    /// <param name="interval">How long between runs.</param>
    /// <exception cref="ArgumentOutOfRangeException">The interval is zero or negative.</exception>
    public ScheduleBuilder Every(TimeSpan interval)
    {
        _config.ThrowIfOpened();
        return new ScheduleBuilder(_config, ScheduleTimes.Interval(interval));
    }

    /// <summary>
    /// Start a schedule given as a cron expression: work done at set times, for no object.
    /// <code>
    /// config.Rules.Every("0 2 * * *", TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time"))
    ///     .Do(NightlyCleanup);   // 02:00 local time, every day
    /// </code>
    /// </summary>
    /// <param name="cron">Five fields (minute, hour, day of month, month, day of week), or six with seconds first.</param>
    /// <param name="timeZone">The time zone the expression is in; UTC when null.</param>
    /// <exception cref="ArgumentException">The text is not a cron expression.</exception>
    public ScheduleBuilder Every(string cron, TimeZoneInfo? timeZone = null)
    {
        _config.ThrowIfOpened();
        return new ScheduleBuilder(_config, ScheduleTimes.Cron(cron, timeZone));
    }

    /// <summary>
    /// Start a rule for deletes of <typeparamref name="T"/> (and of types derived from it). Give it
    /// an action with <c>.Do(handler)</c>, which runs inside the delete, or <c>.DoQueued(handler)</c>,
    /// which runs later, from the queue. Both are given the object as it was. A rule with no action
    /// makes opening the database throw.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">Which deletes the rule is for, looking at the deleted object; null for all.</param>
    /// <exception cref="ArgumentException">The filter uses <c>Changed</c>, which a delete cannot answer.</exception>
    public DeletedRuleBuilder<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null) where T : class, new()
    {
        _config.ThrowIfOpened();
        var parsed = RuleActions.DeletedFilter(filter);
        var rule = new RuleRegistration(typeof(T), deleted: true);
        _config.RuleRegistrations.Add(rule);
        return new DeletedRuleBuilder<T>(_config, rule, parsed);
    }
}

/// <summary>
/// The rules of an open database (<see cref="LottaDB.Rules"/>): add inline rules to it, and see
/// and steer the queued rules of its configuration.
/// <code>
/// using var rule = db.Rules.When&lt;Note&gt;().Do(Refresh);
/// var status = await db.Rules.StatusAsync();
/// await db.Rules.RetryAsync("Note/summarize");
/// </code>
/// </summary>
public sealed class LiveRuleSet
{
    private readonly LottaDB _db;

    internal LiveRuleSet(LottaDB db) => _db = db;

    /// <summary>
    /// Start an inline rule for saves of <typeparamref name="T"/> (and of types derived from it).
    /// It is removed when the handle its <c>.Do</c> returns is disposed.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">Which saves the rule is for; null for all. See <see cref="RuleSet.When{T}"/>.</param>
    public LiveRule<T> When<T>(Expression<Func<T, bool>>? filter = null) where T : class, new() => new(_db, RuleFilter.Parse(filter));

    /// <summary>
    /// Start an inline rule for deletes of <typeparamref name="T"/> (and of types derived from it).
    /// It is removed when the handle its <c>.Do</c> returns is disposed.
    /// </summary>
    /// <typeparam name="T">The object type to react to.</typeparam>
    /// <param name="filter">Which deletes the rule is for; null for all.</param>
    public LiveDeletedRule<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null) where T : class, new() => new(_db, RuleActions.DeletedFilter(filter));

    /// <summary>
    /// Where the queued rules stand. Per rule: runs in progress, work set aside as failed, and the
    /// last error. Per type: how much is waiting. Waiting work is counted per type because one
    /// message lists every rule that a save matched. Work set aside for a rule that is no longer
    /// in the configuration is listed under that rule's name, marked orphaned.
    /// </summary>
    /// <param name="cancellationToken">Cancels the reading.</param>
    public Task<RulesStatus> StatusAsync(CancellationToken cancellationToken = default) => _db.RulesStatusAsync(cancellationToken);

    /// <summary>
    /// Puts a rule's work that was set aside as failed back in its queue, with a fresh count of
    /// attempts. Works for an orphaned rule too (one no longer in the configuration, as
    /// <see cref="StatusAsync"/> lists it): its work is then run by all of the type's current
    /// queued rules, which is how work set aside under an old name is retried after a rename.
    /// </summary>
    /// <param name="rule">The rule: <c>Note/summarize</c>, or just <c>summarize</c> when only one rule has that name.</param>
    /// <param name="cancellationToken">Stops the retry; what has been put back stays put back.</param>
    /// <returns>How many pieces of work were put back.</returns>
    /// <exception cref="ArgumentException">No rule has that name, or several do.</exception>
    public Task<int> RetryAsync(string rule, CancellationToken cancellationToken = default) => _db.RetryRuleAsync(rule, cancellationToken);

    /// <summary>
    /// Deletes a rule's work that was set aside as failed: for work that should not be tried
    /// again, and for an orphaned rule.
    /// </summary>
    /// <param name="rule">The rule: <c>Note/summarize</c>, or just <c>summarize</c> when only one rule has that name.</param>
    /// <param name="cancellationToken">Stops the deleting; what has been deleted stays deleted.</param>
    /// <returns>How many pieces of work were deleted.</returns>
    /// <exception cref="ArgumentException">No rule has that name, or several do.</exception>
    public Task<int> DiscardAsync(string rule, CancellationToken cancellationToken = default) => _db.DiscardRuleAsync(rule, cancellationToken);

    /// <summary>
    /// Queues a rule for every stored object its filter is true for now, as if each had just been
    /// saved: for when the rule is new, or its handler has changed and its work should be done
    /// again. <see cref="Rule.Changed"/> in the filter is taken to be true. The objects are found
    /// through the search index when it can express the filter, and by reading every object when
    /// it cannot.
    /// </summary>
    /// <param name="rule">The rule: <c>Note/summarize</c>, or just <c>summarize</c> when only one rule has that name.</param>
    /// <param name="cancellationToken">Stops the backfill; what has been queued stays queued.</param>
    /// <returns>How many objects the rule was queued for.</returns>
    /// <exception cref="ArgumentException">No rule has that name, or several do.</exception>
    public Task<int> BackfillAsync(string rule, CancellationToken cancellationToken = default) => _db.BackfillRuleAsync(rule, cancellationToken);
}

/// <summary>
/// A rule for saves of <typeparamref name="T"/> (and of types derived from it), made by
/// <see cref="RuleSet.When{T}"/>. Give it one or more actions: <see cref="Do"/> to act inside the
/// save, <see cref="DoQueued(RuleHandler{T}, string?)"/> to run a task later, <see cref="Modify"/>
/// to change the object as part of the save, <see cref="ModifyQueued(RuleHandler{T}, string?)"/>
/// to change it later,
/// <c>Materialize</c> to keep other objects in step with it,
/// <see cref="After"/> and <see cref="At"/> for actions at a later time. A rule with no
/// action makes opening the database throw. Rules must be complete before the database opens;
/// adding to or changing one afterwards throws.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class RuleBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly RuleRegistration _rule;
    private readonly RuleFilter<T> _filter;

    internal RuleBuilder(LottaConfiguration config, RuleRegistration rule, RuleFilter<T> filter)
    {
        _config = config;
        _rule = rule;
        _filter = filter;
    }

    /// <summary>
    /// Act inside every save the rule's filter is true for, after the object is written and
    /// indexed. The handler has full database access through <see cref="RuleContext.Db"/>; what it
    /// saves or deletes is part of the save's <see cref="ObjectResult.Changes"/>, and what it
    /// throws is in the save's <see cref="ObjectResult.Errors"/> (the save itself still happens).
    /// Inline actions run in the order they were added. It has no name and is never retried.
    /// </summary>
    /// <param name="handler">The work to do.</param>
    public RuleBuilder<T> Do(RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _config.ThrowIfOpened();
        _config.OnRegistrations.Add(new OnRegistration(typeof(T), RuleActions.Inline(TriggerKind.Saved, _filter.OnSave, handler)));
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Run a task later, from a queue, rather than inside the save: email, webhooks, calls to
    /// other services, anything slow. It is <see cref="Do"/>, queued. Every save the rule's filter
    /// is true for queues it; the handler is then given the object as it is when it runs, and is
    /// retried, waiting longer each time, until it succeeds or has failed
    /// <see cref="ILottaConfiguration.RuleMaxAttempts"/> times, when its work is set aside
    /// (<see cref="LiveRuleSet.StatusAsync"/>, <see cref="LiveRuleSet.RetryAsync"/>). It is
    /// delivered at least once, so it may run more than once for one save: give
    /// <see cref="RuleContext.IdempotencyKey"/> to an outside service so it can drop duplicates.
    /// Changes the handler makes to the object are not stored; use
    /// <see cref="ModifyQueued(string, RuleHandler{T})"/> for that.
    /// </summary>
    /// <param name="name">
    /// Identifies the rule across runs of the program, together with <typeparamref name="T"/>:
    /// <c>Note/notify</c>. 3 to 40 lower-case letters and digits, with single hyphens between
    /// them, unique among the rules for the type.
    /// </param>
    /// <param name="handler">The work to do.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public RuleBuilder<T> DoQueued(string name, RuleHandler<T> handler) => DoQueued(name, handler, options: null);

    /// <summary>
    /// Run a task later, from a queue, as <see cref="DoQueued(string, RuleHandler{T})"/>, with a
    /// priority or a concurrency limit of its own; see <see cref="QueuedRuleOptions"/>.
    /// </summary>
    public RuleBuilder<T> DoQueued(string name, RuleHandler<T> handler, QueuedRuleOptions? options)
    {
        _config.AddQueuedRule(RuleVerb.Do, name, _filter, handler, options: options);
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Run a task later, from a queue, as <see cref="DoQueued(string, RuleHandler{T})"/>, with the
    /// rule named after the handler method: its name without an <c>Async</c> suffix, in kebab
    /// case, so <c>Notify</c> gives <c>notify</c> and <c>handlers.NotifyFollowersAsync</c> gives
    /// <c>notify-followers</c>. Renaming the method renames the rule; work waiting under the old
    /// name is then run by all of the type's queued rules, once.
    /// </summary>
    /// <param name="handler">A method: <c>Notify</c> or <c>handlers.Notify</c>. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="handler"/> is not a method name (a lambda, or a call), or the name made from
    /// it is not a valid rule name or is already used for the type.
    /// </exception>
    public RuleBuilder<T> DoQueued(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => DoQueued(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(DoQueued)), handler);

    /// <summary>
    /// Change the object as part of the save: stamp a time, normalise a value, work out a
    /// property from others. The handler is given the object being saved, before it is written,
    /// and edits it; the one write then stores the result, and the caller's object carries the
    /// change. Keep it quick: the save waits for it. For slow work use
    /// <see cref="ModifyQueued(string, RuleHandler{T})"/>.
    /// <para>
    /// Modify actions run in the order they were added, before any <see cref="Do"/> or
    /// <c>Materialize</c> action, which therefore see the object as changed here.
    /// <see cref="Rule.Changed"/> in the filter compares what the caller is saving with the
    /// version it replaces. The handler must not change the object's key.
    /// </para>
    /// What the handler throws is in the save's <see cref="ObjectResult.Errors"/>, and the object
    /// is still saved, as the handler left it. It has no name and is never retried.
    /// </summary>
    /// <param name="handler">The change to make, on the object it is given.</param>
    public RuleBuilder<T> Modify(RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _config.ThrowIfOpened();
        var filter = _filter.OnSave;
        _config.ModifyRules.Add(new ModifyRule(typeof(T), (entity, db, cancellationToken) =>
            filter == null || filter((T)entity)
                ? handler((T)entity, new RuleContext(db, rule: "", ruleId: "", attempt: 1, entity, etag: null), cancellationToken)
                : Task.CompletedTask));
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Change the object itself later, from a queue: embeddings, summaries, classification. It is
    /// <see cref="Modify"/>, queued. The handler is given a copy of the object as it is when it
    /// runs, edits it, and returns nothing. LottaDB compares the copy with the original
    /// afterwards and writes it back only if something changed, outside any lock held during the
    /// handler.
    /// <para>
    /// If the object was saved by someone else meanwhile: when the filter names the rule's inputs
    /// with <see cref="Rule.Changed"/>, those inputs are still the same, and nobody touched the
    /// properties the handler set, only those properties are applied to the newer version;
    /// otherwise the result is dropped (a save that changed the inputs queued a fresh run). The
    /// handler is never called again because of a conflict. The rule's own write does not queue
    /// the rule again.
    /// </para>
    /// It is retried like <see cref="DoQueued(string, RuleHandler{T})"/>, and may run more than
    /// once for one save.
    /// </summary>
    /// <param name="name">
    /// Identifies the rule across runs of the program, together with <typeparamref name="T"/>:
    /// <c>Note/summarize</c>. 3 to 40 lower-case letters and digits, with single hyphens between
    /// them, unique among the rules for the type.
    /// </param>
    /// <param name="handler">The work to do, on the copy it is given.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public RuleBuilder<T> ModifyQueued(string name, RuleHandler<T> handler) => ModifyQueued(name, handler, options: null);

    /// <summary>
    /// Change the object later, from a queue, as <see cref="ModifyQueued(string, RuleHandler{T})"/>,
    /// with a priority or a concurrency limit of its own; see <see cref="QueuedRuleOptions"/>.
    /// </summary>
    public RuleBuilder<T> ModifyQueued(string name, RuleHandler<T> handler, QueuedRuleOptions? options)
    {
        _config.AddQueuedRule(RuleVerb.Modify, name, _filter, handler, options: options);
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Change the object later, from a queue, as <see cref="ModifyQueued(string, RuleHandler{T})"/>,
    /// with the rule named after the handler method: <c>Summarize</c> gives <c>summarize</c>.
    /// </summary>
    /// <param name="handler">A method: <c>Summarize</c> or <c>handlers.Summarize</c>. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="handler"/> is not a method name (a lambda, or a call), or the name made from
    /// it is not a valid rule name or is already used for the type.
    /// </exception>
    public RuleBuilder<T> ModifyQueued(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => ModifyQueued(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(ModifyQueued)), handler);

    /// <summary>
    /// Keep other objects in step with this one: a view, a set of links, anything that can be
    /// made from the object. The projection returns what should exist for the object; LottaDB
    /// saves what is new or different, deletes what the object no longer produces, and deletes
    /// all of it when the object is deleted.
    /// <para>
    /// It runs inside the save, like <see cref="Do"/>; <c>MaterializeQueued</c> runs it from the queue. The rule's filter says which objects have a projection at all:
    /// when it becomes false for an object, what the object produced is deleted.
    /// </para>
    /// <para>
    /// A produced object is stored under its own <c>[Key]</c>, which the projection must set and
    /// keep the same from run to run. It belongs to the one object it was produced from: two
    /// objects, or two rules, producing the same key is an error. A projection cannot produce
    /// objects of the type it is for.
    /// </para>
    /// </summary>
    /// <typeparam name="TView">The type produced. Returning a collection of objects keeps each of them.</typeparam>
    /// <param name="name">
    /// Identifies the rule across runs of the program, together with <typeparamref name="T"/>:
    /// <c>Note/note-view</c>. 3 to 40 lower-case letters and digits, with single hyphens between
    /// them, unique among the rules for the type. What a rule produced is found again by this
    /// name, so renaming a rule hands its objects to the new name the next time each source runs.
    /// </param>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public MaterializeBuilder<T> Materialize<TView>(string name, Func<T, TView?> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: false, name, (item, _, _) => Task.FromResult(RuleActions.Produced(projection(item))));
    }

    /// <summary>
    /// Keep other objects in step with this one, as <see cref="Materialize{TView}(string, Func{T, TView})"/>,
    /// with the rule named after the projection: a method gives its name (<c>ProjectNote</c> gives
    /// <c>project-note</c>), and a lambda gives the type it produces (<c>NoteView</c> gives
    /// <c>note-view</c>).
    /// </summary>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    /// <exception cref="ArgumentException">The name made is not a valid rule name, or is already used for the type: name the rule with the other overload.</exception>
    public MaterializeBuilder<T> Materialize<TView>(Func<T, TView?> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Materialize(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>Keep several objects in step with this one, as <see cref="Materialize{TView}(string, Func{T, TView})"/>: one for each the projection returns.</summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/links</c>.</param>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    public MaterializeBuilder<T> Materialize<TView>(string name, Func<T, IEnumerable<TView>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: false, name, (item, _, _) => Task.FromResult(RuleActions.Produced(projection(item))));
    }

    /// <summary>Keep several objects in step with this one, with the rule named after the projection; see <see cref="Materialize{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaterializeBuilder<T> Materialize<TView>(Func<T, IEnumerable<TView>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Materialize(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>
    /// Keep an object in step with this one, as <see cref="Materialize{TView}(string, Func{T, TView})"/>,
    /// with a projection that reads other objects through <see cref="RuleContext.Db"/>. Say which
    /// changes to those objects should run it again with <see cref="MaterializeBuilder{T}.RefreshWhen"/>.
    /// </summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/note-view</c>.</param>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    public MaterializeBuilder<T> Materialize<TView>(string name, Func<T, RuleContext, CancellationToken, Task<TView?>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: false, name, async (item, context, cancellationToken) =>
            RuleActions.Produced(await projection(item, context, cancellationToken).ConfigureAwait(false)));
    }

    /// <summary>Keep an object in step with this one, with a projection that reads other objects and the rule named after it; see <see cref="Materialize{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaterializeBuilder<T> Materialize<TView>(Func<T, RuleContext, CancellationToken, Task<TView?>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Materialize(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>Keep several objects in step with this one, with a projection that reads other objects; see <see cref="Materialize{TView}(string, Func{T, RuleContext, CancellationToken, Task{TView}})"/>.</summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/links</c>.</param>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    public MaterializeBuilder<T> Materialize<TView>(string name, Func<T, RuleContext, CancellationToken, Task<IEnumerable<TView>>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: false, name, async (item, context, cancellationToken) =>
            RuleActions.Produced(await projection(item, context, cancellationToken).ConfigureAwait(false)));
    }

    /// <summary>Keep several objects in step with this one, with a projection that reads other objects and the rule named after it; see <see cref="Materialize{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaterializeBuilder<T> Materialize<TView>(Func<T, RuleContext, CancellationToken, Task<IEnumerable<TView>>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Materialize(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>
    /// Keep other objects in step with this one, as <see cref="Materialize{TView}(string, Func{T, TView})"/>,
    /// but run the projection from the queue instead of inside the save: for a projection too slow
    /// for the save to wait for, such as one that reads many other objects.
    /// <para>
    /// What the object produces is there shortly after the save returns, not when it returns. A
    /// projection that throws is retried, then set aside, like <see cref="DoQueued(string, RuleHandler{T})"/>,
    /// and shows in <c>db.Rules.StatusAsync</c> rather than in the save's errors. Deleting the
    /// object still deletes what it produced inside the delete.
    /// </para>
    /// </summary>
    /// <typeparam name="TView">The type produced. Returning a collection of objects keeps each of them.</typeparam>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/note-view</c>.</param>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public MaterializeBuilder<T> MaterializeQueued<TView>(string name, Func<T, TView?> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: true, name, (item, _, _) => Task.FromResult(RuleActions.Produced(projection(item))));
    }

    /// <summary>
    /// Keep other objects in step with this one from the queue, as <see cref="Materialize{TView}(string, Func{T, TView})"/>,
    /// with the rule named after the projection: a method gives its name (<c>ProjectNote</c> gives
    /// <c>project-note</c>), and a lambda gives the type it produces (<c>NoteView</c> gives
    /// <c>note-view</c>).
    /// </summary>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    /// <exception cref="ArgumentException">The name made is not a valid rule name, or is already used for the type: name the rule with the other overload.</exception>
    public MaterializeBuilder<T> MaterializeQueued<TView>(Func<T, TView?> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => MaterializeQueued(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>Keep several objects in step with this one from the queue, as <see cref="Materialize{TView}(string, Func{T, TView})"/>: one for each the projection returns.</summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/links</c>.</param>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    public MaterializeBuilder<T> MaterializeQueued<TView>(string name, Func<T, IEnumerable<TView>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: true, name, (item, _, _) => Task.FromResult(RuleActions.Produced(projection(item))));
    }

    /// <summary>Keep several objects in step with this one from the queue, with the rule named after the projection; see <see cref="Materialize{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaterializeBuilder<T> MaterializeQueued<TView>(Func<T, IEnumerable<TView>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => MaterializeQueued(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>
    /// Keep an object in step with this one from the queue, as <see cref="Materialize{TView}(string, Func{T, TView})"/>,
    /// with a projection that reads other objects through <see cref="RuleContext.Db"/>. Say which
    /// changes to those objects should run it again with <see cref="MaterializeBuilder{T}.RefreshWhen"/>.
    /// </summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/note-view</c>.</param>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    public MaterializeBuilder<T> MaterializeQueued<TView>(string name, Func<T, RuleContext, CancellationToken, Task<TView?>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: true, name, async (item, context, cancellationToken) =>
            RuleActions.Produced(await projection(item, context, cancellationToken).ConfigureAwait(false)));
    }

    /// <summary>Keep an object in step with this one from the queue, with a projection that reads other objects and the rule named after it; see <see cref="Materialize{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaterializeBuilder<T> MaterializeQueued<TView>(Func<T, RuleContext, CancellationToken, Task<TView?>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => MaterializeQueued(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>Keep several objects in step with this one from the queue, with a projection that reads other objects; see <see cref="Materialize{TView}(string, Func{T, RuleContext, CancellationToken, Task{TView}})"/>.</summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/links</c>.</param>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    public MaterializeBuilder<T> MaterializeQueued<TView>(string name, Func<T, RuleContext, CancellationToken, Task<IEnumerable<TView>>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaterialize(queued: true, name, async (item, context, cancellationToken) =>
            RuleActions.Produced(await projection(item, context, cancellationToken).ConfigureAwait(false)));
    }

    /// <summary>Keep several objects in step with this one from the queue, with a projection that reads other objects and the rule named after it; see <see cref="Materialize{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaterializeBuilder<T> MaterializeQueued<TView>(Func<T, RuleContext, CancellationToken, Task<IEnumerable<TView>>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => MaterializeQueued(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>
    /// Act some time after the save, rather than in it:
    /// <code>
    /// config.Rules.When&lt;Invite&gt;(i =&gt; Changed(i.SentAt) &amp;&amp; !i.Accepted)
    ///     .After(TimeSpan.FromDays(3))
    ///     .Do(SendReminder);
    /// </code>
    /// The filter is checked in the save, and again (without its <c>Changed</c> parts) when the
    /// time comes: an invite accepted meanwhile gets no reminder. Every save the filter is true
    /// for starts a wait of its own, so the filter usually names the change that starts it with
    /// <see cref="Rule.Changed"/>. What follows runs from the queue.
    /// </summary>
    /// <param name="delay">How long to wait. It may be longer than a queue can hide a message for (7 days): the wait is then made in steps.</param>
    /// <returns>The delayed part of the rule: give it its work with <c>.Do</c> or <c>.Modify</c>.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The delay is negative.</exception>
    public DelayedRuleBuilder<T> After(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        _config.ThrowIfOpened();
        return new DelayedRuleBuilder<T>(_config, _rule, _filter, delay, at: null);
    }

    /// <summary>
    /// Act at a time taken from the object:
    /// <code>
    /// config.Rules.When&lt;Session&gt;().At(s =&gt; s.ExpiresAt).Do(EndSession);
    /// </code>
    /// The time is read in the save, and again when it comes: if the object then gives a later
    /// time (the session was extended), the action waits for that; if it gives none, the action
    /// is dropped. A time already past runs the action as soon as it can be. What follows runs
    /// from the queue.
    /// </summary>
    /// <param name="time">The time to run at, from the object; null for not at all.</param>
    /// <returns>The delayed part of the rule: give it its work with <c>.Do</c> or <c>.Modify</c>.</returns>
    public DelayedRuleBuilder<T> At(Func<T, DateTimeOffset?> time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _config.ThrowIfOpened();
        return new DelayedRuleBuilder<T>(_config, _rule, _filter, after: null, time);
    }

    private MaterializeBuilder<T> AddMaterialize(bool queued, string name, Func<T, RuleContext, CancellationToken, Task<List<object>>> project)
    {
        var rule = _config.AddMaterializeRule(name, _filter, project, queued);
        // In every save and delete of the object: the projection when it runs inside the save,
        // and the clean-up when the object is deleted.
        _config.OnRegistrations.Add(new OnRegistration(typeof(T),
            (EntityHandler<T>)((entity, kind, db, cancellationToken) => db.MaterializeInSaveAsync(rule, entity, kind, cancellationToken))));
        _rule.Actions++;
        return new MaterializeBuilder<T>(_config, rule);
    }
}

/// <summary>
/// A <c>Materialize</c> rule, as <see cref="RuleBuilder{T}.Materialize{TView}(string, Func{T, TView})"/>
/// returns it: say which other objects it depends on.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class MaterializeBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly QueuedRule _rule;

    internal MaterializeBuilder(LottaConfiguration config, QueuedRule rule)
    {
        _config = config;
        _rule = rule;
    }

    /// <summary>
    /// Run the projection again when an object it reads changes. Every save or delete of a
    /// <typeparamref name="TOther"/> asks which <typeparamref name="T"/> objects depend on it,
    /// and queues the rule for each:
    /// <code>
    /// config.Rules.When&lt;Note&gt;()
    ///     .Materialize(ProjectNoteView)
    ///     .RefreshWhen&lt;Actor&gt;(actor =&gt; note =&gt; note.AuthorId == actor.Username);
    /// </code>
    /// The objects are found through the search index when it can express the condition
    /// (<c>[Queryable]</c> properties compared with values of the other object), and by reading
    /// every object when it cannot. The refresh runs from the queue, not inside that object's save.
    /// </summary>
    /// <typeparam name="TOther">The type the projection reads.</typeparam>
    /// <param name="sources">Given the object that changed, a condition that is true for the objects whose projection reads it.</param>
    public MaterializeBuilder<T> RefreshWhen<TOther>(Func<TOther, Expression<Func<T, bool>>> sources) where TOther : class, new()
        => RefreshWhen(null, sources);

    /// <summary>
    /// Run the projection again when an object it reads changes in a way that matters to it, as
    /// <see cref="RefreshWhen{TOther}(Func{TOther, Expression{Func{T, bool}}})"/> but only for the
    /// saves <paramref name="when"/> is true for. Name what the projection reads with
    /// <see cref="Rule.Changed"/>, so that other saves of the object queue nothing:
    /// <code>
    /// config.Rules.When&lt;Note&gt;()
    ///     .Materialize(ProjectNoteView)
    ///     .RefreshWhen&lt;Actor&gt;(actor =&gt; Changed(actor.DisplayName),
    ///                         actor =&gt; note =&gt; note.AuthorId == actor.Username);
    /// </code>
    /// A delete of the other object always refreshes, whatever <paramref name="when"/> says.
    /// </summary>
    /// <typeparam name="TOther">The type the projection reads.</typeparam>
    /// <param name="when">Which saves of the other object refresh; null for all. It runs inside that object's save, like a rule's filter.</param>
    /// <param name="sources">Given the object that changed, a condition that is true for the objects whose projection reads it.</param>
    /// <exception cref="ArgumentException"><paramref name="when"/> uses <c>Changed</c> in a way that cannot be evaluated.</exception>
    public MaterializeBuilder<T> RefreshWhen<TOther>(Expression<Func<TOther, bool>>? when, Func<TOther, Expression<Func<T, bool>>> sources) where TOther : class, new()
    {
        ArgumentNullException.ThrowIfNull(sources);
        _config.ThrowIfOpened();
        var rule = _rule;
        var filter = RuleFilter.Parse(when);
        if (filter.UsesChanged)
            _config.ChangedTypes.Add(typeof(TOther));
        var matters = filter.OnSave;
        _config.OnRegistrations.Add(new OnRegistration(typeof(TOther),
            (EntityHandler<TOther>)((other, kind, db, cancellationToken) =>
                kind == TriggerKind.Deleted || matters == null || matters(other)
                    ? db.RefreshSourcesAsync(rule, sources(other), cancellationToken)
                    : Task.CompletedTask)));
        return this;
    }
}

/// <summary>
/// The delayed part of a rule, as <see cref="RuleBuilder{T}.After"/> and
/// <see cref="RuleBuilder{T}.At"/> return it. Its actions run from the queue when the time comes,
/// and are retried and set aside like <see cref="RuleBuilder{T}.DoQueued(string, RuleHandler{T})"/>:
/// <see cref="Do(string, RuleHandler{T})"/> to run a task, <see cref="Modify(string, RuleHandler{T})"/>
/// to change the object.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class DelayedRuleBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly RuleRegistration _rule;
    private readonly RuleFilter<T> _filter;
    private readonly TimeSpan? _after;
    private readonly Func<T, DateTimeOffset?>? _at;

    internal DelayedRuleBuilder(LottaConfiguration config, RuleRegistration rule, RuleFilter<T> filter, TimeSpan? after, Func<T, DateTimeOffset?>? at)
    {
        _config = config;
        _rule = rule;
        _filter = filter;
        _after = after;
        _at = at;
    }

    /// <summary>
    /// Run a task when the time comes, as <see cref="RuleBuilder{T}.DoQueued(string, RuleHandler{T})"/>
    /// does as soon as it can. The handler is given the object as it is then.
    /// </summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Invite/send-reminder</c>.</param>
    /// <param name="handler">The work to do.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    public DelayedRuleBuilder<T> Do(string name, RuleHandler<T> handler)
    {
        _config.AddQueuedRule(RuleVerb.Do, name, _filter, handler, _after, _at);
        _rule.Actions++;
        return this;
    }

    /// <summary>Run a task when the time comes, with the rule named after the handler method: <c>SendReminder</c> gives <c>send-reminder</c>.</summary>
    /// <param name="handler">A method. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="handler"/> is not a method name, or the name made from it is not valid or is already used.</exception>
    public DelayedRuleBuilder<T> Do(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => Do(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(Do)), handler);

    /// <summary>
    /// Change the object when the time comes, as <see cref="RuleBuilder{T}.ModifyQueued(string, RuleHandler{T})"/>
    /// does as soon as it can: the handler edits a copy, which is written back if it changed.
    /// </summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Trial/mark-expired</c>.</param>
    /// <param name="handler">The work to do, on the copy it is given.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    public DelayedRuleBuilder<T> Modify(string name, RuleHandler<T> handler)
    {
        _config.AddQueuedRule(RuleVerb.Modify, name, _filter, handler, _after, _at);
        _rule.Actions++;
        return this;
    }

    /// <summary>Change the object when the time comes, with the rule named after the handler method: <c>MarkExpired</c> gives <c>mark-expired</c>.</summary>
    /// <param name="handler">A method. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="handler"/> is not a method name, or the name made from it is not valid or is already used.</exception>
    public DelayedRuleBuilder<T> Modify(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => Modify(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(Modify)), handler);
}

/// <summary>
/// A rule for deletes of <typeparamref name="T"/> (and of types derived from it), made by
/// <see cref="RuleSet.WhenDeleted{T}"/>. Give it an action with <c>Do</c> (inside the delete) or
/// <c>DoQueued</c> (later, from the queue); a rule with no action makes opening the database throw.
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

    /// <summary>
    /// Run a task from the queue after every delete the rule's filter is true for: <see cref="Do"/>,
    /// queued. For telling an outside service the object is gone, and anything else too slow, or
    /// too likely to fail, for the delete to wait for.
    /// <para>
    /// The handler is given the object as it was. It is delivered at least once: a handler that
    /// throws is retried, then set aside, and <see cref="RuleContext.IdempotencyKey"/> is the same
    /// on every retry. The deleted object is kept aside as a tombstone, out of sight of every
    /// read and query, until the last of its queued rules has run; its key is free at once.
    /// </para>
    /// </summary>
    /// <param name="name">
    /// Identifies the rule across runs of the program, together with <typeparamref name="T"/>:
    /// <c>Note/unpublish</c>. 3 to 40 lower-case letters and digits, with single hyphens between
    /// them, unique among the rules for the type.
    /// </param>
    /// <param name="handler">The work to do.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public DeletedRuleBuilder<T> DoQueued(string name, RuleHandler<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _config.AddDeletedRule(name, _filter, handler);
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Run a task from the queue after a delete, as <see cref="DoQueued(string, RuleHandler{T})"/>,
    /// with the rule named after the handler method: <c>RemoveFromSearch</c> gives <c>remove-from-search</c>.
    /// </summary>
    /// <param name="handler">The work to do: a method, not a lambda.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="handler"/> is not a method name (a lambda, or a call), or the name made from
    /// it is not a valid rule name or is already used for the type.
    /// </exception>
    public DeletedRuleBuilder<T> DoQueued(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => DoQueued(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(DoQueued)), handler);
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
    private readonly RuleFilter<T> _filter;

    internal LiveRule(LottaDB db, RuleFilter<T> filter)
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
        if (_filter.UsesChanged)
            _db.TrackChanges(typeof(T));
        return _db.AddHandler(RuleActions.Inline(TriggerKind.Saved, _filter.OnSave, handler));
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

/// <summary>When queued rules run.</summary>
public enum QueueMode
{
    /// <summary>By themselves, in the background, for as long as the database is open.</summary>
    Auto,

    /// <summary>Only when <see cref="LottaDB.DrainAsync"/> is called.</summary>
    Manual,
}

/// <summary>Where a database's queued rules stand; see <see cref="LiveRuleSet.StatusAsync"/>.</summary>
/// <param name="Rules">One entry per queued rule in the configuration, and one per orphaned rule that has work set aside.</param>
/// <param name="Types">One entry per object type that has had work queued.</param>
public sealed record RulesStatus(IReadOnlyList<RuleStatus> Rules, IReadOnlyList<RuleTypeStatus> Types);

/// <summary>Where one queued rule stands.</summary>
/// <param name="Rule">The rule's identity: the type it is for and its name, <c>Note/summarize</c>.</param>
/// <param name="Running">Runs in progress in this process.</param>
/// <param name="Poisoned">
/// Pieces of work set aside after failing too often. Counted by reading the set-aside work, up to
/// 32 pieces per object type, so with more than that set aside it is a lower bound;
/// <see cref="RuleTypeStatus.Poisoned"/> has the full count per type.
/// </param>
/// <param name="LastError">The message of the rule's most recent failure in this process; null after a success.</param>
/// <param name="Orphaned">
/// True when no rule in the configuration has this identity: the rule was renamed or removed, and
/// work set aside for it remains. Retry it with <see cref="LiveRuleSet.RetryAsync"/> (it then runs
/// all of the type's current queued rules) or delete it with <see cref="LiveRuleSet.DiscardAsync"/>.
/// </param>
public sealed record RuleStatus(string Rule, int Running, int Poisoned, string? LastError, bool Orphaned = false);

/// <summary>How much queued work there is for one object type.</summary>
/// <param name="Type">The full name of the object type.</param>
/// <param name="Waiting">Messages in the type's queue, including any being worked on or waiting to be retried. One message is one save, whatever number of rules it lists. Approximate.</param>
/// <param name="Poisoned">Pieces of work set aside after failing too often, for any rule. Approximate.</param>
/// <param name="Orphaned">
/// True when the configuration has no queued rule for the type any more (or the type no longer
/// exists). Its waiting work is not run.
/// </param>
public sealed record RuleTypeStatus(string Type, int Waiting, int Poisoned, bool Orphaned = false);

/// <summary>Which queued work is taken first when there is more ready than can run at once.</summary>
public enum RulePriority
{
    /// <summary>Taken only when no other work is ready: slow work that can wait, such as calls to a language model.</summary>
    Low = -1,

    /// <summary>The default.</summary>
    Normal = 0,

    /// <summary>Taken before all other work.</summary>
    High = 1,
}

/// <summary>
/// How a queued rule's work is scheduled, beyond the database's own settings. A rule given
/// either setting has a queue of its own, apart from the other rules of its type, so that its
/// waiting work never holds theirs up.
/// </summary>
public sealed record QueuedRuleOptions
{
    /// <summary>
    /// Which work is taken first. When a slot to run in frees up, ready work of a higher priority
    /// is always taken before work of a lower one; work of the same priority is taken in turn.
    /// Give slow work <see cref="RulePriority.Low"/> so it cannot keep quick work waiting.
    /// </summary>
    public RulePriority Priority { get; init; } = RulePriority.Normal;

    /// <summary>
    /// The most runs of this rule at once, in one process, within
    /// <see cref="ILottaConfiguration.MaxConcurrency"/>; null for no limit of its own. Use it to
    /// keep within what a service the rule calls can take (a model server's slots, an API's rate
    /// limit) while other rules use the rest.
    /// </summary>
    public int? MaxConcurrency { get; init; }
}

/// <summary>The kinds of queued action, in the order a run does them.</summary>
internal enum RuleVerb
{
    /// <summary>Changes the object itself.</summary>
    Modify = 0,

    /// <summary>Keeps other objects in step with the object.</summary>
    Materialize = 1,

    /// <summary>Runs a task.</summary>
    Do = 2,
}

/// <summary>
/// A queued action as registered: what it is for and how to call it. Its identity is
/// <see cref="Id"/>, the type it was registered for and its name.
/// </summary>
internal sealed class QueuedRule
{
    /// <summary><c>Note/summarize</c>: what messages and set-aside work name the rule by.</summary>
    public required string Id { get; init; }

    /// <summary>The name alone: <c>summarize</c>.</summary>
    public required string Name { get; init; }

    /// <summary>The type the rule was registered for; it also runs for types derived from it.</summary>
    public required Type ObjectType { get; init; }

    public required RuleVerb Verb { get; init; }

    /// <summary>A <c>Materialize</c> rule that runs inside the save; false for <c>MaterializeQueued</c> and every other verb.</summary>
    public bool InSave { get; init; }

    /// <summary>A rule of deletes (<c>WhenDeleted(...).DoQueued</c>): queued by a delete, never by a save, and given the object as it was. <see cref="OnSave"/> is then its filter, checked in the delete.</summary>
    public bool OnDelete { get; init; }

    /// <summary>Where the rule comes among all queued rules, for a stable order within a phase.</summary>
    public required int Order { get; init; }

    /// <summary>Calls the handler.</summary>
    public required Func<object, RuleContext, CancellationToken, Task> Invoke { get; init; }

    /// <summary>The whole filter, checked in the save; null when every save matches.</summary>
    public Func<object, bool>? OnSave { get; init; }

    /// <summary>The filter without its <c>Changed</c> parts, checked when a run comes up; null when nothing is left to check.</summary>
    public Func<object, bool>? Rest { get; init; }

    /// <summary>
    /// Asks the search index for the objects <see cref="Rest"/> is true for; null when there is
    /// nothing to ask. Throws when the index cannot express the filter.
    /// </summary>
    public Func<LottaDB, System.Collections.IEnumerable>? FindIndexed { get; init; }

    /// <summary>The values named by <c>Changed(...)</c> in the filter: a <c>Modify</c> rule's inputs.</summary>
    public Func<object, object?>[] Inputs { get; init; } = [];

    /// <summary>Whether the filter uses <c>Changed(...)</c>.</summary>
    public bool UsesChanged { get; init; }

    /// <summary>For a <c>Materialize</c> rule: the objects it keeps for one source object. Null for the other verbs.</summary>
    public Func<object, RuleContext, CancellationToken, Task<List<object>>>? Project { get; init; }

    /// <summary>How long after the save the rule runs (<c>After</c>); null for as soon as it can.</summary>
    public TimeSpan? After { get; set; }

    /// <summary>The time, taken from the object, at which the rule runs (<c>At</c>); null when the rule has none.</summary>
    public Func<object, DateTimeOffset?>? At { get; set; }

    /// <summary>Which work is taken first; see <see cref="QueuedRuleOptions.Priority"/>.</summary>
    public RulePriority Priority { get; set; }

    /// <summary>The most runs of the rule at once in one process; null for no limit of its own.</summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    /// The most objects one run of the rule takes at once, from its own queue; 1 for one at a
    /// time. Only the <c>embed</c> rule (<see cref="Rule.EmbedVectors"/>) is run in batches: one
    /// call to the embedding generator and one write for all of them.
    /// </summary>
    public int BatchSize { get; set; } = 1;

    /// <summary>Whether the rule's work has a queue of its own, apart from the other rules of its type.</summary>
    public bool OwnQueue => Priority != RulePriority.Normal || MaxConcurrency != null || BatchSize > 1;
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
    public static EntityHandler<T> Inline<T>(TriggerKind trigger, Func<T, bool>? filter, RuleHandler<T> handler) where T : class =>
        (entity, kind, db, cancellationToken) =>
            kind == trigger && (filter?.Invoke(entity) ?? true)
                ? handler(entity, new RuleContext(db, rule: "", ruleId: "", attempt: 1, entity, etag: null), cancellationToken)
                : Task.CompletedTask;

    /// <summary>What a projection returned, as the objects to keep: none for null, each element of a collection, or the one object.</summary>
    public static List<object> Produced(object? result) => result switch
    {
        null => [],
        System.Collections.IEnumerable many => [.. many.OfType<object>()],   // which leaves out nulls
        _ => [result],
    };

    /// <summary>The filter of a rule for deletes, compiled. A delete has no earlier version to compare with, so it cannot use <c>Changed</c>.</summary>
    public static Func<T, bool>? DeletedFilter<T>(Expression<Func<T, bool>>? filter) where T : class
    {
        var parsed = RuleFilter.Parse(filter);
        if (parsed.UsesChanged)
            throw new ArgumentException(
                "Changed(...) cannot be used in a WhenDeleted filter: a delete changes nothing but whether the object exists.", nameof(filter));
        return parsed.OnSave;
    }
}

/// <summary>An in-save <c>Modify</c> action: run on an object of the type (or a type derived from it) before its row is written.</summary>
/// <param name="ObjectType">The type the action was registered for.</param>
/// <param name="Invoke">Checks the filter and calls the handler.</param>
internal sealed record ModifyRule(Type ObjectType, Func<object, LottaDB, CancellationToken, Task> Invoke);
