using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Lotta.Internal;

namespace Lotta;

/// <summary>
/// The work a rule does for one object. Every verb (<c>Do</c>, <c>Update</c>, <c>Then</c>) takes
/// this shape.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
/// <param name="item">
/// The object that was saved, or deleted. For <c>Update</c>, a copy to edit. It carries its key and
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

    /// <summary>The rule's name, such as <c>summarize</c>; empty for an inline <c>Do</c>, which has none.</summary>
    public string Rule { get; }

    /// <summary>1 on the first run, 2 on the first retry, and so on. Always 1 for <c>Do</c>.</summary>
    public int Attempt { get; }

    /// <summary>
    /// A key made from the object's type, key and ETag at the save that caused the run, and the
    /// rule's name. It is the same on every retry of that work, so an outside service given it can
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
    /// <c>Update</c> rule what its inputs are.
    /// <code>
    /// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.IsPublished) &amp;&amp; n.IsPublished).Then(NotifyFollowers);
    /// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.Content)).Update(Summarize);
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
}

/// <summary>
/// A database's rules, set in its configuration (<see cref="ILottaConfiguration.Rules"/>): what to
/// do when objects are saved or deleted.
/// <code>
/// config.Rules.When&lt;Note&gt;(n =&gt; n.IsPublished).Do(UpdateFeedView);
/// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.Content)).Update(Summarize);
/// config.Rules.When&lt;Note&gt;(n =&gt; Changed(n.IsPublished) &amp;&amp; n.IsPublished).Then(NotifyFollowers);
/// config.Rules.WhenDeleted&lt;Note&gt;().Do(RemoveFeedView);
/// </code>
/// </summary>
public sealed class RuleSet
{
    private readonly LottaConfiguration _config;

    internal RuleSet(LottaConfiguration config) => _config = config;

    /// <summary>
    /// Start a rule for saves of <typeparamref name="T"/> (and of types derived from it). Give it
    /// actions: <c>.Do(handler)</c> runs inside the save; <c>.Update(handler)</c> changes the
    /// object in the background; <c>.Maintain(projection)</c> keeps other objects in step with
    /// it; <c>.Then(handler)</c> does work outside the database in the background. Several actions may follow one <c>When</c>, and several rules may be made for
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
    /// skipped, and not counted as a run). For <c>.Maintain</c> it says which objects have a
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
    /// Start a rule for deletes of <typeparamref name="T"/> (and of types derived from it). Give it
    /// an action with <c>.Do(handler)</c>, which runs inside the delete and is given the object as
    /// it was. A rule with no action makes opening the database throw.
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
/// save, <see cref="Update(RuleHandler{T}, string?)"/> to change the object in the background,
/// <c>Maintain</c> to keep other objects in step with it,
/// <see cref="Then(RuleHandler{T}, string?)"/> for work outside the database. A rule with no
/// action makes opening the database throw. Rules must be complete before the database opens;
/// adding to or changing one afterwards throws.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class RuleBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly RuleRegistration _rule;
    private readonly RuleFilter<T> _filter;
    private TimeSpan? _after;
    private Func<T, DateTimeOffset?>? _at;

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
        ThrowIfTimed(nameof(Do));
        _config.OnRegistrations.Add(new OnRegistration(typeof(T), RuleActions.Inline(TriggerKind.Saved, _filter.OnSave, handler)));
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Do work outside the database, later and in the background: email, webhooks, calls to other
    /// services. Every save the rule's filter is true for queues it; the handler is then given the
    /// object as it is when it runs, and is retried, waiting longer each time, until it succeeds
    /// or has failed <see cref="ILottaConfiguration.RuleMaxAttempts"/> times, when its work is set
    /// aside (<see cref="LiveRuleSet.StatusAsync"/>, <see cref="LiveRuleSet.RetryAsync"/>). It is
    /// delivered at least once, so it may run more than once for one save: give
    /// <see cref="RuleContext.IdempotencyKey"/> to the outside service so it can drop duplicates.
    /// </summary>
    /// <param name="name">
    /// Identifies the rule across runs of the program, together with <typeparamref name="T"/>:
    /// <c>Note/notify</c>. 3 to 40 lower-case letters and digits, with single hyphens between
    /// them, unique among the rules for the type.
    /// </param>
    /// <param name="handler">The work to do.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public RuleBuilder<T> Then(string name, RuleHandler<T> handler)
    {
        _config.AddQueuedRule(RuleVerb.Then, name, _filter, handler, _after, _at);
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Do work outside the database, as <see cref="Then(string, RuleHandler{T})"/>, with the rule
    /// named after the handler method: its name without an <c>Async</c> suffix, in kebab case, so
    /// <c>Notify</c> gives <c>notify</c> and <c>handlers.NotifyFollowersAsync</c> gives
    /// <c>notify-followers</c>. Renaming the method renames the rule; work waiting under the old
    /// name is then run by all of the type's queued rules, once.
    /// </summary>
    /// <param name="handler">A method: <c>Notify</c> or <c>handlers.Notify</c>. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="handler"/> is not a method name (a lambda, or a call), or the name made from
    /// it is not a valid rule name or is already used for the type.
    /// </exception>
    public RuleBuilder<T> Then(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => Then(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(Then)), handler);

    /// <summary>
    /// Change the object itself, later and in the background: embeddings, summaries,
    /// classification. The handler is given a copy of the object as it is when it runs, edits it,
    /// and returns nothing. LottaDB compares the copy with the original afterwards and writes it
    /// back only if something changed, outside any lock held during the handler.
    /// <para>
    /// If the object was saved by someone else meanwhile: when the filter names the rule's inputs
    /// with <see cref="Rule.Changed"/>, those inputs are still the same, and nobody touched the
    /// properties the handler set, only those properties are applied to the newer version;
    /// otherwise the result is dropped (a save that changed the inputs queued a fresh run). The
    /// handler is never called again because of a conflict. The rule's own write does not queue
    /// the rule again.
    /// </para>
    /// It is retried like <see cref="Then(string, RuleHandler{T})"/>, and may run more than once
    /// for one save.
    /// </summary>
    /// <param name="name">
    /// Identifies the rule across runs of the program, together with <typeparamref name="T"/>:
    /// <c>Note/summarize</c>. 3 to 40 lower-case letters and digits, with single hyphens between
    /// them, unique among the rules for the type.
    /// </param>
    /// <param name="handler">The work to do, on the copy it is given.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for the type.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database is opened, if its storage has no queues.</exception>
    public RuleBuilder<T> Update(string name, RuleHandler<T> handler)
    {
        _config.AddQueuedRule(RuleVerb.Update, name, _filter, handler, _after, _at);
        _rule.Actions++;
        return this;
    }

    /// <summary>
    /// Change the object in the background, as <see cref="Update(string, RuleHandler{T})"/>, with
    /// the rule named after the handler method: <c>Summarize</c> gives <c>summarize</c>.
    /// </summary>
    /// <param name="handler">A method: <c>Summarize</c> or <c>handlers.Summarize</c>. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="handler"/> is not a method name (a lambda, or a call), or the name made from
    /// it is not a valid rule name or is already used for the type.
    /// </exception>
    public RuleBuilder<T> Update(RuleHandler<T> handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => Update(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(Update)), handler);

    /// <summary>
    /// Keep other objects in step with this one: a view, a set of links, anything that can be
    /// made from the object. The projection returns what should exist for the object; LottaDB
    /// saves what is new or different, deletes what the object no longer produces, and deletes
    /// all of it when the object is deleted.
    /// <para>
    /// It runs inside the save, like <see cref="Do"/>, unless <see cref="MaintainBuilder{T}.Queued"/>
    /// moves it to the background. The rule's filter says which objects have a projection at all:
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
    public MaintainBuilder<T> Maintain<TView>(string name, Func<T, TView?> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaintain(name, (item, _, _) => Task.FromResult(RuleActions.Produced(projection(item))));
    }

    /// <summary>
    /// Keep other objects in step with this one, as <see cref="Maintain{TView}(string, Func{T, TView})"/>,
    /// with the rule named after the projection: a method gives its name (<c>ProjectNote</c> gives
    /// <c>project-note</c>), and a lambda gives the type it produces (<c>NoteView</c> gives
    /// <c>note-view</c>).
    /// </summary>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    /// <exception cref="ArgumentException">The name made is not a valid rule name, or is already used for the type: name the rule with the other overload.</exception>
    public MaintainBuilder<T> Maintain<TView>(Func<T, TView?> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Maintain(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>Keep several objects in step with this one, as <see cref="Maintain{TView}(string, Func{T, TView})"/>: one for each the projection returns.</summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/links</c>.</param>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    public MaintainBuilder<T> Maintain<TView>(string name, Func<T, IEnumerable<TView>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaintain(name, (item, _, _) => Task.FromResult(RuleActions.Produced(projection(item))));
    }

    /// <summary>Keep several objects in step with this one, with the rule named after the projection; see <see cref="Maintain{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaintainBuilder<T> Maintain<TView>(Func<T, IEnumerable<TView>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Maintain(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>
    /// Keep an object in step with this one, as <see cref="Maintain{TView}(string, Func{T, TView})"/>,
    /// with a projection that reads other objects through <see cref="RuleContext.Db"/>. Say which
    /// changes to those objects should run it again with <see cref="MaintainBuilder{T}.RefreshWhen"/>.
    /// </summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/note-view</c>.</param>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    public MaintainBuilder<T> Maintain<TView>(string name, Func<T, RuleContext, CancellationToken, Task<TView?>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaintain(name, async (item, context, cancellationToken) =>
            RuleActions.Produced(await projection(item, context, cancellationToken).ConfigureAwait(false)));
    }

    /// <summary>Keep an object in step with this one, with a projection that reads other objects and the rule named after it; see <see cref="Maintain{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the object to keep for one source object; null for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaintainBuilder<T> Maintain<TView>(Func<T, RuleContext, CancellationToken, Task<TView?>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Maintain(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>Keep several objects in step with this one, with a projection that reads other objects; see <see cref="Maintain{TView}(string, Func{T, RuleContext, CancellationToken, Task{TView}})"/>.</summary>
    /// <param name="name">Identifies the rule, with <typeparamref name="T"/>: <c>Note/links</c>.</param>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    public MaintainBuilder<T> Maintain<TView>(string name, Func<T, RuleContext, CancellationToken, Task<IEnumerable<TView>>> projection) where TView : class, new()
    {
        ArgumentNullException.ThrowIfNull(projection);
        return AddMaintain(name, async (item, context, cancellationToken) =>
            RuleActions.Produced(await projection(item, context, cancellationToken).ConfigureAwait(false)));
    }

    /// <summary>Keep several objects in step with this one, with a projection that reads other objects and the rule named after it; see <see cref="Maintain{TView}(Func{T, TView}, string?)"/>.</summary>
    /// <param name="projection">Makes the objects to keep for one source object; empty for none.</param>
    /// <param name="projectionExpression">Filled in by the compiler with the source text of <paramref name="projection"/>.</param>
    public MaintainBuilder<T> Maintain<TView>(Func<T, RuleContext, CancellationToken, Task<IEnumerable<TView>>> projection,
        [CallerArgumentExpression(nameof(projection))] string? projectionExpression = null) where TView : class, new()
        => Maintain(RuleQueueNames.ProjectionName(projectionExpression, typeof(TView)), projection);

    /// <summary>
    /// Run the <c>Then</c> and <c>Update</c> actions that follow some time after the save, rather
    /// than as soon as they can be:
    /// <code>
    /// config.Rules.When&lt;Invite&gt;(i =&gt; Changed(i.SentAt) &amp;&amp; !i.Accepted)
    ///     .After(TimeSpan.FromDays(3))
    ///     .Then(SendReminder);
    /// </code>
    /// The filter is checked in the save, and again (without its <c>Changed</c> parts) when the
    /// time comes: an invite accepted meanwhile gets no reminder. Every save the filter is true
    /// for starts a wait of its own, so the filter usually names the change that starts it with
    /// <see cref="Rule.Changed"/>. Replaces an earlier <c>After</c> or <c>At</c> on this rule.
    /// </summary>
    /// <param name="delay">How long to wait. It may be longer than a queue can hide a message for (7 days): the wait is then made in steps.</param>
    /// <exception cref="ArgumentOutOfRangeException">The delay is negative.</exception>
    public RuleBuilder<T> After(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        _config.ThrowIfOpened();
        _after = delay;
        _at = null;
        return this;
    }

    /// <summary>
    /// Run the <c>Then</c> and <c>Update</c> actions that follow at a time taken from the object:
    /// <code>
    /// config.Rules.When&lt;Session&gt;().At(s =&gt; s.ExpiresAt).Then(EndSession);
    /// </code>
    /// The time is read in the save, and again when it comes: if the object then gives a later
    /// time (the session was extended), the action waits for that; if it gives none, the action
    /// is dropped. A time already past runs the action as soon as it can be. Replaces an earlier
    /// <c>After</c> or <c>At</c> on this rule.
    /// </summary>
    /// <param name="time">The time to run at, from the object; null for not at all.</param>
    public RuleBuilder<T> At(Func<T, DateTimeOffset?> time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _config.ThrowIfOpened();
        _at = time;
        _after = null;
        return this;
    }

    /// <summary>Throws for a verb that cannot be delayed, when <c>After</c> or <c>At</c> came before it.</summary>
    private void ThrowIfTimed(string verb)
    {
        if (_after != null || _at != null)
            throw new InvalidOperationException(
                $".{verb} runs inside the save, so it cannot follow .After or .At. Delay a .Then or an .Update instead.");
    }

    private MaintainBuilder<T> AddMaintain(string name, Func<T, RuleContext, CancellationToken, Task<List<object>>> project)
    {
        ThrowIfTimed("Maintain");
        var rule = _config.AddMaintainRule(name, _filter, project);
        // In every save and delete of the object: the projection when it runs inline, and the
        // clean-up when the object is deleted.
        _config.OnRegistrations.Add(new OnRegistration(typeof(T),
            (EntityHandler<T>)((entity, kind, db, cancellationToken) => db.MaintainInSaveAsync(rule, entity, kind, cancellationToken))));
        _rule.Actions++;
        return new MaintainBuilder<T>(_config, rule);
    }
}

/// <summary>
/// A <c>Maintain</c> rule, as <see cref="RuleBuilder{T}.Maintain{TView}(string, Func{T, TView})"/>
/// returns it: say whether it runs in the background, and which other objects it depends on.
/// </summary>
/// <typeparam name="T">The object type the rule is for.</typeparam>
public sealed class MaintainBuilder<T> where T : class, new()
{
    private readonly LottaConfiguration _config;
    private readonly QueuedRule _rule;

    internal MaintainBuilder(LottaConfiguration config, QueuedRule rule)
    {
        _config = config;
        _rule = rule;
    }

    /// <summary>
    /// Run the projection in the background rather than inside the save: for a projection that is
    /// slow, or that produces many objects. It is then queued, retried and set aside like
    /// <see cref="RuleBuilder{T}.Then(string, RuleHandler{T})"/>, and what it keeps lags the
    /// object until its run comes up. Deleting the object still deletes what it produced at once.
    /// </summary>
    public MaintainBuilder<T> Queued()
    {
        _config.ThrowIfOpened();
        _rule.Inline = false;
        return this;
    }

    /// <summary>
    /// Run the projection again when an object it reads changes. Every save or delete of a
    /// <typeparamref name="TOther"/> asks which <typeparamref name="T"/> objects depend on it,
    /// and queues the rule for each:
    /// <code>
    /// config.Rules.When&lt;Note&gt;()
    ///     .Maintain(ProjectNoteView)
    ///     .RefreshWhen&lt;Actor&gt;(actor =&gt; note =&gt; note.AuthorId == actor.Username);
    /// </code>
    /// The objects are found through the search index when it can express the condition
    /// (<c>[Queryable]</c> properties compared with values of the other object), and by reading
    /// every object when it cannot. The refresh always runs in the background, also for a rule
    /// that otherwise runs inside the save.
    /// </summary>
    /// <typeparam name="TOther">The type the projection reads.</typeparam>
    /// <param name="sources">Given the object that changed, a condition that is true for the objects whose projection reads it.</param>
    public MaintainBuilder<T> RefreshWhen<TOther>(Func<TOther, Expression<Func<T, bool>>> sources) where TOther : class, new()
    {
        ArgumentNullException.ThrowIfNull(sources);
        _config.ThrowIfOpened();
        var rule = _rule;
        _config.OnRegistrations.Add(new OnRegistration(typeof(TOther),
            (EntityHandler<TOther>)((other, _, db, cancellationToken) => db.RefreshSourcesAsync(rule, sources(other), cancellationToken))));
        return this;
    }
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

/// <summary>The kinds of queued action, in the order a run does them.</summary>
internal enum RuleVerb
{
    /// <summary>Changes the object itself.</summary>
    Update = 0,

    /// <summary>Keeps other objects in step with the object.</summary>
    Maintain = 1,

    /// <summary>Does work outside the database.</summary>
    Then = 2,
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

    /// <summary>The values named by <c>Changed(...)</c> in the filter: an <c>Update</c> rule's inputs.</summary>
    public Func<object, object?>[] Inputs { get; init; } = [];

    /// <summary>Whether the filter uses <c>Changed(...)</c>.</summary>
    public bool UsesChanged { get; init; }

    /// <summary>For a <c>Maintain</c> rule: the objects it keeps for one source object. Null for the other verbs.</summary>
    public Func<object, RuleContext, CancellationToken, Task<List<object>>>? Project { get; init; }

    /// <summary>How long after the save the rule runs (<c>After</c>); null for as soon as it can.</summary>
    public TimeSpan? After { get; set; }

    /// <summary>The time, taken from the object, at which the rule runs (<c>At</c>); null when the rule has none.</summary>
    public Func<object, DateTimeOffset?>? At { get; set; }

    /// <summary>
    /// For a <c>Maintain</c> rule: whether it runs inside the save (the default) rather than from
    /// the queue (<c>.Queued()</c>). Either way a refresh or a backfill runs it from the queue.
    /// </summary>
    public bool Inline { get; set; }
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
