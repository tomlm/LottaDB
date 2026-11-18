using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using Lotta.Internal;

namespace Lotta;

// Maintain rules (When<T>().Maintain(projection)): objects kept in step with a source object.
//
// The source's row remembers what it produced: a _produced value beside the object (not in it)
// lists, per rule, the keys of the objects made from it. Each produced object's row is marked the
// other way with _source, the rule and the source's key. A run compares the keys the projection
// returns now with the keys the row lists, reads those few rows by key, and saves and deletes the
// difference. Nothing is looked up through the search index.
public partial class LottaDB
{
    /// <summary>What one rule is to write for one source object, worked out before the source is written and applied after.</summary>
    internal sealed class PendingViews
    {
        /// <summary>What the projection returned, by key.</summary>
        public Dictionary<string, object> Views = new(StringComparer.Ordinal);

        /// <summary>The keys the source produced under the rule before.</summary>
        public string[] Previous = [];

        /// <summary>What the filter or the projection threw; reported where the rule's other errors are.</summary>
        public Exception? Error;
    }

    private readonly ConcurrentDictionary<Type, QueuedRule[]> _maintainRulesByType = new();

    /// <summary>Up to this many rows are read by key one at a time; more are read with one query per hundred.</summary>
    private const int MaxPointReads = 4;

    /// <summary>How the last refresh found the objects to run again: "index" or "scan". For tests.</summary>
    internal string? LastRefreshPath { get; private set; }

    /// <summary>What marks an object as produced by a rule from one source object: <c>Note/note-view n42</c>.</summary>
    private static string SourceStamp(string ruleId, string sourceKey) => $"{ruleId} {sourceKey}";

    /// <summary>The <c>Maintain</c> rules that apply to a concrete type.</summary>
    private QueuedRule[] MaintainRulesFor(Type type) =>
        _maintainRulesByType.GetOrAdd(type, t => [.. QueuedRulesFor(t).Where(r => r.Verb == RuleVerb.Maintain)]);

    /// <summary>
    /// Before a source object's row is written: runs the projections of the rules that run inside
    /// the save, and puts on the object the list of keys its row is to carry, so the one write
    /// records it. What is to be saved and deleted for it is kept on the object until
    /// <see cref="MaintainInSaveAsync"/> applies it, after the write. Called under the object's lock.
    /// </summary>
    /// <param name="known">What the stored row lists, when the caller has just read it; null to find out here.</param>
    private async Task PrepareMaintainAsync(object entity, string key, IReadOnlyDictionary<string, string[]>? known, CancellationToken cancellationToken)
    {
        if (entity is JsonDocument)
            return;
        var rules = MaintainRulesFor(entity.GetType());
        if (rules.Length == 0)
            return;

        // An object with an ETag is written only if it is still the stored version, so what it was loaded with is what is stored.
        var previous = known ?? (entity.GetETag() != null ? entity.GetProduced() : null);
        if (previous == null)
        {
            var (stored, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken).ConfigureAwait(false);
            previous = stored?.GetProduced() ?? ObjectMetadata.NothingProduced;
        }

        Dictionary<string, string[]>? next = null;
        Dictionary<string, PendingViews>? pending = null;
        foreach (var rule in rules)
        {
            // A rule that runs from the queue leaves the list as it is until its run.
            if (!rule.Inline)
                continue;
            var plan = new PendingViews { Previous = previous.GetValueOrDefault(rule.Id) ?? [] };
            try
            {
                if (rule.OnSave is not { } onSave || onSave(entity))
                    plan.Views = await ProjectAsync(rule, entity, key, attempt: 1, etag: null, cancellationToken).ConfigureAwait(false);
                else if (!(rule.Rest is { } rest && !rest(entity)))
                    continue;   // only a Changed(...) part is false: nothing to do in this save
                // Otherwise the rule is no longer for this object: it produces nothing, so what it produced goes.
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                // The save still happens, with what the rule produced before left as it is.
                (pending ??= new(StringComparer.Ordinal))[rule.Id] = new PendingViews { Error = e };
                continue;
            }
            (pending ??= new(StringComparer.Ordinal))[rule.Id] = plan;
            SetKeys(ref next, previous, rule.Id, plan);
        }
        entity.SetProduced(next ?? previous);
        entity.SetPendingMaintain(pending);
    }

    /// <summary>Notes in <paramref name="next"/> the keys a rule now produces, when they differ from the ones listed before.</summary>
    private static void SetKeys(ref Dictionary<string, string[]>? next, IReadOnlyDictionary<string, string[]> previous, string ruleId, PendingViews plan)
    {
        var keys = plan.Views.Keys.ToArray();
        if (plan.Previous.SequenceEqual(keys, StringComparer.Ordinal))
            return;
        next ??= new Dictionary<string, string[]>(previous, StringComparer.Ordinal);
        if (keys.Length == 0)
            next.Remove(ruleId);
        else
            next[ruleId] = keys;
    }

    /// <summary>Runs a rule's projection for one source object and checks what it returned.</summary>
    private async Task<Dictionary<string, object>> ProjectAsync(QueuedRule rule, object source, string sourceKey, int attempt, string? etag, CancellationToken cancellationToken)
    {
        var produced = new Dictionary<string, object>(StringComparer.Ordinal);
        var context = new RuleContext(this, rule.Name, rule.Id, attempt, source, etag);
        foreach (var view in await rule.Project!(source, context, cancellationToken).ConfigureAwait(false))
        {
            if (rule.ObjectType.IsInstanceOfType(view))
                throw new InvalidOperationException(
                    $"Rule '{rule.Id}' produced a {view.GetType().Name}: a projection cannot produce objects of the type it is for.");
            var meta = GetMeta(view.GetType());
            var key = meta.GetKey(view);
            if (meta.KeyMode == KeyMode.Auto && meta.SetKey != null)
                meta.SetKey(view, key);
            if (!produced.TryAdd(key, view))
                throw new InvalidOperationException($"Rule '{rule.Id}' produced two objects with the key '{key}' for '{sourceKey}'.");
        }
        return produced;
    }

    /// <summary>
    /// The part a <c>Maintain</c> rule has in every save and delete of the type it is for, after
    /// the row is written. On a save, applies what <see cref="PrepareMaintainAsync"/> worked out.
    /// On a delete, deletes what the object's row listed for the rule, however the rule runs.
    /// Called inside the save or delete, which holds the object's lock, so the result is part of
    /// its changes and errors.
    /// </summary>
    internal Task MaintainInSaveAsync(QueuedRule rule, object entity, TriggerKind kind, CancellationToken cancellationToken)
    {
        var key = AsyncKeyOf(entity);
        if (kind == TriggerKind.Deleted)
        {
            var keys = entity.GetProduced()?.GetValueOrDefault(rule.Id);
            return keys is null or { Length: 0 }
                ? Task.CompletedTask
                : ApplyMaintainAsync(rule, key, new PendingViews { Previous = keys }, cancellationToken);
        }

        if (entity.GetPendingMaintain() is not { } pending || !pending.Remove(rule.Id, out var plan))
            return Task.CompletedTask;
        return plan.Error != null ? Task.FromException(plan.Error) : ApplyMaintainAsync(rule, key, plan, cancellationToken);
    }

    /// <summary>
    /// Runs a <c>Maintain</c> rule from the queue. The object is read again under its lock, so the
    /// projection sees what is stored and no save of the object can come between the projection
    /// and the writes: a refresh can then never overwrite the result of a newer save.
    /// </summary>
    private async Task RunMaintainAsync(QueuedRule rule, object entity, int attempt, string? etag, CancellationToken cancellationToken)
    {
        var key = AsyncKeyOf(entity);
        using var writerLease = await AcquireWriterAsync(cancellationToken).ConfigureAwait(false);
        // Taken here, not in a helper: the saves below must find the lock held by their own call chain.
        using var keyLock = await AcquireLockAsync(key, cancellationToken).ConfigureAwait(false);

        var stored = await GetAsync(key, cancellationToken).ConfigureAwait(false);
        // Deleted since (its delete removed what it produced), or replaced by an object of another type.
        if (stored == null || stored.GetType() != entity.GetType())
            return;

        var previous = stored.GetProduced() ?? ObjectMetadata.NothingProduced;
        var plan = new PendingViews { Previous = previous.GetValueOrDefault(rule.Id) ?? [] };
        // Outside the rule's filter the object produces nothing, so what it produced goes.
        if (rule.Rest is not { } rest || rest(stored))
            plan.Views = await ProjectAsync(rule, stored, key, attempt, etag, cancellationToken).ConfigureAwait(false);

        Dictionary<string, string[]>? next = null;
        SetKeys(ref next, previous, rule.Id, plan);
        if (next != null)
        {
            // The list of keys changed: the object's row is written again to record it, with the object as it is.
            stored.SetProduced(next);
            await RewriteRowAsync(stored, key, cancellationToken).ConfigureAwait(false);
        }
        await ApplyMaintainAsync(rule, key, plan, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes an object's row again with the object unchanged, to record what is kept beside it.
    /// The row gets a new ETag, which the search index is given too; no rule runs and nothing is queued.
    /// </summary>
    private async Task RewriteRowAsync(object stored, string key, CancellationToken cancellationToken)
    {
        string etag;
        try
        {
            etag = await _tableAdapter.ReplaceAsync(_lottaCatalog.Name, key, stored, GetMeta(stored.GetType()), stored.GetETag()!, cancellationToken).ConfigureAwait(false);
        }
        catch (Azure.RequestFailedException e) when (e.Status == 412)
        {
            throw new ConcurrencyException(key, stored.GetType());
        }
        EntityMapper.AnnotateAfterWrite(stored, key, etag);
        var errors = new List<Exception>();
        await RunHandlersAsync(stored, stored.GetType(), TriggerKind.Saved, errors, cancellationToken, builtInOnly: true).ConfigureAwait(false);
        if (errors.Count > 0)
            throw new AggregateException($"'{key}' was written, but could not be indexed.", errors);
        // The rules of this run that come next are given the object as it now is.
        if (_asyncRun.Value is { } run && run.Db == this && run.Key == key)
            run.Dirty = true;
    }

    /// <summary>
    /// Brings the stored objects in line with what one rule produces for one source object: saves
    /// what is new or different, and deletes what the source produced before and no longer does.
    /// The rows concerned are read by key, in one batch, and written in batches. The caller holds
    /// the source's lock.
    /// </summary>
    private async Task ApplyMaintainAsync(QueuedRule rule, string sourceKey, PendingViews plan, CancellationToken cancellationToken)
    {
        var stamp = SourceStamp(rule.Id, sourceKey);
        var gone = plan.Previous.Where(key => !plan.Views.ContainsKey(key)).ToList();
        if (plan.Views.Count == 0 && gone.Count == 0)
            return;

        var stored = new Dictionary<string, object>(StringComparer.Ordinal);
        var keys = plan.Views.Keys.Concat(gone).ToList();
        if (keys.Count <= MaxPointReads)
        {
            // A few rows are read quicker one by one than with a query.
            foreach (var key in keys)
            {
                var (found, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken).ConfigureAwait(false);
                if (found != null)
                    stored[key] = found;
            }
        }
        else
        {
            await foreach (var found in _tableAdapter.GetManyAsync(_lottaCatalog.Name, keys, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (found?.GetKey() is { } key)
                    stored[key] = found;
            }
        }

        var changed = new List<object>();
        foreach (var (key, view) in plan.Views)
        {
            if (stored.TryGetValue(key, out var existing))
            {
                var owner = existing.GetSource();
                if (owner == stamp)
                {
                    if (existing.GetType() == view.GetType()
                        && JsonSerializer.Serialize(existing, existing.GetType()) == JsonSerializer.Serialize(view, view.GetType()))
                        continue;
                }
                else if (owner != null && owner.IndexOf(' ') is > 0 and var space && _config.QueuedRules.Any(r => r.Id == owner[..space]))
                {
                    throw new InvalidOperationException(
                        $"Rule '{rule.Id}' produced '{key}' for '{sourceKey}', but that object belongs to '{owner}'. An object has one owner: give the two different keys.");
                }
                // Otherwise it is taken over: an object saved by hand, or left by a rule that no longer exists.
            }
            view.SetSource(stamp);
            changed.Add(view);
        }

        // Inside a save, what these writes change and what goes wrong in them is part of that save's result.
        var chainChanges = _chainChanges.Value;
        var chainErrors = _chainErrors.Value;
        var failures = new List<Exception>();
        void Note(ObjectResult result)
        {
            chainChanges?.AddRange(result.Changes);
            (chainErrors ?? failures).AddRange(result.Errors);
        }

        if (changed.Count > 0)
            Note(await SaveManyAsync(changed, cancellationToken).ConfigureAwait(false));

        // Only what is still this source's: a key it lists but another object has since taken is left alone.
        var deletes = gone.Where(key => stored.TryGetValue(key, out var existing) && existing.GetSource() == stamp)
            .Select(key => ((object?)stored[key], (Type?)stored[key].GetType(), key)).ToList();
        if (deletes.Count > 0)
            Note(await DeleteManyAsyncCore(deletes.ToAsyncEnumerable(), cancellationToken).ConfigureAwait(false));

        if (failures.Count > 0)
            throw new AggregateException($"Rule '{rule.Id}' wrote what '{sourceKey}' produces, but rules of the objects written failed.", failures);
    }

    /// <summary>
    /// Before an object is deleted: makes sure the object in hand says what its row lists as
    /// produced, so the delete can remove it. An object that was not loaded is read first, and
    /// only for types that have <c>Maintain</c> rules.
    /// </summary>
    private async Task NoteProducedAsync(object entity, string key, CancellationToken cancellationToken)
    {
        if (entity is JsonDocument || entity.GetProduced() != null || MaintainRulesFor(entity.GetType()).Length == 0)
            return;
        var (stored, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken).ConfigureAwait(false);
        entity.SetProduced(stored?.GetProduced() ?? ObjectMetadata.NothingProduced);
    }

    /// <summary>
    /// Queues a <c>Maintain</c> rule for every source object a condition is true for: the part
    /// <c>RefreshWhen</c> has in every save and delete of the type the projection reads. The
    /// objects are found through the search index when it can express the condition, and by
    /// reading every object when it cannot.
    /// </summary>
    internal async Task RefreshSourcesAsync<T>(QueuedRule rule, Expression<Func<T, bool>> condition, CancellationToken cancellationToken)
        where T : class, new()
    {
        if (_config.ReadOnly)
            return;
        var isTrueFor = condition.Compile();
        List<T>? sources = null;
        try
        {
            sources = [.. Search<T>().Where(condition)];
            LastRefreshPath = "index";
        }
        catch (Exception e) when (e is not OperationCanceledException and not ObjectDisposedException)
        {
            // Not something the index can answer; read everything instead.
        }
        if (sources == null)
        {
            LastRefreshPath = "scan";
            sources = [];
            await foreach (var entity in GetManyAsync(cancellationToken).ConfigureAwait(false))
            {
                if (entity is T source)
                    sources.Add(source);
            }
        }

        var run = _asyncRun.Value;
        var hop = run is null ? 0 : run.Hop + 1;
        if (hop > _config.MaxQueueHops)
            throw new InvalidOperationException(
                $"Rule '{rule.Id}' was not refreshed: {hop} queued rules in a row led to this save, more than MaxQueueHops ({_config.MaxQueueHops}).");

        var queued = false;
        foreach (var source in sources)
        {
            // Checked on every candidate: the index may return more than the condition is true for.
            if (!isTrueFor(source))
                continue;
            await QueueRuleAsync(source, rule.Id, hop, cancellationToken).ConfigureAwait(false);
            queued = true;
        }
        if (queued)
            Signal();
    }
}
