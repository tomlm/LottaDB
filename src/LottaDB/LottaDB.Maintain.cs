using System.Linq.Expressions;
using System.Text.Json;
using Lotta.Internal;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Lotta;

// Maintain rules (When<T>().Maintain(projection)): objects kept in step with a source object. Each
// produced object is marked with the rule and the source's key in the _source field, stored and
// indexed beside it, so one index query finds everything a source owns. A run compares what the
// projection returns with what the source owns, and saves and deletes the difference.
public partial class LottaDB
{
    /// <summary>How the last refresh found the objects to run again: "index" or "scan". For tests.</summary>
    internal string? LastRefreshPath { get; private set; }

    /// <summary>What marks an object as produced by a rule from one source object: <c>Note/note-view n42</c>.</summary>
    private static string SourceStamp(string ruleId, string sourceKey) => $"{ruleId} {sourceKey}";

    /// <summary>
    /// The part a <c>Maintain</c> rule has in every save and delete of the type it is for. On a
    /// save, runs the projection if the rule runs inline. On a delete, deletes what the object
    /// produced, however the rule runs. Called inside the save or delete, which holds the object's
    /// lock, so the result is part of its changes and errors.
    /// </summary>
    internal Task MaintainInSaveAsync(QueuedRule rule, object entity, TriggerKind kind, CancellationToken cancellationToken)
    {
        var key = AsyncKeyOf(entity);
        if (kind == TriggerKind.Deleted)
            return MaintainAsync(rule, entity, key, attempt: 1, etag: null, inScope: false, cancellationToken);
        if (!rule.Inline)
            return Task.CompletedTask;

        bool inScope;
        if (rule.OnSave is not { } onSave || onSave(entity))
            inScope = true;
        else if (rule.Rest is { } rest && !rest(entity))
            inScope = false;   // the rule is no longer for this object: what it produced goes
        else
            return Task.CompletedTask;   // only a Changed(...) part is false: nothing to do in this save
        return MaintainAsync(rule, entity, key, attempt: 1, etag: null, inScope, cancellationToken);
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
        var here = stored != null && stored.GetType() == entity.GetType();
        var inScope = here && (rule.Rest is not { } rest || rest(stored!));
        await MaintainAsync(rule, here ? stored! : entity, key, attempt, etag, inScope, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Brings what one source object owns under a rule in line with what its projection returns
    /// now: saves what is new or different, deletes what is no longer produced. The caller holds
    /// the source's lock.
    /// </summary>
    /// <param name="inScope">False to produce nothing, which deletes everything the source owns: it was deleted, or the rule's filter is no longer true for it.</param>
    private async Task MaintainAsync(QueuedRule rule, object source, string sourceKey, int attempt, string? etag, bool inScope, CancellationToken cancellationToken)
    {
        var stamp = SourceStamp(rule.Id, sourceKey);
        var produced = new Dictionary<string, object>(StringComparer.Ordinal);
        if (inScope)
        {
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
        }

        var owned = OwnedBy(stamp);
        // Inside a save, what goes wrong in these writes is already part of the save's errors.
        var isRoot = _chainChanges.Value == null;
        var failures = new List<Exception>();

        foreach (var (key, view) in produced)
        {
            var json = JsonSerializer.Serialize(view, view.GetType());
            if (owned.TryGetValue(key, out var existing))
            {
                if (Same(existing, view, json))
                    continue;
            }
            else
            {
                // Not known to the index as this source's: see what, if anything, is stored there.
                var (stored, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken).ConfigureAwait(false);
                var owner = stored?.GetSource();
                if (owner == stamp)
                {
                    if (Same(stored!, view, json))
                        continue;
                }
                else if (owner != null && _config.QueuedRules.Any(r => r.Id == owner[..owner.IndexOf(' ')]))
                {
                    throw new InvalidOperationException(
                        $"Rule '{rule.Id}' produced '{key}' for '{sourceKey}', but that object belongs to '{owner}'. An object has one owner: give the two different keys.");
                }
                // Otherwise it is taken over: an object saved by hand, or left by a rule that no longer exists.
            }

            view.SetSource(stamp);
            var saved = await SaveAsync(view, cancellationToken).ConfigureAwait(false);
            if (isRoot)
                failures.AddRange(saved.Errors);
        }

        foreach (var key in owned.Keys)
        {
            if (produced.ContainsKey(key))
                continue;
            var deleted = await DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            if (isRoot)
                failures.AddRange(deleted.Errors);
        }

        if (failures.Count > 0)
            throw new AggregateException($"Rule '{rule.Id}' wrote what '{sourceKey}' produces, but rules of the objects written failed.", failures);

        static bool Same(object existing, object view, string json) =>
            existing.GetType() == view.GetType() && JsonSerializer.Serialize(existing, existing.GetType()) == json;
    }

    /// <summary>
    /// The objects marked as produced from one source by one rule, by key: one query of the search
    /// index, through the index writer the caller's write lease holds. That reader sees every
    /// write up to now, committed or not, by this process and (since the lease is the only one)
    /// by any other, and it costs no commit.
    /// </summary>
    private Dictionary<string, object> OwnedBy(string stamp)
    {
        var owned = new Dictionary<string, object>(StringComparer.Ordinal);
        using var reader = DirectoryReader.Open(RequireWriter(), applyAllDeletes: true);
        var searcher = new IndexSearcher(reader);
        var hits = searcher.Search(new TermQuery(new Term(StorageFields.Source, stamp)), Math.Max(1, reader.MaxDoc));
        foreach (var hit in hits.ScoreDocs)
        {
            var document = searcher.Doc(hit.Doc);
            if (document.Get(StorageFields.Key) is { } key)
                owned[key] = EntityMapper.FromLuceneDocument(document, this);
        }
        return owned;
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
