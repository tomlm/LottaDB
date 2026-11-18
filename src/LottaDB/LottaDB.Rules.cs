using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Lotta.Internal;

namespace Lotta;

// Queued rules (When<T>().Update(...), .Then(...), and .Maintain(...) when it runs from the queue): a save sends one small message to the queue of
// the object's type, listing the rules the save matched, and a pump runs them later, outside the
// write lease, retrying each rule until it succeeds. The queues are the only stored state; see
// Rules-Next.md.
public partial class LottaDB
{
    /// <summary>The queue of one concrete object type, with the queue its failed work is set aside in.</summary>
    private sealed class TypeQueue(string typeName, Type? type, QueueClient queue, QueueClient poison, QueuedRule[] rules)
    {
        /// <summary>The full name of the type, as the queue records it.</summary>
        public string TypeName { get; } = typeName;

        /// <summary>The type; null when this program has no type of that name.</summary>
        public Type? Type { get; } = type;

        public QueueClient Queue { get; } = queue;
        public QueueClient Poison { get; } = poison;

        /// <summary>The queued rules that apply to the type, in the order a run does them.</summary>
        public QueuedRule[] Rules { get; } = rules;

        /// <summary>Whether no queued rule is for the type any more: its work is listed, but not run.</summary>
        public bool Orphaned => Rules.Length == 0;
    }

    /// <summary>What this process knows about one queued rule.</summary>
    private sealed class RuleState
    {
        public int Running;
        public volatile string? LastError;
    }

    /// <summary>The run of one message that the current call chain is inside, if any.</summary>
    private sealed class AsyncRun(LottaDB db, int hop, string key)
    {
        public LottaDB Db { get; } = db;

        /// <summary>How many queued rules led to the message being run.</summary>
        public int Hop { get; } = hop;

        /// <summary>The key of the object the message is for.</summary>
        public string Key { get; } = key;

        /// <summary>The rule now running.</summary>
        public string RuleId = "";

        /// <summary>The rules this run has yet to do, after the one now running.</summary>
        public HashSet<string> Remaining = [];

        /// <summary>
        /// Set when the object was saved during the run (by an <c>Update</c> rule's write, or by a
        /// handler), so the next rule is given the object as it now is.
        /// </summary>
        public volatile bool Dirty;
    }

    /// <summary>How the run of one message went.</summary>
    private sealed class MessageRun
    {
        /// <summary>The rules not yet done, in the order they are run.</summary>
        public List<string> Pending = [];

        public Dictionary<string, Exception> Failed = new(StringComparer.Ordinal);

        /// <summary>Stopped early through no fault of a rule: closing, a cancelled drain, or the drain's limit.</summary>
        public bool Interrupted;

        /// <summary>Stopped because another process holds the write lock.</summary>
        public bool Contended;

        /// <summary>The object is gone, or is no longer of this queue's type: nothing is left to do.</summary>
        public bool Gone;

        /// <summary>How many rules were run, failed runs included.</summary>
        public int Runs;

        /// <summary>Set when a rule was left for later because its object now gives it a later time (<c>At</c>): the earliest such time.</summary>
        public DateTimeOffset? DeferUntil;
    }

    private static readonly TimeSpan MaxLease = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan NeverExpires = TimeSpan.FromSeconds(-1);
    private const int MaxUpdateWrites = 10;
    /// <summary>The longest a queue hides a message for. A longer wait is made in steps.</summary>
    internal static readonly TimeSpan MaxHide = TimeSpan.FromDays(7);

    /// <summary>How long to hide a message that is to run at <paramref name="due"/>; null when that time has come.</summary>
    internal static TimeSpan? HideFor(DateTimeOffset due, DateTimeOffset now) =>
        due <= now ? null : (due - now < MaxHide ? due - now : MaxHide);

    private static readonly AsyncLocal<AsyncRun?> _asyncRun = new();

    private readonly ConcurrentDictionary<Type, TypeQueue> _queueByType = new();
    private readonly ConcurrentDictionary<Type, QueuedRule[]> _rulesByType = new();
    private readonly ConcurrentDictionary<string, RuleState> _ruleStates = new(StringComparer.Ordinal);
    private readonly object _typeQueuesLock = new();
    /// <summary>Every known type queue, orphaned ones included. Replaced, never changed, when one is added.</summary>
    private volatile TypeQueue[] _typeQueues = [];
    private int _nextQueue;

    /// <summary>Per object key, the end of the last run started for it: the next run for the key waits for it.</summary>
    private readonly Dictionary<string, Task> _keyTails = new(StringComparer.Ordinal);

    /// <summary>Caps how many messages are worked on at once, whoever started them (the loop or a drain).</summary>
    private SemaphoreSlim? _asyncSlots;
    private readonly SemaphoreSlim _asyncSignal = new(0);
    private readonly CancellationTokenSource _asyncCts = new();
    private Task? _asyncLoop;

    private readonly ConcurrentDictionary<Type, bool> _tracksChanges = new();
    private readonly ConcurrentDictionary<Type, byte> _liveChangedTypes = new();

    private int MaxConcurrency => Math.Max(1, _config.MaxConcurrency);

    private SemaphoreSlim AsyncSlots =>
        LazyInitializer.EnsureInitialized(ref _asyncSlots, () => new SemaphoreSlim(MaxConcurrency, MaxConcurrency));

    private bool HasQueuedRules => _config.QueuedRules.Count > 0 && !_config.ReadOnly;

    /// <summary>The queued rules that apply to a concrete type, in the order a run does them: by phase, then as registered.</summary>
    private QueuedRule[] QueuedRulesFor(Type type) =>
        _rulesByType.GetOrAdd(type, t =>
            [.. _config.QueuedRules.Where(r => r.ObjectType.IsAssignableFrom(t)).OrderBy(r => (int)r.Verb).ThenBy(r => r.Order)]);

    // === Changed(...) ===

    /// <summary>Makes saves of a type note the version they replace, for a rule added to the open database that uses <c>Changed</c>.</summary>
    internal void TrackChanges(Type type)
    {
        _liveChangedTypes[type] = 0;
        _tracksChanges.Clear();
    }

    /// <summary>Whether any rule for the type (or a type it derives from) uses <c>Changed</c>.</summary>
    private bool TracksChanges(Type concrete) =>
        (_config.ChangedTypes.Count > 0 || !_liveChangedTypes.IsEmpty)
        && _tracksChanges.GetOrAdd(concrete, type =>
            _config.ChangedTypes.Any(c => c.IsAssignableFrom(type)) || _liveChangedTypes.Keys.Any(c => c.IsAssignableFrom(type)));

    /// <summary>
    /// Before a save's write: notes on the object the version the save replaces, so the rules'
    /// <c>Changed(...)</c> can compare the two. Only for types that have such rules. An object that
    /// was loaded (or saved before) is compared with what it then was; one that was not is
    /// compared with the stored version, read here. <see cref="ForgetPrevious"/> ends it.
    /// </summary>
    /// <param name="jsonBefore">The object's JSON when it was last loaded or saved; null when it never was.</param>
    private async Task NotePreviousAsync(object entity, string key, string? jsonBefore, CancellationToken cancellationToken)
    {
        if (entity is JsonDocument || !TracksChanges(entity.GetType()))
            return;
        if (jsonBefore == null)
        {
            var (stored, _) = await _tableAdapter.GetAsync(_lottaCatalog.Name, key, cancellationToken: cancellationToken).ConfigureAwait(false);
            // Stored as another type: nothing to compare with, so the object counts as new.
            if (stored != null && stored.GetType() == entity.GetType())
                jsonBefore = stored.GetJson();
        }
        entity.SetPrevious(jsonBefore);
    }

    /// <summary>After a save's rules have been checked: drops the note of the version it replaced.</summary>
    private static void ForgetPrevious(object entity)
    {
        if (entity is not JsonDocument)
            entity.SetPrevious(null);
    }

    // === Opening and closing ===

    /// <summary>
    /// Finds the type queues left by earlier runs of the program, so their work is run. Called by
    /// the catalog as the last step of opening the database; does nothing for a read-only
    /// database or one without queued rules. A queue for a type first saved in this run is made
    /// when that save happens. <see cref="StartRules"/> then starts running them.
    /// </summary>
    internal async Task PrepareRulesAsync(CancellationToken cancellationToken)
    {
        if (!HasQueuedRules || _typeQueues.Length > 0)
            return;
        var queues = _lottaCatalog.GetQueueServiceClient()
            ?? throw new NotSupportedException($"Rules that run from a queue (.Update, .Maintain, .Then) need queue storage, which {_lottaCatalog.StorageProvider} storage does not provide.");

        var names = new List<string>();
        var prefix = RuleQueueNames.DatabasePrefix(_lottaCatalog.Name, _databaseId);
        await foreach (var found in queues.GetQueuesAsync(prefix: prefix, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (!found.Name.EndsWith(RuleQueueNames.PoisonSuffix, StringComparison.Ordinal))
                names.Add(found.Name);
        }

        foreach (var name in names)
        {
            var queue = queues.GetQueueClient(name);
            string? typeName = null;
            try
            {
                var about = (await queue.GetPropertiesAsync(cancellationToken).ConfigureAwait(false)).Value.Metadata;
                var entry = about?.FirstOrDefault(m => string.Equals(m.Key, RuleQueueNames.TypeMetadata, StringComparison.OrdinalIgnoreCase));
                if (entry?.Value is { Length: > 0 } recorded)
                    typeName = Uri.UnescapeDataString(recorded);
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                continue;   // deleted since it was listed
            }

            var type = TypeUtils.ResolveType(typeName);
            var poison = queues.GetQueueClient(RuleQueueNames.Poison(name));
            await poison.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            // A queue that does not say whose it is, or is for a type no rule is for now, is kept and shown, but not run.
            AddTypeQueue(new TypeQueue(typeName ?? name, type, queue, poison, type == null ? [] : QueuedRulesFor(type)));
        }
    }

    private TypeQueue AddTypeQueue(TypeQueue queue)
    {
        lock (_typeQueuesLock)
        {
            if (queue.Type != null)
            {
                if (_queueByType.TryGetValue(queue.Type, out var known))
                    return known;
                _queueByType[queue.Type] = queue;
            }
            _typeQueues = [.. _typeQueues, queue];
            return queue;
        }
    }

    /// <summary>The queue of a concrete type, made the first time work is queued for the type.</summary>
    private async Task<TypeQueue> TypeQueueAsync(Type type, CancellationToken cancellationToken)
    {
        if (_queueByType.TryGetValue(type, out var known))
            return known;
        var queues = _lottaCatalog.GetQueueServiceClient()
            ?? throw new NotSupportedException($"Rules that run from a queue (.Update, .Maintain, .Then) need queue storage, which {_lottaCatalog.StorageProvider} storage does not provide.");

        var name = RuleQueueNames.Queue(_lottaCatalog.Name, _databaseId, type);
        var queue = queues.GetQueueClient(name);
        var poison = queues.GetQueueClient(RuleQueueNames.Poison(name));
        var typeName = type.FullName ?? type.Name;
        // The names are hashes; say in words whose queue this is. The type is also how the queue is recognised when the database is next opened.
        var about = new Dictionary<string, string>
        {
            ["catalog"] = Uri.EscapeDataString(_lottaCatalog.Name),
            ["database"] = Uri.EscapeDataString(_databaseId),
            [RuleQueueNames.TypeMetadata] = Uri.EscapeDataString(typeName),
        };
        await queue.CreateIfNotExistsAsync(about, cancellationToken).ConfigureAwait(false);
        await poison.CreateIfNotExistsAsync(about, cancellationToken).ConfigureAwait(false);
        return AddTypeQueue(new TypeQueue(typeName, type, queue, poison, QueuedRulesFor(type)));
    }

    /// <summary>
    /// Unless queued rules are run by hand, starts running them. Called by the catalog once it
    /// has kept the fully open database, and only then: a database it does not keep (the catalog
    /// was disposed or deleted meanwhile) must run no rule.
    /// </summary>
    internal void StartRules()
    {
        if (_config.QueueMode != QueueMode.Auto)
            return;
        // Started without the caller's context, so nothing of a save chain leaks into the loops.
        using (ExecutionContext.SuppressFlow())
        {
            if (HasQueuedRules && _asyncLoop == null)
                _asyncLoop = Task.Run(() => RuleLoopAsync(_asyncCts.Token));
            if (HasSchedules && _scheduleLoop == null)
                _scheduleLoop = Task.Run(() => ScheduleLoopAsync(_asyncCts.Token));
        }
    }

    /// <summary>
    /// Stops running queued rules. A run in progress, the background loop's or a
    /// <see cref="DrainAsync"/>'s, has its token cancelled and its work stays queued with no
    /// failure counted. Waits up to 10 seconds in all for the runs to end, so the database is not
    /// torn down under them.
    /// </summary>
    private void StopAsyncHandlers()
    {
        _asyncCts.Cancel();
        // A handler that disposes its own database cannot wait for itself to finish.
        if (_asyncRun.Value?.Db == this)
            return;
        // One deadline for all the waits, so runs that ignore their token hold Dispose 10 seconds in all.
        var waiting = System.Diagnostics.Stopwatch.StartNew();
        TimeSpan Left() => TimeSpan.FromTicks(Math.Max(0, (TimeSpan.FromSeconds(10) - waiting.Elapsed).Ticks));
        if (_asyncLoop != null)
        {
            try { _asyncLoop.Wait(Left()); } catch { }
        }
        if (_scheduleLoop != null)
        {
            try { _scheduleLoop.Wait(Left()); } catch { }
        }
        // Every run holds a slot until its message is settled.
        if (_asyncSlots is { } slots)
        {
            var held = 0;
            while (held < MaxConcurrency && slots.Wait(Left()))
                held++;
            if (held > 0)
                slots.Release(held);
        }
    }

    // === Queuing ===

    /// <summary>
    /// Queues the rules a save matched: one message, to the queue of the object's type, listing
    /// them. Called on each save path after the table write and the inline rules, so the index is
    /// up to date before a queued rule can see the object. A filter that throws, or a queue that
    /// cannot be written to, is reported in <paramref name="errors"/>; the save itself has already
    /// happened.
    /// </summary>
    private async Task EnqueueAsync(object entity, string key, List<Exception> errors, CancellationToken cancellationToken)
    {
        if (!HasQueuedRules || entity is JsonDocument)
            return;
        var type = entity.GetType();
        var rules = QueuedRulesFor(type);
        if (rules.Length == 0)
            return;

        // A save of the object a run is for, made inside that run (an Update rule's write, or a
        // handler saving the object it was given): the rule that made it is not queued again, which
        // is what keeps a rule from triggering itself, and neither are the rules the run has yet
        // to do, since they are about to be given the object as saved here.
        var run = _asyncRun.Value;
        var own = run != null && run.Db == this && run.Key == key ? run : null;
        if (own != null)
            own.Dirty = true;

        var matched = new List<string>();
        // Delayed rules (After, At), each with the time it is to run.
        var later = new List<(string Rule, DateTimeOffset Due)>();
        var now = DateTimeOffset.UtcNow;
        foreach (var rule in rules)
        {
            if (own != null && (rule.Id == own.RuleId || own.Remaining.Contains(rule.Id)))
                continue;
            // A Maintain rule that runs inside the save has just run.
            if (rule.Inline)
                continue;
            try
            {
                // Not for this save: nothing to queue, run or read back. Except that a Maintain
                // rule which is no longer for the object at all has what it produced to delete.
                if (rule.OnSave is { } onSave && !onSave(entity)
                    && !(rule.Verb == RuleVerb.Maintain && rule.Rest is { } rest && !rest(entity)))
                    continue;
                if (rule.After is { } after)
                {
                    later.Add((rule.Id, now + after));
                    continue;
                }
                if (rule.At is { } at)
                {
                    // No time: not at all.
                    if (at(entity) is { } due)
                        later.Add((rule.Id, due));
                    continue;
                }
            }
            catch (Exception e)
            {
                errors.Add(new InvalidOperationException($"'{key}' was saved, but the filter of rule '{rule.Id}' failed: {e.Message}", e));
                continue;
            }
            matched.Add(rule.Id);
        }
        if (matched.Count == 0 && later.Count == 0)
            return;

        var hop = run is null ? 0 : run.Hop + 1;
        try
        {
            var queue = await TypeQueueAsync(type, cancellationToken).ConfigureAwait(false);
            if (hop > _config.MaxQueueHops)
            {
                // A chain this long is rules feeding one another; stop it where it can be seen.
                foreach (var rule in matched.Concat(later.Select(l => l.Rule)))
                {
                    var record = new PoisonRecord
                    {
                        Type = queue.TypeName,
                        Key = key,
                        ETag = entity.GetETag(),
                        Rule = rule,
                        Error = $"Set aside without running: {hop} queued rules in a row led to this save, more than MaxQueueHops ({_config.MaxQueueHops}).",
                    };
                    await queue.Poison.SendMessageAsync(record.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
                }
                return;
            }

            if (matched.Count > 0)
            {
                var message = new RuleMessage { Key = key, ETag = entity.GetETag(), Rules = matched, Hop = hop };
                await queue.Queue.SendMessageAsync(message.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
            }
            // A delayed rule has a message of its own, hidden until its time (or as long as a queue can hide it).
            foreach (var (rule, due) in later)
            {
                var message = new RuleMessage { Key = key, ETag = entity.GetETag(), Rules = [rule], Hop = hop, Due = due };
                await queue.Queue.SendMessageAsync(message.ToText(), HideFor(due, now), NeverExpires, cancellationToken).ConfigureAwait(false);
            }
            Signal();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            errors.Add(new InvalidOperationException($"'{key}' was saved, but its rules could not be queued: {e.Message}", e));
        }
    }

    /// <summary>Wakes the background loop: there is new work.</summary>
    private void Signal()
    {
        if (_asyncSignal.CurrentCount == 0)
            _asyncSignal.Release();
    }

    /// <summary>The key an object was saved or loaded under.</summary>
    private string AsyncKeyOf(object entity) => entity.GetKey() ?? GetMeta(entity.GetType()).GetKey(entity);

    // === Running ===

    private async Task RuleLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await PumpAsync(max: null, forever: true, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (Exception)
            {
                // Queue storage is unreachable for the moment; the messages are still there.
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// Takes messages and runs their rules, several messages at once up to
    /// <see cref="ILottaConfiguration.MaxConcurrency"/>: until nothing is ready and nothing is in
    /// progress, or for ever, waiting for more. With a limit on the number of rule runs, one
    /// message at a time, so the limit can be kept exactly.
    /// </summary>
    /// <returns>How many rules were run, failed runs included.</returns>
    private async Task<int> PumpAsync(int? max, bool forever, CancellationToken cancellationToken)
    {
        var lease = _config.RuleTimeout < MaxLease ? _config.RuleTimeout : MaxLease;
        if (lease < TimeSpan.FromSeconds(1))
            lease = TimeSpan.FromSeconds(1);
        var most = max is null ? MaxConcurrency : 1;
        var slots = AsyncSlots;
        var running = new List<Task<int>>();
        var runs = 0;
        var idle = TimeSpan.FromMilliseconds(100);
        Task<bool>? wake = null;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                for (var n = running.Count - 1; n >= 0; n--)
                {
                    if (!running[n].IsCompleted)
                        continue;
                    var finished = running[n];
                    running.RemoveAt(n);
                    runs += await finished.ConfigureAwait(false);
                }
                if (max is { } limit && runs >= limit)
                    return runs;

                var started = false;
                if (running.Count < most)
                {
                    await slots.WaitAsync(cancellationToken).ConfigureAwait(false);
                    var held = true;
                    try
                    {
                        if (await ReceiveNextAsync(lease, cancellationToken).ConfigureAwait(false) is { } next)
                        {
                            held = false;
                            running.Add(RunInSlotAsync(next.Queue, next.Message, lease, max - runs, cancellationToken));
                            started = true;
                        }
                    }
                    finally
                    {
                        if (held)
                            slots.Release();
                    }
                }
                if (started)
                {
                    idle = TimeSpan.FromMilliseconds(100);
                    continue;
                }

                if (!forever)
                {
                    if (running.Count == 0)
                        return runs;
                    // What is in progress may queue more, or free an object another message waits for.
                    await Task.WhenAny(running).ConfigureAwait(false);
                    continue;
                }

                // Woken at once by a save in this process; otherwise look again for work from other
                // processes and for messages whose wait has run out, less and less often.
                wake ??= _asyncSignal.WaitAsync(idle, cancellationToken);
                await Task.WhenAny([.. running, wake]).ConfigureAwait(false);
                if (wake.IsCompleted)
                {
                    var signalled = await wake.ConfigureAwait(false);
                    wake = null;
                    idle = signalled ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromMilliseconds(Math.Min(idle.TotalMilliseconds * 2, 5000));
                }
            }
        }
        finally
        {
            // Nothing is left running behind the caller's back: each run settles its own message.
            foreach (var run in running)
            {
                try { await run.ConfigureAwait(false); } catch { }
            }
        }

        async Task<int> RunInSlotAsync(TypeQueue queue, QueueMessage message, TimeSpan held, int? budget, CancellationToken token)
        {
            try
            {
                return await RunMessageAsync(queue, message, held, budget, token).ConfigureAwait(false);
            }
            finally
            {
                slots.Release();
            }
        }
    }

    /// <summary>Takes the next ready message, looking at the type queues in turn so none is starved.</summary>
    private async Task<(TypeQueue Queue, QueueMessage Message)?> ReceiveNextAsync(TimeSpan lease, CancellationToken cancellationToken)
    {
        var queues = _typeQueues;
        for (var n = 0; n < queues.Length; n++)
        {
            var queue = queues[(int)((uint)Interlocked.Increment(ref _nextQueue) % (uint)queues.Length)];
            if (queue.Orphaned)
                continue;
            var received = (await queue.Queue.ReceiveMessagesAsync(1, lease, cancellationToken).ConfigureAwait(false)).Value;
            if (received.Length > 0)
                return (queue, received[0]);
        }
        return null;
    }

    /// <summary>
    /// Runs the rules one message lists, for the object as it now is, and settles the message:
    /// deleted when every rule is done, otherwise replaced in place by one listing what is left,
    /// hidden until its next try.
    /// </summary>
    /// <param name="budget">The most rules to run; null for no limit. What is left over stays in the message, ready at once.</param>
    /// <returns>How many rules were run, failed runs included.</returns>
    private async Task<int> RunMessageAsync(TypeQueue queue, QueueMessage received, TimeSpan lease, int? budget, CancellationToken cancellationToken)
    {
        var message = RuleMessage.TryParse(received.Body.ToString());
        if (message == null)
        {
            // Not one of ours; keep it where it can be looked at rather than run or lose it.
            await queue.Poison.SendMessageAsync(received.Body.ToString(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
            await queue.Queue.DeleteMessageAsync(received.MessageId, received.PopReceipt, cancellationToken).ConfigureAwait(false);
            return 0;
        }

        // Not yet time (a wait longer than a queue can hide a message for, made in steps): hidden again, with nothing run.
        if (message.Due is { } due && HideFor(due, DateTimeOffset.UtcNow) is { } hide)
        {
            await queue.Queue.UpdateMessageAsync(received.MessageId, received.PopReceipt, (string?)null, hide, cancellationToken).ConfigureAwait(false);
            return 0;
        }

        // The pop receipt changes every time the hold on the message is extended.
        var receipt = received.PopReceipt;
        var holdLost = false;
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var holdCts = new CancellationTokenSource();
        var holding = HoldAsync();

        async Task HoldAsync()
        {
            try
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromTicks(lease.Ticks / 2), holdCts.Token).ConfigureAwait(false);
                    receipt = (await queue.Queue.UpdateMessageAsync(received.MessageId, receipt, (string?)null, lease, holdCts.Token).ConfigureAwait(false)).Value.PopReceipt;
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (RequestFailedException)
            {
                // The hold was lost, so someone else may be given the message: stop this run.
                holdLost = true;
                runCts.Cancel();
            }
        }

        var state = new MessageRun();
        // Two runs for one object never overlap: this one starts when the one before it has ended.
        var turn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task before;
        lock (_keyTails)
        {
            before = _keyTails.GetValueOrDefault(message.Key) ?? Task.CompletedTask;
            _keyTails[message.Key] = turn.Task;
        }
        try
        {
            await before.WaitAsync(runCts.Token).ConfigureAwait(false);
            Task run;
            // Outside the write lease and with no save chain's context: a handler's own saves are ordinary root saves.
            using (ExecutionContext.SuppressFlow())
                run = Task.Run(() => RunRulesAsync(queue, message, state, budget, runCts.Token, cancellationToken));
            await run.ConfigureAwait(false);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested || holdLost)
        {
            // Closing, a cancelled drain, or the hold was lost while waiting for a turn or reading the object.
            state.Interrupted = true;
        }
        catch (WriteLockUnavailableException)
        {
            state.Contended = true;
        }
        finally
        {
            lock (_keyTails)
            {
                if (_keyTails.TryGetValue(message.Key, out var tail) && tail == turn.Task)
                    _keyTails.Remove(message.Key);
            }
            turn.SetResult();
            holdCts.Cancel();
            await holding.ConfigureAwait(false);
        }

        // Someone else may hold the message now; whatever is written to it here would be refused.
        if (holdLost)
            return state.Runs;

        // What follows must happen even while closing, or the work would be done again for nothing.
        var settle = CancellationToken.None;
        try
        {
            if (state.Gone)
            {
                await queue.Queue.DeleteMessageAsync(received.MessageId, receipt, settle).ConfigureAwait(false);
                return state.Runs;
            }

            var tries = 0;
            foreach (var (rule, failure) in state.Failed)
            {
                var text = failure.Message.Length <= 500 ? failure.Message : failure.Message[..500];
                _ruleStates.GetOrAdd(rule, _ => new RuleState()).LastError = failure.Message;
                var attempts = message.AttemptsOf(rule) + 1;
                if (attempts >= _config.RuleMaxAttempts)
                {
                    // Set aside by itself: the other rules of the message carry on without it.
                    var record = new PoisonRecord { Type = queue.TypeName, Key = message.Key, ETag = message.ETag, Rule = rule, Error = text, Attempts = attempts };
                    await queue.Poison.SendMessageAsync(record.ToText(), null, NeverExpires, settle).ConfigureAwait(false);
                    state.Pending.Remove(rule);
                    continue;
                }
                (message.Attempts ??= [])[rule] = attempts;
                (message.LastError ??= [])[rule] = text;
                tries = Math.Max(tries, attempts);
            }

            if (state.Pending.Count == 0)
            {
                await queue.Queue.DeleteMessageAsync(received.MessageId, receipt, settle).ConfigureAwait(false);
                return state.Runs;
            }

            // Kept in place, listing only what is left, with each failure written into it.
            message.Rules = state.Pending;
            message.Attempts = Only(message.Attempts, state.Pending);
            message.LastError = Only(message.LastError, state.Pending);
            TimeSpan wait;
            if (tries == 0 && !state.Contended && !state.Interrupted && state.DeferUntil is { } until)
            {
                // What is left has a later time now (At): hidden until then.
                message.Due = until;
                wait = HideFor(until, DateTimeOffset.UtcNow) ?? TimeSpan.Zero;
            }
            else if (tries > 0)
                wait = RetryWait(tries);              // hidden for a wait that doubles with each failure
            else if (state.Contended)
                wait = RetryWait(++message.Waits);    // no failure counted, but given back at once it would spin while the lock is held
            else
                wait = TimeSpan.Zero;                 // interrupted: given back at once, with no failure counted
            await queue.Queue.UpdateMessageAsync(received.MessageId, receipt, message.ToText(), wait, settle).ConfigureAwait(false);
        }
        catch (RequestFailedException)
        {
            // The hold ran out before this could be recorded. The message is still queued and will be run again.
        }
        return state.Runs;

        static Dictionary<string, TValue>? Only<TValue>(Dictionary<string, TValue>? all, List<string> rules)
        {
            if (all == null)
                return null;
            var kept = all.Where(entry => rules.Contains(entry.Key)).ToDictionary(entry => entry.Key, entry => entry.Value);
            return kept.Count == 0 ? null : kept;
        }
    }

    /// <summary>
    /// Reads the object once and runs the message's rules on it in phase order (<c>Update</c>
    /// rules, then <c>Maintain</c>, then <c>Then</c>), noting in <paramref name="state"/> how each went. A rule
    /// that fails does not stop the others.
    /// </summary>
    /// <param name="token">Cancelled when the run must stop: closing, a cancelled drain, or the hold on the message was lost.</param>
    /// <param name="stopping">Cancelled only for closing or a cancelled drain, which are no fault of a rule.</param>
    private async Task RunRulesAsync(TypeQueue queue, RuleMessage message, MessageRun state, int? budget, CancellationToken token, CancellationToken stopping)
    {
        var found = await GetAsync(message.Key, token).ConfigureAwait(false);
        if (found == null || found.GetType() != queue.Type)
        {
            // Deleted since, or replaced by an object of another type, whose save queued its own rules.
            state.Gone = true;
            return;
        }
        object entity = found;

        // The rules the message names. If it names one this configuration does not have (renamed or
        // removed since), every queued rule of the type is run instead: rules are safe to run
        // twice, and that way no work is lost to a rename.
        var named = message.Rules.ToHashSet(StringComparer.Ordinal);
        var todo = named.All(id => queue.Rules.Any(r => r.Id == id))
            ? queue.Rules.Where(r => named.Contains(r.Id)).ToList()
            : [.. queue.Rules];
        state.Pending = [.. todo.Select(r => r.Id)];

        var run = new AsyncRun(this, message.Hop, message.Key);
        _asyncRun.Value = run;
        for (var n = 0; n < todo.Count; n++)
        {
            var rule = todo[n];
            if (budget is { } most && state.Runs >= most)
            {
                state.Interrupted = true;
                return;
            }
            run.RuleId = rule.Id;
            run.Remaining = [.. todo.Skip(n + 1).Select(r => r.Id)];
            var ruleState = _ruleStates.GetOrAdd(rule.Id, _ => new RuleState());
            using var ruleCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            var counted = false;
            try
            {
                if (run.Dirty)
                {
                    // Saved during this run: the next rule is given the object as it now is.
                    run.Dirty = false;
                    var latest = await GetAsync(message.Key, token).ConfigureAwait(false);
                    if (latest == null || latest.GetType() != queue.Type)
                    {
                        state.Gone = true;
                        return;
                    }
                    entity = latest;
                }

                // A run is counted from here, so that a filter that throws is a failed run.
                state.Runs++;
                counted = true;
                // The object may have changed since it was queued; Changed(...) is taken to be true.
                // (A Maintain rule runs all the same: it then deletes what the object produced.)
                if (rule.Verb != RuleVerb.Maintain && rule.Rest is { } rest && !rest(entity))
                {
                    state.Runs--;   // not for this object any more: skipped, which is no run
                    state.Pending.Remove(rule.Id);
                    continue;
                }

                if (rule.At is { } at)
                {
                    var when = at(entity);
                    if (when is null)
                    {
                        state.Runs--;   // the object gives no time any more: dropped, which is no run
                        state.Pending.Remove(rule.Id);
                        continue;
                    }
                    if (when > DateTimeOffset.UtcNow)
                    {
                        state.Runs--;   // the object gives a later time now: left in the message for then
                        if (state.DeferUntil is null || when < state.DeferUntil)
                            state.DeferUntil = when;
                        continue;
                    }
                }

                ruleCts.CancelAfter(_config.RuleTimeout);
                Interlocked.Increment(ref ruleState.Running);
                try
                {
                    var attempt = message.AttemptsOf(rule.Id) + 1;
                    if (rule.Verb == RuleVerb.Update)
                        await RunUpdateAsync(rule, entity, attempt, message.ETag, run, ruleCts.Token).ConfigureAwait(false);
                    else if (rule.Verb == RuleVerb.Maintain)
                        await RunMaintainAsync(rule, entity, attempt, message.ETag, ruleCts.Token).ConfigureAwait(false);
                    else
                        await rule.Invoke(entity, new RuleContext(this, rule.Name, rule.Id, attempt, entity, message.ETag), ruleCts.Token).ConfigureAwait(false);
                }
                finally
                {
                    Interlocked.Decrement(ref ruleState.Running);
                }
                state.Pending.Remove(rule.Id);
                ruleState.LastError = null;
            }
            catch (Exception) when (stopping.IsCancellationRequested)
            {
                // The database is closing, or the drain was cancelled. Whatever the handler threw
                // then, its cancellation or a database disposed under it, is no fault of its own.
                if (counted)
                    state.Runs--;
                state.Interrupted = true;
                return;
            }
            catch (Exception) when (token.IsCancellationRequested)
            {
                throw;   // the hold on the message was lost: stop, and settle nothing
            }
            catch (WriteLockUnavailableException)
            {
                state.Contended = true;   // another process is writing; no fault of the rule's
                return;
            }
            catch (OperationCanceledException) when (ruleCts.IsCancellationRequested)
            {
                state.Failed[rule.Id] = new TimeoutException($"The run was stopped after {_config.RuleTimeout}.");
            }
            catch (Exception e)
            {
                state.Failed[rule.Id] = e;
            }
        }
    }

    /// <summary>
    /// Runs an <c>Update</c> rule: the handler edits a copy, and the copy is written back only if
    /// the handler changed it. If the object was saved by someone else meanwhile, the handler's
    /// changes are applied to the newer version when that is safe (the rule's <c>Changed</c>
    /// inputs are still the same, and nobody else touched the properties the handler set), and
    /// dropped otherwise. The handler is called once, whatever happens.
    /// </summary>
    private async Task RunUpdateAsync(QueuedRule rule, object original, int attempt, string? etag, AsyncRun run, CancellationToken cancellationToken)
    {
        var type = original.GetType();
        var key = AsyncKeyOf(original);
        var originalJson = original.GetJson() ?? JsonSerializer.Serialize(original, type);
        var copy = CopyOf(originalJson, type, key, original.GetETag(), original.GetSource());

        await rule.Invoke(copy, new RuleContext(this, rule.Name, rule.Id, attempt, copy, etag), cancellationToken).ConfigureAwait(false);

        var copyJson = JsonSerializer.Serialize(copy, type);
        if (copyJson == originalJson)
            return;   // nothing changed, nothing to write

        try
        {
            // Conditional on the ETag the copy was made from.
            await SaveAsync(copy, cancellationToken).ConfigureAwait(false);
            return;
        }
        catch (ConcurrencyException)
        {
        }

        // Saved by someone else during the run. The rules after this one get the newer object.
        run.Dirty = true;
        // Without named inputs there is no telling whether the result still fits the newer object.
        if (rule.Inputs.Length == 0)
            return;

        var before = JsonNode.Parse(originalJson)!.AsObject();
        var after = JsonNode.Parse(copyJson)!.AsObject();
        // What the handler set: the top-level properties that differ between the original and the copy.
        var outputs = before.Select(p => p.Key).Union(after.Select(p => p.Key))
            .Where(name => !JsonNode.DeepEquals(before[name], after[name])).ToList();

        for (var write = 0; write < MaxUpdateWrites; write++)
        {
            var latest = await GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (latest == null || latest.GetType() != type)
                return;   // deleted, or replaced by something else
            // The inputs changed: the save that changed them queued a fresh run, and this result is out of date.
            if (!RuleFilter.SameValues(original, latest, rule.Inputs))
                return;
            var merged = JsonNode.Parse(latest.GetJson() ?? JsonSerializer.Serialize(latest, type))!.AsObject();
            // Someone else set a property the handler set: theirs stands.
            if (outputs.Any(name => !JsonNode.DeepEquals(before[name], merged[name])))
                return;
            foreach (var name in outputs)
            {
                if (after.ContainsKey(name))
                    merged[name] = after[name]?.DeepClone();
                else
                    merged.Remove(name);
            }
            var latestJson = latest.GetJson() ?? JsonSerializer.Serialize(latest, type);
            var result = CopyOf(merged.ToJsonString(), type, key, latest.GetETag(), latest.GetSource());
            // As loaded, so the save sees what it replaces.
            result.SetJson(latestJson);
            try
            {
                await SaveAsync(result, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (ConcurrencyException)
            {
                // Saved again meanwhile: look at the newest version, without calling the handler again.
            }
        }
        throw new InvalidOperationException(
            $"The result of rule '{rule.Id}' for '{key}' could not be written: the object was saved by others {MaxUpdateWrites} times in a row.");
    }

    /// <summary>An object made from JSON, with what an object loaded from the database carries.</summary>
    private object CopyOf(string json, Type type, string key, string? etag, string? source)
    {
        var copy = JsonSerializer.Deserialize(json, type)
            ?? throw new InvalidOperationException($"'{key}' could not be copied: its stored form is empty.");
        copy.SetJson(json);
        copy.SetKey(key);
        if (etag != null)
            copy.SetETag(etag);
        copy.SetSchema(type.Name);
        copy.SetSource(source);   // an object a Maintain rule produced stays that rule's
        if (copy is BlobFile blob)
            blob.Database = this;
        return copy;
    }

    /// <summary>How long a message is hidden before its next try: the retry delay, doubled for each try after the first, up to a cap.</summary>
    private TimeSpan RetryWait(int tries) =>
        TimeSpan.FromTicks(Math.Min(_config.RuleRetryDelay.Ticks << Math.Min(tries - 1, 20), MaxRetryDelay.Ticks));

    // === Public surface ===

    /// <summary>
    /// Runs queued rules until no work is ready, or until <paramref name="max"/> rule runs have
    /// finished. Work that is waiting to be retried, after a failure or after finding the write
    /// lock held by another process, is not ready and is left for later. Use it in tests, in
    /// command-line tools, and with <see cref="QueueMode.Manual"/>.
    /// </summary>
    /// <param name="max">
    /// The most rule runs to do; null for no limit. With a limit, objects are worked on one at a
    /// time; without, up to <see cref="ILottaConfiguration.MaxConcurrency"/> at once.
    /// </param>
    /// <param name="cancellationToken">Stops the drain.</param>
    /// <returns>How many rules were run, counting failed runs. A rule skipped because its filter is no longer true is not a run.</returns>
    /// <remarks>
    /// The token each handler is given is cancelled when <paramref name="cancellationToken"/> is,
    /// or when the database is disposed. A run stopped that way is not counted as a failure: its
    /// work stays queued and runs again later. The drain then throws
    /// <see cref="OperationCanceledException"/>, rather than return a count, even when the run
    /// stopped was the last one <paramref name="max"/> allows.
    /// </remarks>
    /// <exception cref="InvalidOperationException">Called from inside a handler or a save, which would wait on itself.</exception>
    /// <exception cref="OperationCanceledException">The drain was cancelled, or the database was disposed during it.</exception>
    public async Task<int> DrainAsync(int? max = null, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_asyncRun.Value != null || _chainChanges.Value != null)
            throw new InvalidOperationException("DrainAsync cannot be called from inside a handler or a save.");
        if (max is <= 0)
            return 0;

        // Closing the database stops a drain's runs as it stops the background loop's.
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _asyncCts.Token);
        // Schedules whose time has come are run first; each counts as a run.
        var runs = HasSchedules ? (await RunDueSchedulesAsync(max, stopping.Token).ConfigureAwait(false)).Runs : 0;
        if (HasQueuedRules && !(max is { } most && runs >= most))
            runs += await PumpAsync(max - runs, forever: false, stopping.Token).ConfigureAwait(false);
        // A run interrupted by the cancellation is no run, even the last one max allows, and its work is still queued.
        stopping.Token.ThrowIfCancellationRequested();
        return runs;
    }

    /// <summary>Where the queued rules stand; see <see cref="LiveRuleSet.StatusAsync"/>.</summary>
    internal async Task<RulesStatus> RulesStatusAsync(CancellationToken cancellationToken)
    {
        // Readable after the database is disposed: the queues belong to the catalog.
        var poisoned = new SortedDictionary<string, int>(StringComparer.Ordinal);
        var types = new List<RuleTypeStatus>();
        foreach (var queue in _typeQueues.OrderBy(q => q.TypeName, StringComparer.Ordinal))
        {
            var waiting = (await queue.Queue.GetPropertiesAsync(cancellationToken).ConfigureAwait(false)).Value.ApproximateMessagesCount;
            var aside = (await queue.Poison.GetPropertiesAsync(cancellationToken).ConfigureAwait(false)).Value.ApproximateMessagesCount;
            types.Add(new RuleTypeStatus(queue.TypeName, waiting, aside, queue.Orphaned));
            if (aside == 0)
                continue;
            // Which rules the set-aside work belongs to is only in the records themselves.
            foreach (var peeked in (await queue.Poison.PeekMessagesAsync(32, cancellationToken).ConfigureAwait(false)).Value)
            {
                if (PoisonRecord.TryParse(peeked.Body.ToString()) is { } record)
                    poisoned[record.Rule] = poisoned.GetValueOrDefault(record.Rule) + 1;
            }
        }

        var rules = new List<RuleStatus>();
        foreach (var rule in _config.QueuedRules)
        {
            _ruleStates.TryGetValue(rule.Id, out var state);
            rules.Add(new RuleStatus(rule.Id, state == null ? 0 : Volatile.Read(ref state.Running), poisoned.GetValueOrDefault(rule.Id), state?.LastError));
            poisoned.Remove(rule.Id);
        }
        foreach (var schedule in _config.Schedules)
        {
            _ruleStates.TryGetValue(schedule.Id, out var state);
            rules.Add(new RuleStatus(schedule.Id, state == null ? 0 : Volatile.Read(ref state.Running), 0, state?.LastError));
        }
        // Work set aside under a name no rule has now: the rule was renamed or removed.
        foreach (var (rule, count) in poisoned)
            rules.Add(new RuleStatus(rule, 0, count, null, Orphaned: true));
        return new RulesStatus(rules, types);
    }

    /// <summary>Puts a rule's set-aside work back in its queue; see <see cref="LiveRuleSet.RetryAsync"/>.</summary>
    internal async Task<int> RetryRuleAsync(string rule, CancellationToken cancellationToken)
    {
        var id = RuleIdForChange(rule, registered: false);
        var moved = await WalkPoisonAsync(id, async (queue, record) =>
        {
            // A fresh start: no attempts, and not part of the chain of rules that led to it.
            var message = new RuleMessage { Key = record.Key, ETag = record.ETag, Rules = [id] };
            await queue.Queue.SendMessageAsync(message.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
        if (moved > 0)
            Signal();
        return moved;
    }

    /// <summary>Deletes a rule's set-aside work; see <see cref="LiveRuleSet.DiscardAsync"/>.</summary>
    internal Task<int> DiscardRuleAsync(string rule, CancellationToken cancellationToken) =>
        WalkPoisonAsync(RuleIdForChange(rule, registered: false), (_, _) => Task.CompletedTask, cancellationToken);

    /// <summary>
    /// Goes through all set-aside work, takes out the records of one rule after giving each to
    /// <paramref name="take"/>, and leaves the rest as it was. A record is handed on before it is
    /// deleted, so a crash part way through can run a piece of work twice but never loses one.
    /// </summary>
    /// <returns>How many records were taken out.</returns>
    private async Task<int> WalkPoisonAsync(string rule, Func<TypeQueue, PoisonRecord, Task> take, CancellationToken cancellationToken)
    {
        var taken = 0;
        foreach (var queue in _typeQueues)
        {
            // The records of other rules, hidden while this goes on so each is seen once, with their latest receipts.
            var kept = new Dictionary<string, string>(StringComparer.Ordinal);
            try
            {
                while (true)
                {
                    var batch = (await queue.Poison.ReceiveMessagesAsync(32, TimeSpan.FromSeconds(60), cancellationToken).ConfigureAwait(false)).Value;
                    var fresh = 0;
                    foreach (var poisoned in batch)
                    {
                        if (PoisonRecord.TryParse(poisoned.Body.ToString()) is { } record && record.Rule == rule)
                        {
                            await take(queue, record).ConfigureAwait(false);
                            await queue.Poison.DeleteMessageAsync(poisoned.MessageId, poisoned.PopReceipt, cancellationToken).ConfigureAwait(false);
                            kept.Remove(poisoned.MessageId);
                            taken++;
                            fresh++;
                            continue;
                        }
                        // Seen before only when this has gone on longer than the records stay hidden.
                        if (!kept.ContainsKey(poisoned.MessageId))
                            fresh++;
                        kept[poisoned.MessageId] = poisoned.PopReceipt;
                    }
                    if (fresh == 0)
                        break;
                }
            }
            finally
            {
                foreach (var (messageId, receipt) in kept)
                {
                    try
                    {
                        await queue.Poison.UpdateMessageAsync(messageId, receipt, (string?)null, TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (RequestFailedException)
                    {
                        // It shows again by itself when its hiding runs out.
                    }
                }
            }
        }
        return taken;
    }

    /// <summary>How the last backfill found its objects: "index" or "scan". For tests.</summary>
    internal string? LastBackfillPath { get; private set; }

    /// <summary>Queues a rule for every stored object its filter is true for; see <see cref="LiveRuleSet.BackfillAsync"/>.</summary>
    internal async Task<int> BackfillRuleAsync(string rule, CancellationToken cancellationToken)
    {
        var id = RuleIdForChange(rule, registered: true);
        var queued = _config.QueuedRules.First(r => r.Id == id);
        var count = 0;
        await foreach (var entity in BackfillCandidatesAsync(queued, cancellationToken).ConfigureAwait(false))
        {
            if (entity is null or JsonDocument || !queued.ObjectType.IsInstanceOfType(entity))
                continue;
            // Checked on every candidate, whichever way it was found: the index may return more
            // than the filter is true for (an analyzed field, a stale entry).
            if (queued.Rest is { } rest && !rest(entity))
                continue;
            await QueueRuleAsync(entity, id, hop: 0, cancellationToken).ConfigureAwait(false);
            count++;
        }
        if (count > 0)
            Signal();
        return count;
    }

    /// <summary>Queues one rule for one stored object, in the queue of the object's type. The caller wakes the loop.</summary>
    private async Task QueueRuleAsync(object entity, string ruleId, int hop, CancellationToken cancellationToken)
    {
        var queue = await TypeQueueAsync(entity.GetType(), cancellationToken).ConfigureAwait(false);
        var message = new RuleMessage { Key = AsyncKeyOf(entity), ETag = entity.GetETag(), Rules = [ruleId], Hop = hop };
        await queue.Queue.SendMessageAsync(message.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The objects a backfill looks at. With a filter, the search index is asked for the objects
    /// it is true for, which is cheap. If the index cannot express the filter (comparing two
    /// fields, a method call) or the type is not searchable, every object in the database is read
    /// instead. The index may lag the newest writes by up to <c>MaxSearchStaleness</c>, but an
    /// object saved within that window queued the rule itself when it was saved, so missing it
    /// here loses nothing.
    /// </summary>
    private async IAsyncEnumerable<object?> BackfillCandidatesAsync(QueuedRule rule,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        List<object>? indexed = null;
        if (rule.FindIndexed is { } find)
        {
            try
            {
                indexed = [.. find(this).Cast<object>()];
            }
            catch (Exception e) when (e is not OperationCanceledException and not ObjectDisposedException)
            {
                indexed = null;   // not something the index can answer; read everything instead
            }
        }

        if (indexed != null)
        {
            LastBackfillPath = "index";
            foreach (var entity in indexed)
                yield return entity;
            yield break;
        }

        LastBackfillPath = "scan";
        await foreach (var entity in GetManyAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
            yield return entity;
    }

    /// <summary>
    /// The identity of the rule a caller named, for an operation that changes its queued work:
    /// as given when it is a full identity (<c>Note/summarize</c>), or found by name alone
    /// (<c>summarize</c>) when exactly one rule has it.
    /// </summary>
    /// <param name="registered">Whether a full identity must be a rule of this configuration; false to allow an orphaned one.</param>
    private string RuleIdForChange(string rule, bool registered)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(rule);
        if (_config.ReadOnly)
            throw new InvalidOperationException(
                $"Database '{_databaseId}' was opened read-only (ILottaConfiguration.ReadOnly). Writes are not permitted.");
        if (rule.Contains('/'))
        {
            if (registered && !_config.QueuedRules.Any(r => r.Id == rule))
                throw new ArgumentException($"There is no queued rule '{rule}' in database '{_databaseId}'.", nameof(rule));
            return rule;
        }
        var named = _config.QueuedRules.Where(r => r.Name == rule).Select(r => r.Id).ToList();
        return named.Count switch
        {
            1 => named[0],
            0 => throw new ArgumentException($"There is no queued rule named '{rule}' in database '{_databaseId}'.", nameof(rule)),
            _ => throw new ArgumentException($"Several queued rules are named '{rule}': {string.Join(", ", named)}. Say which one.", nameof(rule)),
        };
    }

    // === Clean-up ===

    /// <summary>Empties every rule queue of the database, orphaned ones included; part of resetting the database.</summary>
    private async Task ClearAsyncQueuesAsync(CancellationToken cancellationToken)
    {
        if (_lottaCatalog.GetQueueServiceClient() is not { } queues)
            return;
        var names = new List<string>();
        var prefix = RuleQueueNames.DatabasePrefix(_lottaCatalog.Name, _databaseId);
        await foreach (var queue in queues.GetQueuesAsync(prefix: prefix, cancellationToken: cancellationToken).ConfigureAwait(false))
            names.Add(queue.Name);
        foreach (var name in names)
        {
            try
            {
                await queues.GetQueueClient(name).ClearMessagesAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                // deleted since it was listed
            }
        }
    }

    /// <summary>Deletes the database's queues, including orphaned ones of types that have no rules any more.</summary>
    private async Task DeleteAsyncQueuesAsync(CancellationToken cancellationToken)
    {
        if (_lottaCatalog.GetQueueServiceClient() is { } queues)
            await RuleQueueNames.DeleteQueuesAsync(queues, RuleQueueNames.DatabasePrefix(_lottaCatalog.Name, _databaseId), cancellationToken).ConfigureAwait(false);
        // Their clients are stale now; a queue is made again if work is queued again.
        lock (_typeQueuesLock)
        {
            _queueByType.Clear();
            _typeQueues = [];
        }
    }
}
