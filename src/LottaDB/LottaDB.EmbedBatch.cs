using System.Text.Json;
using Azure;
using Azure.Storage.Queues.Models;
using Lotta.Internal;

namespace Lotta;

// The embed rule run in batches. One message at a time, the cost of embedding is mostly what
// surrounds the call to the model: reading the object, writing its row and its index document,
// settling its message. A batch takes up to QueuedRule.BatchSize messages from the rule's own
// queue, embeds every text in one call and writes every row in one conditional transaction.
// Everything else is as for one message: a turn per object, the rule's filter, a vector put on a
// newer version of an object saved meanwhile, and each message settled by itself.
public partial class LottaDB
{
    /// <summary>One message of a batch, and how its run went.</summary>
    private sealed class BatchItem(QueueMessage received, RuleMessage message)
    {
        public QueueMessage Received { get; } = received;
        public RuleMessage Message { get; } = message;
        public string Key => Message.Key;

        /// <summary>The pop receipt, which changes every time the hold on the message is extended.</summary>
        public string Receipt = received.PopReceipt;

        /// <summary>Set when the hold on the message was lost: someone else may be given it, so it is not settled here.</summary>
        public volatile bool HoldLost;

        public MessageRun State { get; } = new();

        /// <summary>The object as read, and the copy the vectors are put on.</summary>
        public object? Original, Copy;
    }

    /// <summary>The rule a queue runs in batches, if it does.</summary>
    private QueuedRule? BatchRuleOf(TypeQueue queue) =>
        queue.Rule is { } id && queue.Type is { } type
            ? QueuedRulesFor(type).FirstOrDefault(r => r.Id == id && r.BatchSize > 1)
            : null;

    /// <summary>
    /// Takes as many more of a queue's ready messages as the rule's batch size leaves room for,
    /// beside the one the pump has taken. None, if the queue cannot be read just now.
    /// </summary>
    private static async Task<QueueMessage[]> ReceiveMoreAsync(TypeQueue queue, QueuedRule rule, TimeSpan lease, CancellationToken cancellationToken)
    {
        try
        {
            return (await queue.Queue.ReceiveMessagesAsync(rule.BatchSize - 1, lease, cancellationToken).ConfigureAwait(false)).Value;
        }
        catch (RequestFailedException)
        {
            return [];   // the message taken is held; the rest wait for the next batch
        }
    }

    /// <summary>
    /// Runs a batch of a queue's messages. A message the batch cannot take as it is (a second
    /// message for one object, one naming a rule renamed since, one not yet due) is run by itself.
    /// </summary>
    private async Task<int> RunBatchAsync(TypeQueue queue, QueuedRule rule, List<QueueMessage> received, TimeSpan lease, CancellationToken cancellationToken)
    {
        var items = new List<BatchItem>();
        var single = new List<QueueMessage>();
        var keys = new HashSet<string>(StringComparer.Ordinal);
        var now = DateTimeOffset.UtcNow;
        foreach (var message in received)
        {
            var parsed = RuleMessage.TryParse(message.Body.ToString());
            if (parsed is { Rules: [var only], Deleted: null, Tombstone: null } && only == rule.Id
                && (parsed.Due is not { } due || due <= now) && keys.Add(parsed.Key))
                items.Add(new BatchItem(message, parsed));
            else
                single.Add(message);
        }

        var singles = single.Select(message => RunMessageAsync(queue, message, lease, budget: null, cancellationToken)).ToList();
        var runs = items.Count == 0 ? 0 : await RunBatchItemsAsync(queue, rule, items, lease, cancellationToken).ConfigureAwait(false);
        foreach (var run in singles)
            runs += await run.ConfigureAwait(false);
        return runs;
    }

    private async Task<int> RunBatchItemsAsync(TypeQueue queue, QueuedRule rule, List<BatchItem> items, TimeSpan lease, CancellationToken cancellationToken)
    {
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
                    foreach (var item in items.Where(i => !i.HoldLost))
                    {
                        try
                        {
                            item.Receipt = (await queue.Queue.UpdateMessageAsync(item.Received.MessageId, item.Receipt, (string?)null, lease, holdCts.Token).ConfigureAwait(false)).Value.PopReceipt;
                        }
                        catch (RequestFailedException)
                        {
                            item.HoldLost = true;
                        }
                    }
                    // Every message may be given to someone else now: stop the run.
                    if (items.All(i => i.HoldLost))
                    {
                        runCts.Cancel();
                        return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        // Two runs for one object never overlap. The batch takes its turn for every object at once,
        // under the lock, so waits always follow the order turns were taken in and cannot go round in a circle.
        var turn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var before = new List<Task>();
        lock (_keyTails)
        {
            foreach (var item in items)
            {
                if (_keyTails.TryGetValue(item.Key, out var tail))
                    before.Add(tail);
                _keyTails[item.Key] = turn.Task;
            }
        }

        var ruleState = _ruleStates.GetOrAdd(rule.Id, _ => new RuleState());
        try
        {
            await Task.WhenAll(before).WaitAsync(runCts.Token).ConfigureAwait(false);
            Task run;
            // Outside the write lease and with no save chain's context, as for one message.
            using (ExecutionContext.SuppressFlow())
                run = Task.Run(() => EmbedBatchAsync(queue, rule, items, ruleState, runCts.Token, cancellationToken));
            await run.ConfigureAwait(false);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested || items.All(i => i.HoldLost))
        {
            foreach (var item in items)
                item.State.Interrupted = true;
        }
        catch (WriteLockUnavailableException)
        {
            foreach (var item in items)
                item.State.Contended = true;
        }
        finally
        {
            lock (_keyTails)
            {
                foreach (var item in items)
                {
                    if (_keyTails.TryGetValue(item.Key, out var tail) && tail == turn.Task)
                        _keyTails.Remove(item.Key);
                }
            }
            turn.SetResult();
            holdCts.Cancel();
            await holding.ConfigureAwait(false);
        }

        var runs = 0;
        foreach (var item in items)
        {
            runs += item.State.Runs;
            if (!item.HoldLost)
                await SettleAsync(queue, item.Received.MessageId, item.Receipt, item.Message, item.State).ConfigureAwait(false);
        }
        return runs;
    }

    /// <summary>
    /// Reads the batch's objects, embeds the texts that have no vector in one call, and writes
    /// the vectors back: all in one conditional transaction, or, where an object was saved by
    /// someone else meanwhile, one by one onto its newer version.
    /// </summary>
    /// <param name="token">Cancelled when the run must stop: closing, a cancelled drain, or every hold lost.</param>
    /// <param name="stopping">Cancelled only for closing or a cancelled drain, which are no fault of the rule.</param>
    private async Task EmbedBatchAsync(TypeQueue queue, QueuedRule rule, List<BatchItem> items, RuleState ruleState, CancellationToken token, CancellationToken stopping)
    {
        var found = await Task.WhenAll(items.Select(item => GetAsync(item.Key, token))).ConfigureAwait(false);
        var work = new List<BatchItem>();
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            item.State.Pending = [rule.Id];
            if (found[i] is not { } entity || entity.GetType() != queue.Type)
            {
                item.State.Gone = true;   // deleted since, or replaced by an object of another type
                continue;
            }
            // A run is counted from here, so that a filter that throws is a failed run.
            item.State.Runs = 1;
            try
            {
                if (rule.Rest is { } rest && !rest(entity))
                {
                    item.State.Runs = 0;   // not for this object any more: skipped, which is no run
                    item.State.Pending.Clear();
                    continue;
                }
            }
            catch (Exception e)
            {
                item.State.Failed[rule.Id] = e;
                continue;
            }
            item.Original = entity;
            work.Add(item);
        }
        if (work.Count == 0)
            return;

        using var ruleCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        ruleCts.CancelAfter(_config.RuleTimeout);
        Interlocked.Add(ref ruleState.Running, work.Count);
        try
        {
            foreach (var item in work)
            {
                var original = item.Original!;
                var json = original.GetJson() ?? JsonSerializer.Serialize(original, original.GetType());
                item.Copy = CopyOf(json, original.GetType(), item.Key, original.GetETag(), original);
            }
            await EmbedVectorsAsync([.. work.Select(item => (item.Copy!, item.Key))], ruleCts.Token).ConfigureAwait(false);

            var changed = work.Where(item => !SameVectors(item.Copy!, item.Original!)).ToList();
            if (changed.Count > 0 && !await SaveVectorsAsync(rule, changed, ruleCts.Token).ConfigureAwait(false))
            {
                // Some object was saved by someone else meanwhile, so nothing was written: each is
                // written by itself, onto its newer version where it has one.
                foreach (var item in changed)
                {
                    try
                    {
                        await WriteVectorsAsync(rule, item, ruleCts.Token).ConfigureAwait(false);
                    }
                    catch (Exception e) when (e is not OperationCanceledException and not WriteLockUnavailableException)
                    {
                        item.State.Failed[rule.Id] = e;
                    }
                }
            }

            foreach (var item in work.Where(item => !item.State.Failed.ContainsKey(rule.Id)))
                item.State.Pending.Remove(rule.Id);
            ruleState.LastError = work.Select(item => item.State.Failed.GetValueOrDefault(rule.Id)?.Message).LastOrDefault(error => error != null);
        }
        catch (Exception) when (stopping.IsCancellationRequested)
        {
            // Closing, or a cancelled drain: no fault of the rule, and no run.
            foreach (var item in work)
            {
                item.State.Runs = 0;
                item.State.Interrupted = true;
            }
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
            throw;   // every hold was lost: stop, and settle nothing
        }
        catch (WriteLockUnavailableException)
        {
            foreach (var item in work)
                item.State.Contended = true;
        }
        catch (OperationCanceledException) when (ruleCts.IsCancellationRequested)
        {
            foreach (var item in work)
                item.State.Failed[rule.Id] = new TimeoutException($"The run was stopped after {_config.RuleTimeout}.");
        }
        catch (Exception e)
        {
            foreach (var item in work)
                item.State.Failed[rule.Id] = e;
        }
        finally
        {
            Interlocked.Add(ref ruleState.Running, -work.Count);
        }
    }

    /// <summary>
    /// Writes the copies, with their new vectors, in one transaction, each only if its row is
    /// still the version it was copied from; then indexes them and runs their handlers, as a save
    /// does. Each is saved as by its own run of the rule, so the rule is not queued again for it.
    /// </summary>
    /// <returns>False when nothing was written: an object's lock was taken, a row had changed or was gone, or the storage could not write them together.</returns>
    private async Task<bool> SaveVectorsAsync(QueuedRule rule, List<BatchItem> items, CancellationToken cancellationToken)
    {
        if (items.Any(item => item.Copy!.GetETag() == null))
            return false;

        using var writerLease = await AcquireWriterAsync(cancellationToken).ConfigureAwait(false);
        // Only if every object's lock is free now. Waiting for them while holding the others could
        // wait on a save that holds one and waits for another of them (a message's save writing its
        // passages); one by one, each holds a single lock and cannot be part of such a circle.
        using var locks = TryAcquireLocks(items.Select(item => item.Key));
        if (locks is null)
            return false;
        var changes = new List<ObjectChange>();
        var errors = new List<Exception>();
        var before = new string?[items.Count];
        try
        {
            var writes = new List<(string Key, object Object, Internal.TypeMetadata Meta, string ETag)>();
            for (var i = 0; i < items.Count; i++)
            {
                var copy = items[i].Copy!;
                var key = items[i].Key;
                before[i] = copy.GetJson();
                // As SaveAsync does before its write.
                await NotePreviousAsync(copy, key, before[i], cancellationToken).ConfigureAwait(false);
                if (await RunModifiesAsync(copy, cancellationToken).ConfigureAwait(false) is { } modifyErrors)
                    errors.AddRange(modifyErrors);
                await PrepareMaterializeAsync(copy, key, known: null, cancellationToken).ConfigureAwait(false);
                await PrepareVectorsAsync(copy, key, known: null, cancellationToken).ConfigureAwait(false);
                writes.Add((key, copy, GetMeta(copy.GetType()), copy.GetETag()!));
            }

            string[]? etags;
            try
            {
                etags = await _tableAdapter.ReplaceManyAsync(_lottaCatalog.Name, writes, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                etags = null;   // the storage cannot write them together: one by one instead
            }
            if (etags == null)
            {
                foreach (var item in items)
                    ForgetPrevious(item.Copy!);
                return false;
            }

            _chainChanges.Value = changes;
            _chainErrors.Value = errors;
            for (var i = 0; i < items.Count; i++)
            {
                var copy = items[i].Copy!;
                var key = items[i].Key;
                Internal.EntityMapper.AnnotateAfterWrite(copy, key, etags[i]);
                if (copy is BlobFile blob)
                    blob.Database = this;
                _asyncRun.Value = new AsyncRun(this, items[i].Message.Hop, key) { RuleId = rule.Id };
                changes.Add(new ObjectChange { Type = copy.GetType(), Key = key, Kind = ChangeKind.Saved, Object = copy });
                await RunHandlersAsync(copy, copy.GetType(), TriggerKind.Saved, errors, cancellationToken).ConfigureAwait(false);
                // A save that left the object as it was gives queued rules nothing new to do.
                if (before[i] == null || before[i] != copy.GetJson())
                    await EnqueueAsync(copy, key, errors, cancellationToken).ConfigureAwait(false);
                ForgetPrevious(copy);
            }
            return true;
        }
        finally
        {
            _asyncRun.Value = null;
            _chainChanges.Value = null;
            _chainErrors.Value = null;
        }
    }

    /// <summary>
    /// Writes one copy's vectors as a run of the rule for that object alone would: the copy if its
    /// row is unchanged, otherwise the vectors put on the newest version.
    /// </summary>
    private async Task WriteVectorsAsync(QueuedRule rule, BatchItem item, CancellationToken cancellationToken)
    {
        _asyncRun.Value = new AsyncRun(this, item.Message.Hop, item.Key) { RuleId = rule.Id };
        try
        {
            // A fresh copy of the version that was read, with the embedded vectors put on it: the
            // batch's copy may already have been through the in-save Modify rules (and had its
            // JSON replaced by the batch write), so saving it would apply those rules twice.
            var original = item.Original!;
            var json = original.GetJson() ?? JsonSerializer.Serialize(original, original.GetType());
            var fresh = CopyOf(json, original.GetType(), item.Key, original.GetETag(), original);
            AddVectors(fresh, item.Copy!);
            try
            {
                await SaveAsync(fresh, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (ConcurrencyException)
            {
            }
            await WriteVectorsOntoLatestAsync(rule, item.Key, item.Copy!.GetType(), item.Copy, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _asyncRun.Value = null;
        }
    }

    /// <summary>
    /// Puts the vectors of <paramref name="copy"/> on the newest version of the object and saves
    /// it, trying again while others save it meanwhile; a save keeps a vector only where it is
    /// that of the version's text. Nothing is written if the object is gone.
    /// </summary>
    private async Task WriteVectorsOntoLatestAsync(QueuedRule rule, string key, Type type, object copy, CancellationToken cancellationToken)
    {
        for (var write = 0; write < MaxModifyWrites; write++)
        {
            var latest = await GetAsync(key, cancellationToken).ConfigureAwait(false);
            if (latest == null || latest.GetType() != type)
                return;   // deleted, or replaced by something else
            AddVectors(latest, copy);
            try
            {
                await SaveAsync(latest, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (ConcurrencyException)
            {
                // Saved again meanwhile: try the newest version.
            }
        }
        throw new InvalidOperationException(
            $"The vectors of rule '{rule.Id}' for '{key}' could not be written: the object was saved by others {MaxModifyWrites} times in a row.");
    }
}
