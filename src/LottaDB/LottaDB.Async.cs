using System.Collections.Concurrent;
using System.Text.Json;
using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Lotta.Internal;

namespace Lotta;

// Queued handlers (OnAsync<T>): every save queues a small message for each matching handler, and
// a pump runs the handlers later, outside the write lease, retrying until they succeed.
// The queue is the only stored state; see OnAsync-Design.md.
public partial class LottaDB
{
    /// <summary>One registered queued handler and what this database instance knows about it.</summary>
    private sealed class AsyncHandler(AsyncRegistration registration, QueueClient queue, QueueClient poison)
    {
        public AsyncRegistration Registration { get; } = registration;
        public QueueClient Queue { get; } = queue;
        public QueueClient Poison { get; } = poison;

        /// <summary>
        /// For a <c>latest</c> handler: per object key, the highest sequence number this instance
        /// has queued. A message of this instance below that number has a newer one behind it.
        /// </summary>
        public ConcurrentDictionary<string, long> Newest { get; } = new(StringComparer.Ordinal);

        public volatile bool Paused;
        public int Running;
        public volatile string? LastError;
    }

    /// <summary>The queued handler that the current call chain is running inside, if any.</summary>
    private sealed record AsyncRun(LottaDB Db, string Name, int Priority, int Hop);

    private enum AsyncOutcome
    {
        /// <summary>No message was ready.</summary>
        Nothing,
        /// <summary>A message was taken and settled without running its handler.</summary>
        Settled,
        /// <summary>A handler was run.</summary>
        Ran,
    }

    /// <summary>How many notes of "newest message queued" one handler keeps before the table is emptied.</summary>
    private const int MaxNewestNotes = 100_000;
    private static readonly TimeSpan MaxLease = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan NeverExpires = TimeSpan.FromSeconds(-1);

    private static readonly AsyncLocal<AsyncRun?> _asyncRun = new();

    private AsyncHandler[] _asyncHandlers = [];
    /// <summary>The handlers grouped by priority, highest first.</summary>
    private AsyncHandler[][] _asyncTiers = [];
    private readonly ConcurrentDictionary<Type, AsyncHandler[]> _asyncByType = new();
    private readonly string _asyncSender = Guid.NewGuid().ToString("N");
    private long _asyncSeq;

    /// <summary>One handler run at a time for the database, whoever starts it (the loop or a drain).</summary>
    private readonly SemaphoreSlim _asyncRunGate = new(1, 1);
    private readonly SemaphoreSlim _asyncSignal = new(0);
    private readonly CancellationTokenSource _asyncCts = new();
    private Task? _asyncLoop;

    /// <summary>
    /// Makes the queues, so saves queue handler work. Called by the catalog as the last step of
    /// opening the database; never for a read-only database. <see cref="StartAsyncHandlers"/>
    /// then starts running them.
    /// </summary>
    internal void PrepareAsyncHandlers()
    {
        if (_config.AsyncRegistrations.Count == 0 || _config.ReadOnly || _asyncHandlers.Length > 0)
            return;
        var queues = _lottaCatalog.GetQueueServiceClient()
            ?? throw new NotSupportedException($"OnAsync handlers need queue storage, which {_lottaCatalog.StorageProvider} storage does not provide.");

        var handlers = new List<AsyncHandler>();
        foreach (var registration in _config.AsyncRegistrations)
        {
            var queue = queues.GetQueueClient(AsyncQueueNames.Queue(_lottaCatalog.Name, _databaseId, registration.Name));
            var poison = queues.GetQueueClient(AsyncQueueNames.Poison(_lottaCatalog.Name, _databaseId, registration.Name));
            // The names are hashes; say in words whose queue this is.
            var about = new Dictionary<string, string> { ["catalog"] = _lottaCatalog.Name, ["database"] = _databaseId, ["handler"] = registration.Name };
            queue.CreateIfNotExists(about);
            poison.CreateIfNotExists(about);
            handlers.Add(new AsyncHandler(registration, queue, poison));
        }
        _asyncTiers = [.. handlers.GroupBy(h => h.Registration.Priority).OrderByDescending(g => g.Key).Select(g => g.ToArray())];
        _asyncHandlers = [.. handlers];
    }

    /// <summary>
    /// Unless handlers are run by hand, starts running them. Called by the catalog once it has
    /// kept the fully open database, and only then: a database it does not keep (the catalog was
    /// disposed or deleted meanwhile) must run no handler.
    /// </summary>
    internal void StartAsyncHandlers()
    {
        if (_asyncHandlers.Length == 0 || _config.AsyncHandlers != AsyncHandlerMode.Auto || _asyncLoop != null)
            return;
        // Started without the caller's context, so nothing of a save chain leaks into the loop.
        using (ExecutionContext.SuppressFlow())
            _asyncLoop = Task.Run(() => AsyncLoopAsync(_asyncCts.Token));
    }

    /// <summary>
    /// Stops running queued handlers. A run in progress, the background loop's or a
    /// <see cref="DrainAsync"/>'s, has its token cancelled and its message stays queued with no
    /// failure counted. Waits up to 10 seconds in all for it to end, so the database is not torn
    /// down under it.
    /// </summary>
    private void StopAsyncHandlers()
    {
        _asyncCts.Cancel();
        // A handler that disposes its own database cannot wait for itself to finish.
        if (_asyncRun.Value?.Db == this)
            return;
        // One deadline for both waits, so a run that ignores its token holds Dispose 10 seconds, not 20.
        var waiting = System.Diagnostics.Stopwatch.StartNew();
        TimeSpan Left() => TimeSpan.FromTicks(Math.Max(0, (TimeSpan.FromSeconds(10) - waiting.Elapsed).Ticks));
        if (_asyncLoop != null)
        {
            try { _asyncLoop.Wait(Left()); } catch { }
        }
        // A drain's run holds the gate until its message is settled.
        if (_asyncRunGate.Wait(Left()))
            _asyncRunGate.Release();
    }

    // === Queuing ===

    /// <summary>
    /// Queues every handler that should run for a saved object. Called on each save path after
    /// the table write and the inline handlers, so the index is up to date before a queued
    /// handler can see the object. A queue that cannot be written to is reported in
    /// <paramref name="errors"/>; the save itself has already happened.
    /// </summary>
    private async Task EnqueueAsync(object entity, string key, List<Exception> errors, CancellationToken cancellationToken)
    {
        if (_asyncHandlers.Length == 0 || entity is JsonDocument)
            return;
        var matching = _asyncByType.GetOrAdd(entity.GetType(), type => [.. _asyncHandlers.Where(h => h.Registration.ObjectType.IsAssignableFrom(type))]);
        if (matching.Length == 0)
            return;

        // Inside a queued handler of this database only lower priorities are queued. That rules
        // out a handler queuing itself, and any circle of handlers queuing one another.
        var run = _asyncRun.Value;
        var within = run?.Db == this ? run : null;
        var hop = run is null ? 0 : run.Hop + 1;

        var queued = false;
        foreach (var handler in matching)
        {
            if (within != null && handler.Registration.Priority >= within.Priority)
                continue;
            try
            {
                var message = new AsyncMessage
                {
                    Type = entity.GetType().FullName ?? "",
                    Key = key,
                    Sender = _asyncSender,
                    Seq = Interlocked.Increment(ref _asyncSeq),
                    Hop = hop,
                };
                if (hop > _config.AsyncHandlerMaxHops)
                {
                    // A chain this long is handlers of several databases feeding one another; stop it where it can be seen.
                    message.LastError = $"Set aside without running: {hop} queued handlers in a row led to this save, more than AsyncHandlerMaxHops ({_config.AsyncHandlerMaxHops}).";
                    await handler.Poison.SendMessageAsync(message.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                await handler.Queue.SendMessageAsync(message.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
                queued = true;
                // Noted only once the message is in the queue: the note is what lets an older message be dropped.
                if (handler.Registration.Latest)
                {
                    if (handler.Newest.Count >= MaxNewestNotes)
                        handler.Newest.Clear();   // forgetting only costs extra runs
                    handler.Newest.AddOrUpdate(key, message.Seq, (_, seen) => Math.Max(seen, message.Seq));
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                errors.Add(new InvalidOperationException($"'{key}' was saved, but handler '{handler.Registration.Name}' could not be queued for it: {e.Message}", e));
            }
        }

        if (queued && _asyncSignal.CurrentCount == 0)
            _asyncSignal.Release();
    }

    /// <summary>The key an object was saved or loaded under.</summary>
    private string AsyncKeyOf(object entity) => entity.GetKey() ?? GetMeta(entity.GetType()).GetKey(entity);

    // === Running ===

    private async Task AsyncLoopAsync(CancellationToken cancellationToken)
    {
        var idle = TimeSpan.FromMilliseconds(100);
        while (!cancellationToken.IsCancellationRequested)
        {
            var outcome = AsyncOutcome.Nothing;
            try
            {
                await _asyncRunGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    outcome = await RunNextAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _asyncRunGate.Release();
                }
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
            }

            if (outcome != AsyncOutcome.Nothing)
            {
                idle = TimeSpan.FromMilliseconds(100);
                continue;
            }
            // Woken at once by a save in this process; otherwise look again for work from other
            // processes and for messages whose wait has run out, less and less often.
            try
            {
                await _asyncSignal.WaitAsync(idle, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            idle = TimeSpan.FromMilliseconds(Math.Min(idle.TotalMilliseconds * 2, 5000));
        }
    }

    /// <summary>
    /// Takes the next message: from the highest priority that has one ready, and among handlers
    /// of equal priority the oldest. Then settles it.
    /// </summary>
    private async Task<AsyncOutcome> RunNextAsync(CancellationToken cancellationToken)
    {
        foreach (var tier in _asyncTiers)
        {
            AsyncHandler? next = null;
            var active = tier.Where(h => !h.Paused).ToArray();
            if (active.Length == 1)
            {
                next = active[0];
            }
            else
            {
                var oldest = DateTimeOffset.MaxValue;
                foreach (var handler in active)
                {
                    var peeked = (await handler.Queue.PeekMessagesAsync(1, cancellationToken).ConfigureAwait(false)).Value;
                    if (peeked.Length > 0 && (peeked[0].InsertedOn ?? DateTimeOffset.MinValue) < oldest)
                    {
                        oldest = peeked[0].InsertedOn ?? DateTimeOffset.MinValue;
                        next = handler;
                    }
                }
            }
            if (next == null)
                continue;

            var timeout = next.Registration.Options.Timeout;
            var lease = timeout < MaxLease ? timeout : MaxLease;
            if (lease < TimeSpan.FromSeconds(1))
                lease = TimeSpan.FromSeconds(1);
            var received = (await next.Queue.ReceiveMessagesAsync(1, lease, cancellationToken).ConfigureAwait(false)).Value;
            if (received.Length == 0)
            {
                // With one handler in the tier this is how "nothing ready" shows; with several, someone else took it first.
                if (active.Length == 1)
                    continue;
                return AsyncOutcome.Settled;
            }
            return await SettleAsync(next, received[0], lease, cancellationToken).ConfigureAwait(false);
        }
        return AsyncOutcome.Nothing;
    }

    private async Task<AsyncOutcome> SettleAsync(AsyncHandler handler, QueueMessage received, TimeSpan lease, CancellationToken cancellationToken)
    {
        var registration = handler.Registration;
        var message = AsyncMessage.TryParse(received.Body.ToString());
        if (message == null)
        {
            // Not one of ours; keep it where it can be looked at rather than run or lose it.
            await handler.Poison.SendMessageAsync(received.Body.ToString(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
            await handler.Queue.DeleteMessageAsync(received.MessageId, received.PopReceipt, cancellationToken).ConfigureAwait(false);
            return AsyncOutcome.Settled;
        }

        // Latest: this instance queued a newer message for the same object after this one. That
        // one will run on the object as it then is, so this one has nothing left to do.
        if (registration.Latest && message.Sender == _asyncSender
            && handler.Newest.TryGetValue(message.Key, out var newest) && newest > message.Seq)
        {
            await handler.Queue.DeleteMessageAsync(received.MessageId, received.PopReceipt, cancellationToken).ConfigureAwait(false);
            return AsyncOutcome.Settled;
        }

        var entity = await GetAsync(message.Key, cancellationToken).ConfigureAwait(false);
        if (entity == null || !registration.ObjectType.IsInstanceOfType(entity))
        {
            // Deleted since, or replaced by something this handler is not for.
            await handler.Queue.DeleteMessageAsync(received.MessageId, received.PopReceipt, cancellationToken).ConfigureAwait(false);
            ForgetNewest(handler, message);
            return AsyncOutcome.Settled;
        }

        // The pop receipt changes every time the hold on the message is extended.
        var receipt = received.PopReceipt;
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        runCts.CancelAfter(registration.Options.Timeout);
        using var holdCts = new CancellationTokenSource();
        var holding = HoldAsync();

        async Task HoldAsync()
        {
            try
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromTicks(lease.Ticks / 2), holdCts.Token).ConfigureAwait(false);
                    receipt = (await handler.Queue.UpdateMessageAsync(received.MessageId, receipt, (string?)null, lease, holdCts.Token).ConfigureAwait(false)).Value.PopReceipt;
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (RequestFailedException)
            {
                // The hold was lost, so someone else may be given the message: stop this run.
                runCts.Cancel();
            }
        }

        Exception? failure = null;
        var interrupted = false;
        var contended = false;
        Interlocked.Increment(ref handler.Running);
        try
        {
            Task run;
            // Outside the write lease and with no save chain's context: the handler's own saves are ordinary root saves.
            using (ExecutionContext.SuppressFlow())
            {
                run = Task.Run(async () =>
                {
                    _asyncRun.Value = new AsyncRun(this, registration.Name, registration.Priority, message.Hop);
                    await registration.Invoke(entity, this, runCts.Token).ConfigureAwait(false);
                });
            }
            await run.ConfigureAwait(false);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            // The database is closing, or the drain was cancelled. Whatever the handler threw
            // then, its cancellation or a database disposed under it, is no fault of its own.
            interrupted = true;
        }
        catch (WriteLockUnavailableException)
        {
            contended = true;   // another process is writing; no fault of the handler's
        }
        catch (OperationCanceledException) when (runCts.IsCancellationRequested)
        {
            failure = new TimeoutException($"The run was stopped after {registration.Options.Timeout}, or its hold on the message was lost.");
        }
        catch (Exception e)
        {
            failure = e;
        }
        finally
        {
            Interlocked.Decrement(ref handler.Running);
            holdCts.Cancel();
            await holding.ConfigureAwait(false);
        }

        // What follows must happen even while closing, or the message would be worked on again for nothing.
        var settle = CancellationToken.None;
        try
        {
            if (interrupted)
            {
                // Given back at once, with no failure counted.
                await handler.Queue.UpdateMessageAsync(received.MessageId, receipt, (string?)null, TimeSpan.Zero, settle).ConfigureAwait(false);
            }
            else if (contended)
            {
                // No failure counted, so waiting on another process alone never sets the work
                // aside. But it is hidden for a wait that grows as a failure's does: given back
                // at once, it would be run again and again for as long as the lock is held.
                message.Waits++;
                await handler.Queue.UpdateMessageAsync(received.MessageId, receipt, message.ToText(), RetryWait(registration, message.Waits), settle).ConfigureAwait(false);
            }
            else if (failure == null)
            {
                await handler.Queue.DeleteMessageAsync(received.MessageId, receipt, settle).ConfigureAwait(false);
                ForgetNewest(handler, message);
                handler.LastError = null;
            }
            else
            {
                handler.LastError = failure.Message;
                message.Attempts++;
                message.LastError = failure.Message.Length <= 500 ? failure.Message : failure.Message[..500];
                if (message.Attempts >= registration.Options.MaxAttempts)
                {
                    await handler.Poison.SendMessageAsync(message.ToText(), null, NeverExpires, settle).ConfigureAwait(false);
                    await handler.Queue.DeleteMessageAsync(received.MessageId, receipt, settle).ConfigureAwait(false);
                    ForgetNewest(handler, message);
                }
                else
                {
                    // Kept in place with the failure written into it, hidden for a wait that doubles each time.
                    await handler.Queue.UpdateMessageAsync(received.MessageId, receipt, message.ToText(), RetryWait(registration, message.Attempts), settle).ConfigureAwait(false);
                }
            }
        }
        catch (RequestFailedException)
        {
            // The hold ran out before this could be recorded. The message is still queued and will be run again.
        }
        return AsyncOutcome.Ran;
    }

    /// <summary>How long a message is hidden before its next try: the retry delay, doubled for each try after the first, up to a cap.</summary>
    private static TimeSpan RetryWait(AsyncRegistration registration, int tries) =>
        TimeSpan.FromTicks(Math.Min(registration.Options.RetryDelay.Ticks << Math.Min(tries - 1, 20), MaxRetryDelay.Ticks));

    /// <summary>Drops the note that <paramref name="message"/> is the newest for its object, if it still is.</summary>
    private void ForgetNewest(AsyncHandler handler, AsyncMessage message)
    {
        if (handler.Registration.Latest && message.Sender == _asyncSender)
            handler.Newest.TryRemove(new KeyValuePair<string, long>(message.Key, message.Seq));
    }

    private AsyncHandler AsyncHandlerNamed(string name) =>
        _asyncHandlers.FirstOrDefault(h => h.Registration.Name == name)
        ?? throw new ArgumentException($"There is no queued handler named '{name}' in database '{_databaseId}'.", nameof(name));

    // === Public surface ===

    /// <summary>
    /// Runs queued handlers until no work is ready, or until <paramref name="max"/> handler runs
    /// have finished. Work that is waiting to be retried, after a failure or after finding the
    /// write lock held by another process, is not ready and is left for later. Use it in tests,
    /// in command-line tools, and with
    /// <see cref="AsyncHandlerMode.Manual"/>.
    /// </summary>
    /// <param name="max">The most handler runs to do; null for no limit.</param>
    /// <returns>How many handler runs were done, counting failed ones.</returns>
    /// <remarks>
    /// The token each handler is given is cancelled when <paramref name="cancellationToken"/> is,
    /// or when the database is disposed. A run stopped that way is not counted as a failure: its
    /// message stays queued and runs again later. The drain then throws
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

        // Closing the database stops a drain's run as it stops the background loop's.
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _asyncCts.Token);
        var runs = 0;
        while (max is null || runs < max)
        {
            stopping.Token.ThrowIfCancellationRequested();
            AsyncOutcome outcome;
            await _asyncRunGate.WaitAsync(stopping.Token).ConfigureAwait(false);
            try
            {
                outcome = await RunNextAsync(stopping.Token).ConfigureAwait(false);
            }
            finally
            {
                _asyncRunGate.Release();
            }
            // Here as well as at the top: a run interrupted by the cancellation is no run, even the
            // last one max allows, and its message is still queued.
            stopping.Token.ThrowIfCancellationRequested();
            if (outcome == AsyncOutcome.Nothing)
                break;
            if (outcome == AsyncOutcome.Ran)
                runs++;
        }
        return runs;
    }

    /// <summary>
    /// Stops this process from starting runs of a queued handler, or of all of them. Work keeps
    /// being queued, and a run already in progress finishes.
    /// </summary>
    /// <param name="name">The handler; null for all.</param>
    public void PauseHandlers(string? name = null) => SetPaused(name, true);

    /// <summary>Lets a paused handler, or all of them, run again.</summary>
    /// <param name="name">The handler; null for all.</param>
    public void ResumeHandlers(string? name = null)
    {
        SetPaused(name, false);
        if (_asyncSignal.CurrentCount == 0)
            _asyncSignal.Release();
    }

    private void SetPaused(string? name, bool paused)
    {
        foreach (var handler in name is null ? _asyncHandlers : [AsyncHandlerNamed(name)])
            handler.Paused = paused;
    }

    /// <summary>Where each queued handler stands: how much is waiting, running and set aside as failed.</summary>
    public async Task<IReadOnlyList<AsyncHandlerStatus>> GetHandlerStatusAsync(CancellationToken cancellationToken = default)
    {
        var statuses = new List<AsyncHandlerStatus>();
        foreach (var handler in _asyncHandlers)
        {
            var waiting = (await handler.Queue.GetPropertiesAsync(cancellationToken).ConfigureAwait(false)).Value.ApproximateMessagesCount;
            var poisoned = (await handler.Poison.GetPropertiesAsync(cancellationToken).ConfigureAwait(false)).Value.ApproximateMessagesCount;
            statuses.Add(new AsyncHandlerStatus(
                handler.Registration.Name, handler.Registration.Priority, handler.Registration.Latest,
                waiting, Volatile.Read(ref handler.Running), poisoned, handler.LastError, handler.Paused));
        }
        return statuses;
    }

    /// <summary>
    /// Puts a handler's failed work back in its queue with a fresh count of attempts.
    /// </summary>
    /// <returns>How many messages were put back.</returns>
    public async Task<int> RetryPoisonedAsync(string name, CancellationToken cancellationToken = default)
    {
        var handler = AsyncHandlerNamed(name);
        var moved = 0;
        while (true)
        {
            var batch = (await handler.Poison.ReceiveMessagesAsync(32, TimeSpan.FromSeconds(60), cancellationToken).ConfigureAwait(false)).Value;
            if (batch.Length == 0)
                break;
            foreach (var poisoned in batch)
            {
                var text = poisoned.Body.ToString();
                if (AsyncMessage.TryParse(text) is { } message)
                {
                    message.Attempts = 0;
                    message.LastError = null;
                    message.Hop = 0;
                    text = message.ToText();
                }
                await handler.Queue.SendMessageAsync(text, null, NeverExpires, cancellationToken).ConfigureAwait(false);
                await handler.Poison.DeleteMessageAsync(poisoned.MessageId, poisoned.PopReceipt, cancellationToken).ConfigureAwait(false);
                moved++;
            }
        }
        if (moved > 0 && _asyncSignal.CurrentCount == 0)
            _asyncSignal.Release();
        return moved;
    }

    /// <summary>
    /// Queues a handler for every stored object of its type, as if each had just been saved. For
    /// when the handler itself has changed and its work should be done again. This reads every
    /// object in the database, and the work it queues may take long.
    /// </summary>
    /// <returns>How many objects the handler was queued for.</returns>
    public async Task<int> RequeueAsync(string name, CancellationToken cancellationToken = default)
    {
        var handler = AsyncHandlerNamed(name);
        var queued = 0;
        await foreach (var entity in GetManyAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            if (entity is null || !handler.Registration.ObjectType.IsInstanceOfType(entity))
                continue;
            var message = new AsyncMessage
            {
                Type = entity.GetType().FullName ?? "",
                Key = AsyncKeyOf(entity),
                Sender = _asyncSender,
                Seq = Interlocked.Increment(ref _asyncSeq),
            };
            await handler.Queue.SendMessageAsync(message.ToText(), null, NeverExpires, cancellationToken).ConfigureAwait(false);
            if (handler.Registration.Latest)
                handler.Newest.AddOrUpdate(message.Key, message.Seq, (_, seen) => Math.Max(seen, message.Seq));
            queued++;
        }
        if (queued > 0 && _asyncSignal.CurrentCount == 0)
            _asyncSignal.Release();
        return queued;
    }

    // === Clean-up ===

    /// <summary>Empties every handler's queue and poison queue; part of resetting the database.</summary>
    private async Task ClearAsyncQueuesAsync(CancellationToken cancellationToken)
    {
        foreach (var handler in _asyncHandlers)
        {
            await handler.Queue.ClearMessagesAsync(cancellationToken).ConfigureAwait(false);
            await handler.Poison.ClearMessagesAsync(cancellationToken).ConfigureAwait(false);
            handler.Newest.Clear();
        }
    }

    /// <summary>Deletes the database's queues, including those of handlers that are no longer registered.</summary>
    private async Task DeleteAsyncQueuesAsync(CancellationToken cancellationToken)
    {
        if (_lottaCatalog.GetQueueServiceClient() is { } queues)
            await AsyncQueueNames.DeleteQueuesAsync(queues, AsyncQueueNames.DatabasePrefix(_lottaCatalog.Name, _databaseId), cancellationToken).ConfigureAwait(false);
    }
}
