namespace Lotta;

// Schedules (Rules.Every(...).Then(...)): work done at times, for no object. Each schedule has one
// stored record, a RuleSchedule, holding its next time. A process runs the work after moving that
// time on with a conditional write, so of several processes only one runs each time.
public partial class LottaDB
{
    /// <summary>The longest the schedule loop sleeps: how soon it notices a time moved by another process, or a new record.</summary>
    private static readonly TimeSpan MaxScheduleSleep = TimeSpan.FromSeconds(30);

    private Task? _scheduleLoop;

    /// <summary>The time schedules go by. Tests move it; nothing else sets it.</summary>
    internal Func<DateTimeOffset> ScheduleClock { get; set; } = () => DateTimeOffset.UtcNow;

    private bool HasSchedules => _config.Schedules.Count > 0 && !_config.ReadOnly;

    /// <summary>Runs schedules as their times come, for as long as the database is open.</summary>
    private async Task ScheduleLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var sleep = MaxScheduleSleep;
            try
            {
                var (_, next) = await RunDueSchedulesAsync(max: null, cancellationToken).ConfigureAwait(false);
                var wait = next - ScheduleClock();
                if (wait < sleep)
                    sleep = wait > TimeSpan.Zero ? wait : TimeSpan.Zero;
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
                // Storage is unreachable for the moment, or another process holds the write lock: look again shortly.
                sleep = TimeSpan.FromSeconds(1);
            }

            try
            {
                // Not a tight loop when a time has just come: the record's clock and this one may differ by a little.
                await Task.Delay(sleep < TimeSpan.FromMilliseconds(50) ? TimeSpan.FromMilliseconds(50) : sleep, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// Runs every schedule whose time has come and that this process manages to claim.
    /// </summary>
    /// <param name="max">The most runs to do; null for no limit.</param>
    /// <returns>How many schedules were run, failed runs included, and the earliest time any schedule next comes round.</returns>
    private async Task<(int Runs, DateTimeOffset Next)> RunDueSchedulesAsync(int? max, CancellationToken cancellationToken)
    {
        var runs = 0;
        var earliest = DateTimeOffset.MaxValue;
        foreach (var schedule in _config.Schedules)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var now = ScheduleClock();
            var key = RuleSchedule.KeyOf(schedule.Name);
            var record = await GetAsync<RuleSchedule>(key, cancellationToken).ConfigureAwait(false);
            var following = schedule.Next(now);

            if (record == null)
            {
                // First seen: it runs when its time first comes round, not at once.
                await SaveAsync(new RuleSchedule { Id = key, NextRun = following }, cancellationToken).ConfigureAwait(false);
                earliest = Min(earliest, following);
                continue;
            }

            var due = record.NextRun <= now && !(max is { } most && runs >= most);
            // Later than the schedule can now give: the schedule was changed to come round sooner.
            if (!due && record.NextRun <= following)
            {
                earliest = Min(earliest, record.NextRun);
                continue;
            }

            var scheduled = record.NextRun;
            record.NextRun = following;
            if (due)
                record.LastRun = now;
            try
            {
                // Conditional on the record as read: of several processes, one gets to move the time on.
                await SaveAsync(record, cancellationToken).ConfigureAwait(false);
            }
            catch (ConcurrencyException)
            {
                // Claimed by another process, which also set the next time.
                earliest = Min(earliest, following);
                continue;
            }
            earliest = Min(earliest, following);
            if (!due)
                continue;

            runs++;
            var error = await RunScheduleAsync(schedule, scheduled, cancellationToken).ConfigureAwait(false);
            if (error != record.LastError)
            {
                // Best effort: the record says how the last run went.
                try
                {
                    await ChangeAsync<RuleSchedule>(key, stored => { stored.LastError = error; }, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                }
            }
        }
        return (runs, earliest);

        static DateTimeOffset Min(DateTimeOffset a, DateTimeOffset b) => a < b ? a : b;
    }

    /// <summary>Runs one schedule's work for one of its times.</summary>
    /// <returns>What its failure said; null when it succeeded.</returns>
    private async Task<string?> RunScheduleAsync(ScheduleSpec schedule, DateTimeOffset scheduled, CancellationToken cancellationToken)
    {
        var state = _ruleStates.GetOrAdd(schedule.Id, _ => new RuleState());
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_config.RuleTimeout);
        Interlocked.Increment(ref state.Running);
        try
        {
            // The scheduled time stands in for an object's ETag: one idempotency key per time.
            var context = new RuleContext(this, schedule.Name, schedule.Id, attempt: 1, item: null, etag: scheduled.UtcDateTime.ToString("O"));
            Task run;
            // With no save chain's context: the handler's own saves are ordinary root saves.
            using (ExecutionContext.SuppressFlow())
                run = Task.Run(() => schedule.Handler(context, timeout.Token), CancellationToken.None);
            await run.ConfigureAwait(false);
            state.LastError = null;
            return null;
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);   // closing: no fault of the work's
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            return state.LastError = $"The run was stopped after {_config.RuleTimeout}.";
        }
        catch (Exception e)
        {
            return state.LastError = e.Message;
        }
        finally
        {
            Interlocked.Decrement(ref state.Running);
        }
    }
}
