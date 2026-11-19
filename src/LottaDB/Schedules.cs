using System.Runtime.CompilerServices;
using Cronos;
using Lotta.Internal;

namespace Lotta;

/// <summary>The work a schedule does (<see cref="RuleSet.Every(TimeSpan)"/>). It is for no object, so it is given none.</summary>
/// <param name="context">
/// The database, the schedule's name, and a key for dropping duplicates:
/// <see cref="RuleContext.IdempotencyKey"/> is the same for one scheduled time, whoever runs it.
/// <see cref="RuleContext.Attempt"/> is always 1.
/// </param>
/// <param name="cancellationToken">
/// Cancelled when the run outlasts <see cref="ILottaConfiguration.RuleTimeout"/>, or when the
/// database is disposed.
/// </param>
public delegate Task ScheduleHandler(RuleContext context, CancellationToken cancellationToken);

/// <summary>
/// A schedule, as <see cref="RuleSet.Every(TimeSpan)"/> starts it. Give it its work with
/// <see cref="Do(string, ScheduleHandler)"/>; a schedule with no work makes opening the database throw.
/// </summary>
public sealed class ScheduleBuilder
{
    private readonly LottaConfiguration _config;
    private readonly Func<DateTimeOffset, DateTimeOffset> _next;
    private bool _used;

    internal ScheduleBuilder(LottaConfiguration config, Func<DateTimeOffset, DateTimeOffset> next)
    {
        _config = config;
        _next = next;
        _config.SchedulesWithoutWork++;
    }

    /// <summary>
    /// Do this work each time the schedule comes round. One process runs it: whichever first
    /// claims the time, in the schedule's stored record (a <see cref="RuleSchedule"/> named after
    /// the schedule). Times missed while nothing was running are made up for with one run, not one
    /// for each. A run that fails is recorded (<see cref="RuleSchedule.LastError"/>,
    /// <see cref="LiveRuleSet.StatusAsync"/>) and is not retried: the work runs again at the next time.
    /// </summary>
    /// <param name="name">
    /// Identifies the schedule across runs of the program: <c>schedule/nightly-cleanup</c>. 3 to
    /// 40 lower-case letters and digits, with single hyphens between them, unique among schedules.
    /// </param>
    /// <param name="handler">The work to do.</param>
    /// <exception cref="ArgumentException">The name is not a valid rule name, or is already used for a schedule.</exception>
    public ScheduleBuilder Do(string name, ScheduleHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _config.AddSchedule(name, _next, handler);
        if (!_used)
        {
            _used = true;
            _config.SchedulesWithoutWork--;
        }
        return this;
    }

    /// <summary>Do this work each time the schedule comes round, with the schedule named after the handler method: <c>NightlyCleanup</c> gives <c>nightly-cleanup</c>.</summary>
    /// <param name="handler">A method. A lambda has no name, so give one with the other overload.</param>
    /// <param name="handlerExpression">Filled in by the compiler with the source text of <paramref name="handler"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="handler"/> is not a method name, or the name made from it is not valid or is already used.</exception>
    public ScheduleBuilder Do(ScheduleHandler handler,
        [CallerArgumentExpression(nameof(handler))] string? handlerExpression = null)
        => Do(RuleQueueNames.RuleNameFromExpression(handlerExpression, nameof(Do)), handler);
}

/// <summary>
/// The stored record of one schedule: when it next runs, and how its last run went. LottaDB keeps
/// one per schedule, under the key <see cref="KeyOf"/> gives, and reads it like any object:
/// <c>await db.GetAsync&lt;RuleSchedule&gt;(RuleSchedule.KeyOf("nightly-cleanup"))</c>.
/// </summary>
public class RuleSchedule
{
    /// <summary>The key of the record: the schedule's name with a prefix, <c>_schedule-nightly-cleanup</c>.</summary>
    [Key]
    public string Id { get; set; } = "";

    /// <summary>When the schedule next runs. A process claims a run by moving this on with a conditional write.</summary>
    public DateTimeOffset NextRun { get; set; }

    /// <summary>When the last run was started; null before the first.</summary>
    public DateTimeOffset? LastRun { get; set; }

    /// <summary>What the last run's failure said; null when it succeeded.</summary>
    public string? LastError { get; set; }

    /// <summary>The key of the record of the schedule with this name.</summary>
    public static string KeyOf(string name) => $"_schedule-{name}";
}

/// <summary>A schedule as registered.</summary>
internal sealed class ScheduleSpec
{
    /// <summary><c>schedule/nightly-cleanup</c>: what status names the schedule by.</summary>
    public required string Id { get; init; }

    public required string Name { get; init; }

    /// <summary>The first time the schedule comes round after a given time.</summary>
    public required Func<DateTimeOffset, DateTimeOffset> Next { get; init; }

    public required ScheduleHandler Handler { get; init; }
}

internal static class ScheduleTimes
{
    /// <summary>A fixed interval: each time is one interval after the run before it was claimed.</summary>
    public static Func<DateTimeOffset, DateTimeOffset> Interval(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        return after => after + interval;
    }

    /// <summary>A cron expression of five fields, or six with seconds first, in a time zone (UTC when none is given).</summary>
    /// <exception cref="ArgumentException">The text is not a cron expression.</exception>
    public static Func<DateTimeOffset, DateTimeOffset> Cron(string cron, TimeZoneInfo? timeZone)
    {
        ArgumentNullException.ThrowIfNull(cron);
        CronExpression expression;
        try
        {
            var fields = cron.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Length;
            expression = CronExpression.Parse(cron, fields == 6 ? CronFormat.IncludeSeconds : CronFormat.Standard);
        }
        catch (CronFormatException e)
        {
            throw new ArgumentException($"'{cron}' is not a cron expression: {e.Message}", nameof(cron), e);
        }
        var zone = timeZone ?? TimeZoneInfo.Utc;
        // A time that never comes again (a date in the past) is as far off as can be.
        return after => expression.GetNextOccurrence(after, zone) ?? DateTimeOffset.MaxValue;
    }
}
