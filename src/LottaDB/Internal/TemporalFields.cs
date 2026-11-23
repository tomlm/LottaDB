using System.Collections.Concurrent;
using System.Reflection;
using Iciclecreek.Lucene.Net.Temporal;
using Lucene.Net.Documents;

namespace Lotta.Internal;

/// <summary>
/// The temporal fields of a property declared <c>[Temporal]</c> or <c>[Timex]</c> (or
/// <c>.Temporal()</c> / <c>.Timex()</c>): made with Iciclecreek.Lucene.Net.Temporal's strategies,
/// named after the property, in the machine's local time zone. Indexing and querying use the
/// same strategy for a property, so their field names and options always agree.
/// </summary>
internal static class TemporalFields
{
    private static readonly ConcurrentDictionary<PropertyInfo, TemporalStrategy> Strategies = new();

    /// <summary>Wall-clock components (day, weekday, time of day) are those of the local time zone.</summary>
    internal static TemporalOptions Options { get; } = new() { DefaultZone = TimeZoneInfo.Local };

    /// <summary>The strategy of a temporal property.</summary>
    public static TemporalStrategy StrategyFor(IndexedTypeProperty indexed) =>
        Strategies.GetOrAdd(indexed.Property, _ => indexed.Temporal == TemporalMode.Timex
            ? new TimexStrategy(indexed.Property.Name, Options)
            : new TemporalDateStrategy(indexed.Property.Name, Options));

    /// <summary>Adds the temporal fields of <paramref name="source"/>'s temporal properties to <paramref name="target"/>.</summary>
    public static void AddTo(object source, IEnumerable<IndexedTypeProperty> temporal, Document target)
    {
        foreach (var indexed in temporal)
        {
            var strategy = StrategyFor(indexed);
            Field[] fields = [];
            if (strategy is TemporalDateStrategy date && AsDateTimeOffset(indexed.Property.GetValue(source)) is { } when)
            {
                fields = date.CreateIndexableFields(when);
            }
            else if (strategy is TimexStrategy timex
                && AsDateTimeOffset(indexed.TimexAnchor!.GetValue(source)) is { } anchor
                && Values(indexed.Property.GetValue(source)) is { Count: > 0 } values)
            {
                fields = timex.CreateIndexableFields(values.Select(v => new TimexReference(v)), new TemporalContext(Options.DefaultZone, anchor));
            }
            foreach (var field in fields)
                target.Add(field);
        }
    }

    private static List<string> Values(object? value) => value switch
    {
        string one when one.Length > 0 => [one],
        IEnumerable<string> many => [.. many.Where(v => !string.IsNullOrWhiteSpace(v))],
        _ => [],
    };

    /// <summary>A date as an instant: a <c>DateTime</c> of unspecified kind is taken as UTC, as LottaDB stores dates.</summary>
    private static DateTimeOffset? AsDateTimeOffset(object? value) => value switch
    {
        DateTimeOffset offset => offset,
        DateTime { Kind: DateTimeKind.Local } local => new DateTimeOffset(local),
        DateTime other when other != default => new DateTimeOffset(DateTime.SpecifyKind(other, DateTimeKind.Utc)),
        _ => null,
    };
}
