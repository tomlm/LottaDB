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

    /// <summary>
    /// Wall-clock components (day, weekday, time of day) are those of the local time zone. An
    /// unparseable TIMEX value is skipped rather than thrown: one bad value must not leave its
    /// object out of the index, or stop a rebuild (and with it a schema-change database open).
    /// <see cref="AddTo"/> also skips the values this option misses.
    /// </summary>
    internal static TemporalOptions Options { get; } = new() { DefaultZone = TimeZoneInfo.Local, IgnoreInvalidReferences = true };

    /// <summary>The strategy of a temporal property.</summary>
    public static TemporalStrategy StrategyFor(IndexedTypeProperty indexed) =>
        Strategies.GetOrAdd(indexed.Property, _ => indexed.Temporal == TemporalMode.Timex
            ? new TimexStrategy(indexed.Property.Name, Options)
            : new TemporalDateStrategy(indexed.Property.Name, Options));

    /// <summary>
    /// Adds the temporal fields of <paramref name="source"/>'s temporal properties to <paramref name="target"/>.
    /// A TIMEX value whose fields cannot be made (unparseable, or an impossible date such as
    /// <c>(XXXX-02-30,XXXX-03-01,P1D)</c>) is dropped, keeping the property's other values; a date
    /// that cannot be indexed drops that property only. The object and its other fields are always indexed.
    /// </summary>
    public static void AddTo(object source, IEnumerable<IndexedTypeProperty> temporal, Document target)
    {
        foreach (var indexed in temporal)
        {
            var strategy = StrategyFor(indexed);
            Field[] fields = [];
            if (strategy is TemporalDateStrategy date && AsDateTimeOffset(indexed.Property.GetValue(source)) is { } when)
            {
                try
                {
                    fields = date.CreateIndexableFields(when);
                }
                catch (Exception e) when (e is FormatException or ArgumentOutOfRangeException)
                {
                    // A date the library cannot index: skip this property, keep the object.
                }
            }
            else if (strategy is TimexStrategy timex
                && AsDateTimeOffset(indexed.TimexAnchor!.GetValue(source)) is { } anchor
                && Values(indexed.Property.GetValue(source)) is { Count: > 0 } values)
            {
                var context = new TemporalContext(Options.DefaultZone, anchor);
                // IgnoreInvalidReferences only catches parse errors; an impossible day in a
                // recurring range (XXXX-02-30) throws ArgumentOutOfRangeException later. So each
                // value is tried alone first, and only those that index are made together, in one
                // call, so the anchor and version fields are added exactly once.
                var good = values.Select(v => new TimexReference(v)).Where(r => Indexes(timex, r, context)).ToList();
                if (good.Count > 0)
                    fields = timex.CreateIndexableFields(good, context);
            }
            foreach (var field in fields)
                target.Add(field);
        }
    }

    private static bool Indexes(TimexStrategy timex, TimexReference reference, TemporalContext context)
    {
        try
        {
            timex.CreateIndexableFields(reference, context);
            return true;
        }
        catch (Exception e) when (e is FormatException or ArgumentOutOfRangeException)
        {
            return false;
        }
    }

    private static List<string> Values(object? value) => value switch
    {
        string one when one.Length > 0 => [one],
        IEnumerable<string> many => [.. many.Where(v => !string.IsNullOrWhiteSpace(v))],
        _ => [],
    };

    /// <summary>
    /// A date as an instant, or null for a default (unset) date. A <c>DateTime</c> of unspecified
    /// kind is taken as local, as LottaDB's UtcDateTimeConverter and table storage take it.
    /// </summary>
    private static DateTimeOffset? AsDateTimeOffset(object? value) => value switch
    {
        DateTimeOffset offset when offset != default => offset,
        DateTime date when date != default => new DateTimeOffset(date),
        _ => null,
    };
}
