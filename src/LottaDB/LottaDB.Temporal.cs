using Iciclecreek.Lucene.Net.Temporal;
using Lotta.Internal;

namespace Lotta;

// Temporal properties ([Temporal], [Timex], .Temporal(), .Timex()) are indexed with
// Iciclecreek.Lucene.Net.Temporal's strategies; the same strategy builds their queries.
public partial class LottaDB
{
    /// <summary>
    /// The strategy a temporal property of <typeparamref name="T"/> is indexed with, to build
    /// queries for it: a <see cref="TimexStrategy"/> for TIMEX values, a
    /// <see cref="TemporalDateStrategy"/> for a date. Use the queries in a search:
    /// <code>
    /// var mentions = db.TemporalStrategy&lt;Email&gt;(nameof(Email.Mentions));
    /// db.Search&lt;Email&gt;().Where(mentions.MakeQuery("2026-11-W05", TemporalContext.Now(TimeZoneInfo.Local)))
    /// </code>
    /// Dates are read in the machine's local time zone: query with a context in that zone.
    /// </summary>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> is not stored, or the property is not temporal.</exception>
    public TemporalStrategy TemporalStrategy<T>(string property)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(property);
        if (!_metadata.TryGetValue(typeof(T), out var meta))
            throw new InvalidOperationException($"{typeof(T).Name} is not stored in this database.");
        var indexed = meta.IndexedProperties.FirstOrDefault(p => p.Property.Name == property && p.Temporal != TemporalMode.None)
            ?? throw new InvalidOperationException($"{typeof(T).Name}.{property} is not temporal: mark it [Temporal] or [Timex], or .Temporal() or .Timex().");
        return TemporalFields.StrategyFor(indexed);
    }
}
