namespace Lotta;

/// <summary>
/// Indexes a <c>DateTime</c> or <c>DateTimeOffset</c> property for temporal queries with
/// Iciclecreek.Lucene.Net.Temporal: whole or partial dates, so "created on Christmas in any
/// year" (<c>XXXX-12-25</c>) or "on a Monday morning" (<c>XXXX-WXX-1TMO</c>) match, as well as
/// ranges. Its fields are named after the property; query them with the strategy
/// <see cref="LottaDB.TemporalStrategy{T}(string)"/> returns.
/// <code>
/// [Temporal] public DateTime Sent { get; set; }
/// </code>
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class TemporalAttribute : Attribute
{
}

/// <summary>
/// Indexes a property of TIMEX values (a string, or a collection of strings, as an extractor
/// such as Microsoft.Recognizers.Text gives them) for temporal queries with
/// Iciclecreek.Lucene.Net.Temporal, so partial and recurring references match: "every Thursday"
/// (<c>XXXX-WXX-4</c>) matches a query for any Thursday; "the last week of every month"
/// (<c>XXXX-XX-W05</c>) matches the last week of this month. The values are resolved against
/// <see cref="Anchor"/>, the time they were said. Query with the strategy
/// <see cref="LottaDB.TemporalStrategy{T}(string)"/> returns.
/// <code>
/// [Timex(Anchor = nameof(Sent))] public List&lt;string&gt; Mentions { get; set; }
/// </code>
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class TimexAttribute : Attribute
{
    /// <summary>The <c>DateTime</c> or <c>DateTimeOffset</c> property the values were said on.</summary>
    public required string Anchor { get; set; }
}
