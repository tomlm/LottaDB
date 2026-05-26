namespace Lotta.Benchmarks;

/// <summary>
/// Synthetic document used in benchmarks.
/// </summary>
[DefaultSearch(nameof(Content))]
public class BenchmarkDocument
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable]
    public string Title { get; set; } = "";

    /// <summary>Content field — some documents include the word "dog" for SearchAsync tests.</summary>
    [Queryable]
    public string Content { get; set; } = "";

    [Queryable]
    public int Counter { get; set; }
}
