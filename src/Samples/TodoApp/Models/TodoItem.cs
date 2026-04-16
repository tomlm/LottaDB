using Lotta;

namespace TodoApp.Models;

/// <summary>
/// A single todo. [Key] with Auto mode gives each new item a ULID row key.
/// [Queryable] on Title/Notes makes them Lucene-indexed (analyzed) so the
/// search box can do token/prefix matches. IsDone is NotAnalyzed so it can
/// be filtered exactly.
/// </summary>
public class TodoItem
{
    [Key(Mode = KeyMode.Auto)]
    public string Id { get; set; } = "";

    [Queryable]
    public string Title { get; set; } = "";

    [Queryable]
    public string Notes { get; set; } = "";

    [Queryable]
    public bool IsDone { get; set; }

    [Queryable]
    public DateTimeOffset Created { get; set; } = DateTimeOffset.UtcNow;
    
    [Queryable]
    public DateTimeOffset? CompletedAt { get; set; }
}
