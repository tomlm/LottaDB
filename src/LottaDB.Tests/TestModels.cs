using Lotta;
using Lucene.Net.Linq.Mapping;

namespace Lotta.Tests;

public class Actor
{
    [Key]
    public string Username { get; set; } = "";

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string DisplayName { get; set; } = "";

    public string Domain { get; set; } = "";
    public string AvatarUrl { get; set; } = "";
}

public class Note
{
    [Key]
    public string NoteId { get; set; } = "";

    public DateTimeOffset Published { get; set; }

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string AuthorId { get; set; } = "";

    [Field]
    public string Content { get; set; } = "";

    public string Domain { get; set; } = "";
    public IList<string> Tags { get; set; } = new List<string>();
}

public class NoteView
{
    [Key]
    public string Id { get; set; } = "";

    [Field(IndexMode.NotAnalyzed)]
    public string NoteId { get; set; } = "";

    [Field(IndexMode.NotAnalyzed)]
    public string AuthorUsername { get; set; } = "";

    [Field]
    public string AuthorDisplay { get; set; } = "";

    public string AvatarUrl { get; set; } = "";
    public string Domain { get; set; } = "";

    [Field]
    public string Content { get; set; } = "";

    public DateTimeOffset Published { get; set; }
    public IList<string> Tags { get; set; } = new List<string>();
}

public class ModerationView
{
    [Key]
    public string NoteId { get; set; } = "";

    public string Domain { get; set; } = "";
    public string AuthorName { get; set; } = "";
    public string Content { get; set; } = "";
    public DateTimeOffset FlaggedAt { get; set; }
}

public class CycleA
{
    [Key]
    public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}

public class CycleB
{
    [Key]
    public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}

public class FeedEntry
{
    [Key]
    public string Id { get; set; } = "";

    [Field(IndexMode.NotAnalyzed)]
    public string NoteViewId { get; set; } = "";

    [Field]
    public string Title { get; set; } = "";

    public string Domain { get; set; } = "";
    public DateTimeOffset Published { get; set; }
}

public class LogEntry
{
    [Key(Mode = KeyMode.Auto)]
    public string Id { get; set; } = "";

    public DateTimeOffset Timestamp { get; set; }
    public string Source { get; set; } = "";
    public string Message { get; set; } = "";
}

public class OrderWithLines
{
    [Key]
    public string OrderId { get; set; } = "";

    [IgnoreField]
    public string TenantId { get; set; } = "";
    [IgnoreField]
    public decimal Total { get; set; }
    [IgnoreField]
    public List<OrderLine> Lines { get; set; } = new();
    [IgnoreField]
    public Dictionary<string, string> Metadata { get; set; } = new();
}

public class OrderLine
{
    public string ProductId { get; set; } = "";
    public int Quantity { get; set; }
    public decimal Price { get; set; }
}

// Polymorphism test hierarchy
public class BaseEntity
{
    [Key]
    public string Id { get; set; } = "";

    [Field]
    public string Name { get; set; } = "";
}

public class Person : BaseEntity
{
    [Field]
    public string Email { get; set; } = "";
}

public class Employee : Person
{
    [Field]
    public string Department { get; set; } = "";
}
