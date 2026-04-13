using Lotta;
using Lucene.Net.Linq.Mapping;

namespace Lotta.Tests;

public class Actor
{
    [Key]
    [Field(IndexMode.NotAnalyzed, Key = true)]
    public string Username { get; set; } = "";

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string DisplayName { get; set; } = "";

    public string Domain { get; set; } = "";
    public string AvatarUrl { get; set; } = "";
}

public class Note
{
    [Key(Strategy = KeyStrategy.DescendingTime)]
    public DateTimeOffset Published { get; set; }

    public string NoteId { get; set; } = "";

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
    [Field(Key = true)]
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
    [Field(Key = true)]
    public string NoteId { get; set; } = "";

    public string Domain { get; set; } = "";
    public string AuthorName { get; set; } = "";
    public string Content { get; set; } = "";
    public DateTimeOffset FlaggedAt { get; set; }
}

public class CycleA
{
    [Key]
    [Field(Key = true)]
    public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}

public class CycleB
{
    [Key]
    [Field(Key = true)]
    public string Id { get; set; } = "";
    public string Value { get; set; } = "";
}

public class FeedEntry
{
    [Key]
    [Field(Key = true)]
    public string FeedEntryId { get; set; } = "";

    [Field]
    public string Title { get; set; } = "";

    public string Domain { get; set; } = "";
    public DateTimeOffset Published { get; set; }
}

public class LogEntry
{
    [Key(Strategy = KeyStrategy.AscendingTime)]
    public DateTimeOffset Timestamp { get; set; }

    public string Source { get; set; } = "";
    public string Message { get; set; } = "";
    public string LogId { get; set; } = "";
}

public class OrderWithLines
{
    [Key]
    [Field(Key = true)]
    public string OrderId { get; set; } = "";

    public string TenantId { get; set; } = "";
    public decimal Total { get; set; }
    public List<OrderLine> Lines { get; set; } = new();
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
    [Field(Key = true)]
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
