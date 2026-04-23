namespace Lotta.Tests;

public class Actor
{
    [Key]
    public string Username { get; set; } = "";

    [Queryable]
    public string DisplayName { get; set; } = "";

    public string Domain { get; set; } = "";
    public string AvatarUrl { get; set; } = "";

    [Queryable]
    public int Counter { get; set; }

    [Queryable]
    public DateTime CreatedAt { get; set; }

    [Queryable]
    public DateTimeOffset LastSeenAt { get; set; }
}

public class Note
{
    [Key]
    public string NoteId { get; set; } = "";

    public DateTimeOffset Published { get; set; }

    [Queryable]
    public string AuthorId { get; set; } = "";

    [Queryable]
    public string Content { get; set; } = "";

    public string NotQueryable { get; set; } = "";

    public string Domain { get; set; } = "";
    public IList<string> Tags { get; set; } = new List<string>();
}

public class NoteView
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string NoteId { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string AuthorUsername { get; set; } = "";

    [Queryable]
    public string AuthorDisplay { get; set; } = "";

    public string AvatarUrl { get; set; } = "";
    public string Domain { get; set; } = "";

    [Queryable]
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

    [Queryable(QueryableMode.NotAnalyzed)]
    public string NoteViewId { get; set; } = "";

    [Queryable]
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

// Model using Lucene.Net.Linq's [Field, VectorField] attributes directly
public class LuceneVectorDoc
{
    [Key]
    public string Id { get; set; } = "";

    [Lucene.Net.Linq.Mapping.Field, Lucene.Net.Linq.Mapping.VectorField]
    public string Title { get; set; } = "";

    [Lucene.Net.Linq.Mapping.Field]
    public string Category { get; set; } = "";
}

// === Bare models (no attributes at all) — configured entirely via fluent API ===

public class BareActor
{
    public string Username { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string Domain { get; set; } = "";
    public string AvatarUrl { get; set; } = "";
}

public class BareNote
{
    public string NoteId { get; set; } = "";
    public DateTimeOffset Published { get; set; }
    public string AuthorId { get; set; } = "";
    public string Content { get; set; } = "";
}

// Polymorphism test hierarchy
public class BaseEntity
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable]
    public string Name { get; set; } = "";
}

public class Person : BaseEntity
{
    [Queryable]
    public string Email { get; set; } = "";
}

public class Employee : Person
{
    [Queryable]
    public string Department { get; set; } = "";
}

// Vector search test model
public class VectorNote
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable(Vector = true)]
    public string Title { get; set; } = "";

    [Queryable]
    public string Category { get; set; } = "";

    [Queryable(Vector = true)]
    public string Body { get; set; } = "";
}

// Bare vector model for fluent-only config
public class BareVectorNote
{
    public string Id { get; set; } = "";
    public string Title { get; set; } = "";
    public string Category { get; set; } = "";
}
