using LottaDB;
using Lucene.Net.Linq.Mapping;

namespace LottaDB.Tests;

public class Actor
{
    [PartitionKey]
    public string Domain { get; set; } = "";

    [RowKey]
    public string Username { get; set; } = "";

    [Field(IndexMode.NotAnalyzed)]
    public string DisplayName { get; set; } = "";

    public string AvatarUrl { get; set; } = "";
}

public class Note
{
    [PartitionKey]
    public string Domain { get; set; } = "";

    [RowKey(Strategy = RowKeyStrategy.DescendingTime)]
    public DateTimeOffset Published { get; set; }

    [Field(Key = true)]
    public string NoteId { get; set; } = "";

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string AuthorId { get; set; } = "";

    [Field]
    public string Content { get; set; } = "";

    public List<string> Tags { get; set; } = new();
}

public class NoteView
{
    [PartitionKey]
    public string Domain { get; set; } = "";

    [RowKey]
    public string NoteId { get; set; } = "";

    [Field(IndexMode.NotAnalyzed)]
    public string AuthorUsername { get; set; } = "";

    [Field]
    public string AuthorDisplay { get; set; } = "";

    public string AvatarUrl { get; set; } = "";

    [Field]
    public string Content { get; set; } = "";

    public DateTimeOffset Published { get; set; }

    public string[] Tags { get; set; } = Array.Empty<string>();
}

public class ModerationView
{
    [PartitionKey]
    public string Domain { get; set; } = "";

    [RowKey]
    public string NoteId { get; set; } = "";

    public string AuthorName { get; set; } = "";
    public string Content { get; set; } = "";
    public DateTimeOffset FlaggedAt { get; set; }
}

// For cycle detection tests
public class CycleA
{
    [PartitionKey]
    public string Partition { get; set; } = "default";

    [RowKey]
    public string Id { get; set; } = "";

    public string Value { get; set; } = "";
}

public class CycleB
{
    [PartitionKey]
    public string Partition { get; set; } = "default";

    [RowKey]
    public string Id { get; set; } = "";

    public string Value { get; set; } = "";
}

// For cascading CreateView tests
public class FeedEntry
{
    [PartitionKey]
    public string Domain { get; set; } = "";

    [RowKey]
    public string FeedEntryId { get; set; } = "";

    [Field]
    public string Title { get; set; } = "";

    public DateTimeOffset Published { get; set; }
}

// For ascending time tests
public class LogEntry
{
    [PartitionKey]
    public string Source { get; set; } = "";

    [RowKey(Strategy = RowKeyStrategy.AscendingTime)]
    public DateTimeOffset Timestamp { get; set; }

    public string Message { get; set; } = "";
    public string LogId { get; set; } = "";
}

// A complex object for JSON roundtrip testing
public class OrderWithLines
{
    [PartitionKey]
    public string TenantId { get; set; } = "";

    [RowKey]
    public string OrderId { get; set; } = "";

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
