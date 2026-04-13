namespace Lotta.Tests;

/// <summary>
/// Verifies that complex objects with nested collections, dictionaries,
/// and non-primitive properties round-trip correctly through Query (table storage)
/// and Search (Lucene), both deserializing from _json.
/// </summary>
public class JsonRoundtripTests
{
    [Fact]
    public async Task Query_ComplexObject_PreservesNestedCollections()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        var order = new OrderWithLines
        {
            OrderId = "q-order-1",
            TenantId = "tenant-1",
            Total = 299.99m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "widget", Quantity = 3, Price = 49.99m },
                new() { ProductId = "gadget", Quantity = 1, Price = 150.02m },
            },
            Metadata = new Dictionary<string, string>
            {
                ["source"] = "api",
                ["region"] = "us-west"
            }
        };
        await db.SaveAsync(order);

        var results = db.Query<OrderWithLines>().ToList();
        Assert.Single(results);

        var loaded = results[0];
        Assert.Equal("q-order-1", loaded.OrderId);
        Assert.Equal("tenant-1", loaded.TenantId);
        Assert.Equal(299.99m, loaded.Total);
        Assert.Equal(2, loaded.Lines.Count);
        Assert.Equal("widget", loaded.Lines[0].ProductId);
        Assert.Equal(3, loaded.Lines[0].Quantity);
        Assert.Equal(49.99m, loaded.Lines[0].Price);
        Assert.Equal("gadget", loaded.Lines[1].ProductId);
        Assert.Equal(2, loaded.Metadata.Count);
        Assert.Equal("api", loaded.Metadata["source"]);
        Assert.Equal("us-west", loaded.Metadata["region"]);
    }

    [Fact]
    public async Task Query_MultipleComplexObjects_AllPreserved()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "q-multi-1",
            Total = 100m,
            Lines = new List<OrderLine> { new() { ProductId = "a", Quantity = 1, Price = 100m } },
            Metadata = new Dictionary<string, string> { ["key"] = "val1" }
        });
        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "q-multi-2",
            Total = 200m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "b", Quantity = 2, Price = 50m },
                new() { ProductId = "c", Quantity = 2, Price = 50m }
            },
            Metadata = new Dictionary<string, string> { ["key"] = "val2" }
        });

        var results = db.Query<OrderWithLines>().ToList();
        Assert.Equal(2, results.Count);

        var order1 = results.First(o => o.OrderId == "q-multi-1");
        Assert.Single(order1.Lines);
        Assert.Equal("val1", order1.Metadata["key"]);

        var order2 = results.First(o => o.OrderId == "q-multi-2");
        Assert.Equal(2, order2.Lines.Count);
        Assert.Equal("val2", order2.Metadata["key"]);
    }

    [Fact]
    public async Task Search_ReturnsOnlyIndexedFields()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        var order = new OrderWithLines
        {
            OrderId = "s-order-1",
            TenantId = "tenant-1",
            Total = 499.00m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "alpha", Quantity = 5, Price = 99.80m },
            },
            Metadata = new Dictionary<string, string>
            {
                ["channel"] = "mobile",
            }
        };
        await db.SaveAsync(order);

        // Search returns objects from Lucene — only [Field]-annotated properties are populated.
        // Complex types (List<OrderLine>, Dictionary) and non-indexable types (decimal) are lost.
        // Use Query<T>() for full POCO fidelity.
        var results = db.Search<OrderWithLines>().ToList();
        Assert.Single(results);
        Assert.Equal("s-order-1", results[0].OrderId); // [Key][Field(Key=true)] — preserved
        Assert.Equal("tenant-1", results[0].TenantId);  // string property — preserved by convention
    }

    [Fact]
    public async Task Search_Note_PreservesIndexedFields()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Note
        {
            NoteId = "search-rt-1",
            AuthorId = "alice",
            Content = "Full text content here",
            Published = DateTimeOffset.UtcNow,
            Domain = "test.com"
        });

        // Search returns Note with [Field]-annotated properties
        var results = db.Search<Note>().ToList();
        Assert.Single(results);
        Assert.Equal("search-rt-1", results[0].NoteId);
        Assert.Equal("alice", results[0].AuthorId);
        Assert.Equal("test.com", results[0].Domain);
    }

    [Fact]
    public async Task Query_EmptyCollections_PreservedAsEmpty()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "empty-1",
            Total = 0m,
            Lines = new List<OrderLine>(),
            Metadata = new Dictionary<string, string>()
        });

        var results = db.Query<OrderWithLines>().ToList();
        Assert.Single(results);
        Assert.Empty(results[0].Lines);
        Assert.Empty(results[0].Metadata);
    }

    [Fact]
    public async Task Query_NoteWithTags_PreservesListProperty()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Note
        {
            NoteId = "tagged-1",
            AuthorId = "alice",
            Content = "Tagged note",
            Published = DateTimeOffset.UtcNow,
            Tags = new List<string> { "csharp", "dotnet", "lucene" }
        });

        var results = db.Query<Note>().ToList();
        Assert.Single(results);
        Assert.Equal(3, results[0].Tags.Count);
        Assert.Contains("csharp", results[0].Tags);
        Assert.Contains("dotnet", results[0].Tags);
        Assert.Contains("lucene", results[0].Tags);
    }

    [Fact]
    public async Task Search_NoteWithTags_PreservesListProperty()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        await db.SaveAsync(new Note
        {
            NoteId = "tagged-2",
            AuthorId = "bob",
            Content = "Another tagged note",
            Published = DateTimeOffset.UtcNow,
            Tags = new List<string> { "azure", "tables" }
        });

        var results = db.Search<Note>().ToList();
        Assert.Single(results);
        Assert.Equal(2, results[0].Tags.Count);
        Assert.Contains("azure", results[0].Tags);
        Assert.Contains("tables", results[0].Tags);
    }
}
