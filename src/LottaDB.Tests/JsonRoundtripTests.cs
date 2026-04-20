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
        using var db = await LottaDBFixture.CreateDbAsync();

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
        await db.SaveAsync(order, TestContext.Current.CancellationToken);

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
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "q-multi-1",
            Total = 100m,
            Lines = new List<OrderLine> { new() { ProductId = "a", Quantity = 1, Price = 100m } },
            Metadata = new Dictionary<string, string> { ["key"] = "val1" }
        }, TestContext.Current.CancellationToken);
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
        }, TestContext.Current.CancellationToken);

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
    public async Task Search_ComplexObject_PreservesAllProperties()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

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
                ["promo"] = "SAVE10"
            }
        };
        await db.SaveAsync(order, TestContext.Current.CancellationToken);

        // Search deserializes from _json stored in Lucene — full POCO fidelity
        var results = db.Search<OrderWithLines>().ToList();
        Assert.Single(results);

        var loaded = results[0];
        Assert.Equal("s-order-1", loaded.OrderId);
        Assert.Equal("tenant-1", loaded.TenantId);
        Assert.Equal(499.00m, loaded.Total);
        Assert.Single(loaded.Lines);
        Assert.Equal("alpha", loaded.Lines[0].ProductId);
        Assert.Equal(5, loaded.Lines[0].Quantity);
        Assert.Equal(99.80m, loaded.Lines[0].Price);
        Assert.Equal(2, loaded.Metadata.Count);
        Assert.Equal("mobile", loaded.Metadata["channel"]);
        Assert.Equal("SAVE10", loaded.Metadata["promo"]);
    }

    [Fact]
    public async Task Query_And_Search_DeserializeIdentically()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        var order = new OrderWithLines
        {
            OrderId = "sync-1",
            TenantId = "sync-tenant",
            Total = 199.99m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "a", Quantity = 5, Price = 39.998m }
            },
            Metadata = new Dictionary<string, string> { ["key"] = "val" }
        };
        await db.SaveAsync(order, TestContext.Current.CancellationToken);

        var fromQuery = db.Query<OrderWithLines>().First();
        var fromSearch = db.Search<OrderWithLines>().First();

        // Deep equality: both paths should produce identical objects
        Assert.Equal(
            System.Text.Json.JsonSerializer.Serialize(fromQuery),
            System.Text.Json.JsonSerializer.Serialize(fromSearch));
    }

    [Fact]
    public async Task Search_ComplexObject_PreservesNestedCollections()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "search-nested",
            Total = 350m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "x", Quantity = 2, Price = 100m },
                new() { ProductId = "y", Quantity = 3, Price = 50m }
            },
            Metadata = new Dictionary<string, string>
            {
                ["a"] = "1",
                ["b"] = "2"
            }
        }, TestContext.Current.CancellationToken);

        var result = db.Search<OrderWithLines>().First();
        Assert.Equal(350m, result.Total);
        Assert.Equal(2, result.Lines.Count);
        Assert.Equal("x", result.Lines[0].ProductId);
        Assert.Equal(2, result.Metadata.Count);
    }

    [Fact]
    public async Task Search_Note_PreservesIndexedFields()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Note
        {
            NoteId = "search-rt-1",
            AuthorId = "alice",
            Content = "Full text content here",
            Published = DateTimeOffset.UtcNow,
            Domain = "test.com"
        }, TestContext.Current.CancellationToken);

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
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "empty-1",
            Total = 0m,
            Lines = new List<OrderLine>(),
            Metadata = new Dictionary<string, string>()
        }, TestContext.Current.CancellationToken);

        var results = db.Query<OrderWithLines>().ToList();
        Assert.Single(results);
        Assert.Empty(results[0].Lines);
        Assert.Empty(results[0].Metadata);
    }

    [Fact]
    public async Task Query_NoteWithTags_PreservesListProperty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Note
        {
            NoteId = "tagged-1",
            AuthorId = "alice",
            Content = "Tagged note",
            Published = DateTimeOffset.UtcNow,
            Tags = new List<string> { "csharp", "dotnet", "lucene" }
        }, TestContext.Current.CancellationToken);

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
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new Note
        {
            NoteId = "tagged-2",
            AuthorId = "bob",
            Content = "Another tagged note",
            Published = DateTimeOffset.UtcNow,
            Tags = new List<string> { "azure", "tables" }
        }, TestContext.Current.CancellationToken);

        var results = db.Search<Note>().ToList();
        Assert.Single(results);
        Assert.Equal(2, results[0].Tags.Count);
        Assert.Contains("azure", results[0].Tags);
        Assert.Contains("tables", results[0].Tags);
    }

    [Fact]
    public async Task RebuildIndex_PreservesComplexObjectFidelity()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new OrderWithLines
        {
            OrderId = "rebuild-complex",
            Total = 599.99m,
            Lines = new List<OrderLine>
            {
                new() { ProductId = "x", Quantity = 3, Price = 199.997m }
            },
            Metadata = new Dictionary<string, string> { ["k"] = "v" }
        }, TestContext.Current.CancellationToken);

        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        var result = db.Search<OrderWithLines>().First();
        Assert.Equal("rebuild-complex", result.OrderId);
        Assert.Equal(599.99m, result.Total);
        Assert.Single(result.Lines);
        Assert.Equal("x", result.Lines[0].ProductId);
    }

    [Fact]
    public async Task Builder_DerivedObject_SearchPreservesJsonFidelity()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                var actor = await db.GetAsync<Actor>(note.AuthorId);
                await db.SaveAsync(new NoteView
                {
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    AuthorDisplay = actor?.DisplayName ?? "",
                    AuthorUsername = actor?.Username ?? "",
                    Content = note.Content,
                    Published = note.Published,
                    Tags = note.Tags.ToArray(),
                });
            });
        });

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note
        {
            NoteId = "builder-json",
            AuthorId = "alice",
            Content = "Test",
            Published = DateTimeOffset.UtcNow,
            Tags = new List<string> { "a", "b" }
        }, TestContext.Current.CancellationToken);

        // NoteView produced by builder should have Tags preserved through Search
        var view = db.Search<NoteView>()
            .FirstOrDefault(v => v.Id == "nv-builder-json");

        Assert.NotNull(view);
        Assert.Equal("Alice", view.AuthorDisplay);
        Assert.Equal(2, view.Tags.Count);
        Assert.Contains("a", view.Tags);
    }
}
