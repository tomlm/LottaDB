using System.Text.Json;
using Lotta;

namespace Lotta.Tests;

public class DynamicSchemaTests : IClassFixture<LottaDBFixture>
{
    private static readonly JsonElement PersonSchema = JsonDocument.Parse("""
    {
        "properties": {
            "Name": { "type": "string" },
            "Age": { "type": "integer" }
        },
        "key": "Id",
        "keyMode": "Auto"
    }
    """).RootElement;

    private static async Task<LottaDB> CreateDbWithSchema()
    {
        return await LottaDBFixture.CreateDbAsync(config =>
        {
            config.StoreSchema("Person", PersonSchema);
        });
    }

    // === Schema Parsing ===

    [Fact]
    public void Parse_ExtractsProperties()
    {
        var schema = DynamicSchema.Parse("Person", PersonSchema);
        Assert.Equal("Person", schema.TypeName);
        Assert.Equal("Id", schema.KeyProperty);
        Assert.Equal(KeyMode.Auto, schema.KeyMode);
        Assert.Equal(2, schema.Properties.Count);
        Assert.Contains(schema.Properties, p => p.Name == "Name" && p.ClrType == typeof(string));
        Assert.Contains(schema.Properties, p => p.Name == "Age" && p.ClrType == typeof(int));
    }

    [Fact]
    public void Parse_Defaults_WhenNoKeySpecified()
    {
        var schemaJson = JsonDocument.Parse("""{ "properties": { "X": { "type": "string" } } }""").RootElement;
        var schema = DynamicSchema.Parse("Test", schemaJson);
        Assert.Equal("Id", schema.KeyProperty);
        Assert.Equal(KeyMode.Auto, schema.KeyMode);
    }

    [Fact]
    public void Parse_TypeMapping()
    {
        var schemaJson = JsonDocument.Parse("""
        {
            "properties": {
                "S": { "type": "string" },
                "I": { "type": "integer" },
                "D": { "type": "number" },
                "B": { "type": "boolean" }
            }
        }
        """).RootElement;
        var schema = DynamicSchema.Parse("Types", schemaJson);
        Assert.Contains(schema.Properties, p => p.Name == "S" && p.ClrType == typeof(string) && p.IsAnalyzed);
        Assert.Contains(schema.Properties, p => p.Name == "I" && p.ClrType == typeof(int) && !p.IsAnalyzed);
        Assert.Contains(schema.Properties, p => p.Name == "D" && p.ClrType == typeof(double) && !p.IsAnalyzed);
        Assert.Contains(schema.Properties, p => p.Name == "B" && p.ClrType == typeof(bool) && !p.IsAnalyzed);
    }

    [Fact]
    public void ExtractKey_AutoMode_GeneratesUlid()
    {
        var schema = DynamicSchema.Parse("Test", PersonSchema);
        var json = JsonDocument.Parse("""{ "Name": "Tom" }""").RootElement;
        var key = schema.ExtractKey(json);
        Assert.NotEmpty(key);
        Assert.Equal(26, key.Length); // ULID length
    }

    [Fact]
    public void ExtractKey_UsesExistingKey()
    {
        var schema = DynamicSchema.Parse("Test", PersonSchema);
        var json = JsonDocument.Parse("""{ "Id": "my-key", "Name": "Tom" }""").RootElement;
        var key = schema.ExtractKey(json);
        Assert.Equal("my-key", key);
    }

    // === Save & Get Round-Trip ===

    [Fact]
    public async Task SaveAndGet_RoundTripsFullJson()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""
        {
            "Name": "Alice",
            "Age": 30,
            "Email": "alice@example.com",
            "Tags": ["dev", "lead"]
        }
        """);

        var result = await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);
        Assert.NotEmpty(result.Changes);
        var key = result.Changes.First().Key;

        var loaded = await db.GetAsync("Person", key, TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.RootElement.GetProperty("Name").GetString());
        Assert.Equal(30, loaded.RootElement.GetProperty("Age").GetInt32());
        // Non-queryable properties survive round-trip
        Assert.Equal("alice@example.com", loaded.RootElement.GetProperty("Email").GetString());
        Assert.Equal(2, loaded.RootElement.GetProperty("Tags").GetArrayLength());
    }

    [Fact]
    public async Task SaveAsync_AutoKey_AvailableViaGetKey()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Name": "Bob", "Age": 25 }""");

        var result = await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);
        var key = result.Changes.First().Key;

        // Key is available via metadata on the saved document
        Assert.Equal(key, json.GetKey());

        // Key is also available on the loaded document
        var loaded = await db.GetAsync("Person", key, TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal(key, loaded.GetKey());
    }

    [Fact]
    public async Task SaveAsync_ExplicitKey_Uses()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "explicit-123", "Name": "Charlie", "Age": 40 }""");

        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync("Person", "explicit-123", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Charlie", loaded.RootElement.GetProperty("Name").GetString());
    }

    // === Delete ===

    [Fact]
    public async Task DeleteAsync_RemovesDocument()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Name": "ToDelete", "Age": 99 }""");

        var result = await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);
        var key = result.Changes.First().Key;

        await db.DeleteAsync("Person", key, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync("Person", key, TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    // === Search ===

    [Fact]
    public async Task Search_FindsByQueryableField()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Alice Smith", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Bob Jones", "Age": 25 }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();

        var results = db.Search("Person", "Name:Alice").ToList();
        Assert.Single(results);
        Assert.Equal("Alice Smith", results[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Search_NumericRangeQuery()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Young", "Age": 20 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Old", "Age": 50 }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();

        var results = db.Search("Person", "Age:[25 TO 60]").ToList();
        Assert.Single(results);
        Assert.Equal("Old", results[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Search_FreeTextOnContent()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Alice Wonder", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Bob Builder", "Age": 25 }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();

        // Free-text search hits _content_ (which includes analyzed string properties)
        var results = db.Search("Person", "wonder").ToList();
        Assert.Single(results);
        Assert.Equal("Alice Wonder", results[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Search_NoQuery_ReturnsAll()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "A", "Age": 1 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "B", "Age": 2 }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();

        var results = db.Search("Person").ToList();
        Assert.Equal(2, results.Count);
    }

    // === GetManyAsync ===

    [Fact]
    public async Task GetManyAsync_ReturnsAll()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "X", "Age": 1 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Y", "Age": 2 }"""),
            TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync("Person",
            cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task GetManyAsync_WithODataFilter()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Young", "Age": 20 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Old", "Age": 50 }"""),
            TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync("Person",
            filter: "Age gt 30",
            cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Old", results[0].RootElement.GetProperty("Name").GetString());
    }

    // === RebuildSearchIndex ===

    [Fact]
    public async Task RebuildSearchIndex_ReindexesDynamicDocuments()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Alice Rebuild", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Bob Rebuild", "Age": 25 }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();
        Assert.Equal(2, db.Search("Person").Count());

        // Wipe the Lucene index to prove RebuildSearchIndex actually repopulates it
        db.DeleteSearchIndex();
        Assert.Empty(db.Search("Person"));

        // Rebuild from Table Storage
        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        // Dynamic documents should be searchable again
        var results = db.Search("Person").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.RootElement.GetProperty("Name").GetString() == "Alice Rebuild");
        Assert.Contains(results, r => r.RootElement.GetProperty("Name").GetString() == "Bob Rebuild");
    }

    [Fact]
    public async Task RebuildSearchIndex_DynamicAndTypedCoexist()
    {
        using var db = await CreateDbWithSchema();

        // Save a dynamic document
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Dynamic Doc", "Age": 40 }"""),
            TestContext.Current.CancellationToken);

        // Save a typed document
        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "typed-user", DisplayName = "Typed Doc" },
            TestContext.Current.CancellationToken);

        // Wipe the Lucene index to prove RebuildSearchIndex actually repopulates it
        db.DeleteSearchIndex();
        Assert.Empty(db.Search("Person"));
        Assert.Empty(db.Search<Actor>().ToList());

        // Rebuild from Table Storage
        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        // Both should be restored
        var dynamicResults = db.Search("Person").ToList();
        Assert.Single(dynamicResults);
        Assert.Equal("Dynamic Doc", dynamicResults[0].RootElement.GetProperty("Name").GetString());

        var typedResults = db.Search<Actor>().ToList();
        Assert.Single(typedResults);
        Assert.Equal("Typed Doc", typedResults[0].DisplayName);
    }

    // === UpdateSchemaAsync ===

    [Fact]
    public async Task UpdateSchemaAsync_NewFieldBecomesSearchable()
    {
        using var db = await CreateDbWithSchema();

        // Save a doc with an Email field (not queryable yet)
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Alice", "Age": 30, "Email": "alice-at-test" }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();

        // Email is not in the schema — searching by it throws a parse error
        Assert.Throws<Lucene.Net.QueryParsers.Classic.ParseException>(
            () => db.Search("Person", "Email:alice-at-test").ToList());

        // Update schema to add Email as queryable
        var newSchema = JsonDocument.Parse("""
        {
            "properties": {
                "Name": { "type": "string" },
                "Age": { "type": "integer" },
                "Email": { "type": "string" }
            },
            "key": "Id",
            "keyMode": "Auto"
        }
        """).RootElement;

        await db.UpdateSchemaAsync("Person", newSchema, TestContext.Current.CancellationToken);

        // Now Email should be searchable
        var after = db.Search("Person", "Email:alice-at-test").ToList();
        Assert.Single(after);
        Assert.Equal("Alice", after[0].RootElement.GetProperty("Name").GetString());
    }

    // === Dynamic ETag Support ===

    [Fact]
    public async Task GetAsync_Dynamic_ReturnsETag()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "etag-dyn", "Name": "Test", "Age": 25 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        var result = await db.GetAsync("Person", "etag-dyn", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.Equal("Test", result.RootElement.GetProperty("Name").GetString());
        Assert.NotEmpty(result.GetETag()!);
    }

    [Fact]
    public async Task SaveAsync_Dynamic_WithETag_ConditionalWrite_Succeeds()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "etag-dyn-save", "Name": "V1", "Age": 20 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        // GetAsync returns doc with ETag → next SaveAsync is conditional
        var result = await db.GetAsync("Person", "etag-dyn-save", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.NotEmpty(result.GetETag()!);

        // Save the loaded doc (has ETag) → conditional write
        await db.SaveAsync("Person", result, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync("Person", "etag-dyn-save", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task SaveAsync_Dynamic_WithStaleETag_ThrowsConcurrencyException()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "etag-dyn-conflict", "Name": "V1", "Age": 20 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        var result = await db.GetAsync("Person", "etag-dyn-conflict", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        var staleETag = result.GetETag()!;

        // Another writer modifies (updates result's ETag)
        await db.SaveAsync("Person", result, TestContext.Current.CancellationToken);

        // Set stale ETag back on a new doc
        var v3 = JsonDocument.Parse("""{ "Id": "etag-dyn-conflict", "Name": "V3", "Age": 20 }""");
        v3.SetETag(staleETag);
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => db.SaveAsync("Person", v3, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Search_ReturnsETags()
    {
        using var db = await CreateDbWithSchema();
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "ETagSearch", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search("Person", "Name:ETagSearch").ToList();
        Assert.Single(results);
        Assert.Equal("ETagSearch", results[0].RootElement.GetProperty("Name").GetString());
        Assert.NotEmpty(results[0].GetETag()!);
    }

    // === SaveManyAsync ===

    [Fact]
    public async Task SaveManyAsync_Dynamic_BatchSavesAndIndexes()
    {
        using var db = await CreateDbWithSchema();

        var docs = Enumerable.Range(1, 5).Select(i =>
            JsonDocument.Parse($$$"""{ "Name": "Batch{{{i}}}", "Age": {{{i * 10}}} }""")).ToList();

        var result = await db.SaveManyAsync("Person", docs, TestContext.Current.CancellationToken);
        Assert.Equal(5, result.Changes.Count);

        // Verify all are in Table Storage
        var all = await db.GetManyAsync("Person",
            cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Equal(5, all.Count);

        // Verify all are searchable
        db.ReloadSearcher();
        var searchResults = db.Search("Person").ToList();
        Assert.Equal(5, searchResults.Count);
    }

    // === OnSchema Handlers ===

    [Fact]
    public async Task OnSchema_Handler_FiresOnSave()
    {
        var handlerCalled = false;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.StoreSchema("Person", PersonSchema);
            config.OnSchema("Person", (doc, kind, db) =>
            {
                handlerCalled = true;
                Assert.Equal(TriggerKind.Saved, kind);
                Assert.Equal("HandlerTest", doc.RootElement.GetProperty("Name").GetString());
                return Task.CompletedTask;
            });
        });

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "HandlerTest", "Age": 30 }"""),
            TestContext.Current.CancellationToken);

        Assert.True(handlerCalled);
    }

    [Fact]
    public async Task OnSchema_Handler_FiresOnDelete()
    {
        var handlerKind = (TriggerKind?)null;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.StoreSchema("Person", PersonSchema);
            config.OnSchema("Person", (doc, kind, db) =>
            {
                handlerKind = kind;
                return Task.CompletedTask;
            });
        });

        var result = await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Id": "del-handler", "Name": "ToDelete", "Age": 99 }"""),
            TestContext.Current.CancellationToken);

        handlerKind = null; // reset after save
        await db.DeleteAsync("Person", "del-handler", TestContext.Current.CancellationToken);

        Assert.Equal(TriggerKind.Deleted, handlerKind);
    }

    [Fact]
    public async Task OnSchema_Handler_FiresOnSaveManyAsync()
    {
        var savedNames = new List<string>();
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.StoreSchema("Person", PersonSchema);
            config.OnSchema("Person", (doc, kind, db) =>
            {
                if (kind == TriggerKind.Saved)
                    savedNames.Add(doc.RootElement.GetProperty("Name").GetString()!);
                return Task.CompletedTask;
            });
        });

        var docs = new[]
        {
            JsonDocument.Parse("""{ "Name": "Batch1", "Age": 10 }"""),
            JsonDocument.Parse("""{ "Name": "Batch2", "Age": 20 }"""),
            JsonDocument.Parse("""{ "Name": "Batch3", "Age": 30 }"""),
        };

        await db.SaveManyAsync("Person", docs, TestContext.Current.CancellationToken);

        Assert.Equal(3, savedNames.Count);
        Assert.Contains("Batch1", savedNames);
        Assert.Contains("Batch2", savedNames);
        Assert.Contains("Batch3", savedNames);
    }

    // === Dynamic ETag on SaveAsync ===

    [Fact]
    public async Task SaveAsync_Dynamic_SetsETagOnDocument()
    {
        using var db = await CreateDbWithSchema();
        // Use explicit key so the document reference isn't replaced by SetKey
        var json = JsonDocument.Parse("""{ "Id": "etag-on-save", "Name": "ETagOnSave", "Age": 25 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        // The saved document should have its ETag annotated
        Assert.NotNull(json.GetETag());
        Assert.NotEmpty(json.GetETag()!);
    }

    [Fact]
    public async Task Search_AnnotatesETags()
    {
        using var db = await CreateDbWithSchema();
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "SearchETag", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search("Person", "Name:SearchETag").ToList();
        Assert.Single(results);
        Assert.NotNull(results[0].GetETag());
        Assert.NotEmpty(results[0].GetETag()!);
    }

    [Fact]
    public async Task GetManyAsync_AnnotatesETags()
    {
        using var db = await CreateDbWithSchema();
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "GetManyETag", "Age": 30 }"""),
            TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync("Person",
            cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Single(results);
        Assert.NotNull(results[0].GetETag());
        Assert.NotEmpty(results[0].GetETag()!);
    }

    // === Error Handling ===

    [Fact]
    public async Task UnregisteredSchema_ThrowsInvalidOperation()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Name": "test" }""");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.SaveAsync("NonExistent", json, TestContext.Current.CancellationToken));
    }
}
