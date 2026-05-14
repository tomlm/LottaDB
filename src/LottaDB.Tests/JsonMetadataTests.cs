using System.Text.Json;
using Lotta;

namespace Lotta.Tests;

public class JsonMetadataTests : IClassFixture<LottaDBFixture>
{
    private static readonly List<QueryableProperty> PersonProperties = new()
    {
        new() { Name = "Name", Type = "string" },
        new() { Name = "Age", Type = "integer" },
    };

    private static async Task<LottaDB> CreateDbWithSchema()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new JsonDocumentType
        {
            Name = "Person",
            Properties = PersonProperties
        }, TestContext.Current.CancellationToken);
        return db;
    }

    // === Schema as Entity ===

    [Fact]
    public async Task SaveJsonDocumentType_EnablesDynamicCRUD()
    {
        using var db = await CreateDbWithSchema();

        // Dynamic CRUD should work immediately after saving the schema
        var doc = JsonDocument.Parse("""{ "Name": "Test", "Age": 25 }""");
        await db.SaveAsync("Person", doc, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync("Person", doc.GetKey()!, TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task UpdateJsonDocumentType_NewFieldBecomesSearchable()
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

        // Update schema to add Email as queryable (On<JsonDocumentType> triggers reindex)
        await db.SaveAsync(new JsonDocumentType
        {
            Name = "Person",
            Properties = new()
            {
                new() { Name = "Name", Type = "string" },
                new() { Name = "Age", Type = "integer" },
                new() { Name = "Email", Type = "string" },
            }
        }, TestContext.Current.CancellationToken);

        // Now Email should be searchable
        var after = db.Search("Person", "Email:alice-at-test").ToList();
        Assert.Single(after);
        Assert.Equal("Alice", after[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task DeleteJsonDocumentType_RemovesMapper()
    {
        using var db = await CreateDbWithSchema();

        // Save a document
        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Test", "Age": 25 }"""),
            TestContext.Current.CancellationToken);

        // Delete the schema
        await db.DeleteAsync<JsonDocumentType>("Person", TestContext.Current.CancellationToken);

        // Dynamic CRUD should now throw
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "X" }"""), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task GetManyAsync_JsonDocumentType_ListsAllSchemas()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new JsonDocumentType { Name = "Person", Properties = PersonProperties }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new JsonDocumentType
        {
            Name = "Photo",
            Properties = new() { new() { Name = "Width", Type = "integer" }, new() { Name = "Height", Type = "integer" } }
        }, TestContext.Current.CancellationToken);

        var schemas = await db.GetManyAsync<JsonDocumentType>(cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Equal(2, schemas.Count);
    }

    // === Schema Parsing ===

    [Fact]
    public void Parse_FromJsonDocumentType_ExtractsProperties()
    {
        var schema = JsonMetadata.Parse(new JsonDocumentType
        {
            Name = "Person",
            Properties = PersonProperties,
            Key = "Id",
            KeyMode = KeyMode.Auto
        });
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
        var schema = JsonMetadata.Parse("Test", schemaJson);
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
        var schema = JsonMetadata.Parse("Types", schemaJson);
        Assert.Contains(schema.Properties, p => p.Name == "S" && p.ClrType == typeof(string) && p.IsAnalyzed);
        Assert.Contains(schema.Properties, p => p.Name == "I" && p.ClrType == typeof(int) && !p.IsAnalyzed);
        Assert.Contains(schema.Properties, p => p.Name == "D" && p.ClrType == typeof(double) && !p.IsAnalyzed);
        Assert.Contains(schema.Properties, p => p.Name == "B" && p.ClrType == typeof(bool) && !p.IsAnalyzed);
    }

    [Fact]
    public void GetKey_AutoMode_GeneratesUlid()
    {
        var schema = JsonMetadata.Parse("Test", JsonDocument.Parse("""
        { "properties": { "Name": { "type": "string" } }, "key": "Id", "keyMode": "Auto" }
        """).RootElement);
        var json = JsonDocument.Parse("""{ "Name": "Tom" }""").RootElement;
        var key = schema.GetKey(json);
        Assert.NotEmpty(key);
        Assert.Equal(26, key.Length); // ULID length
    }

    [Fact]
    public void GetKey_UsesExistingKey()
    {
        var schema = JsonMetadata.Parse("Test", JsonDocument.Parse("""
        { "properties": { "Name": { "type": "string" } }, "key": "Id", "keyMode": "Auto" }
        """).RootElement);
        var json = JsonDocument.Parse("""{ "Id": "my-key", "Name": "Tom" }""").RootElement;
        var key = schema.GetKey(json);
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

        Assert.Equal(key, json.GetKey());

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

        db.DeleteSearchIndex();
        Assert.Empty(db.Search("Person"));

        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        var results = db.Search("Person").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.RootElement.GetProperty("Name").GetString() == "Alice Rebuild");
        Assert.Contains(results, r => r.RootElement.GetProperty("Name").GetString() == "Bob Rebuild");
    }

    [Fact]
    public async Task RebuildSearchIndex_DynamicAndTypedCoexist()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Dynamic Doc", "Age": 40 }"""),
            TestContext.Current.CancellationToken);

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "typed-user", DisplayName = "Typed Doc" },
            TestContext.Current.CancellationToken);

        db.DeleteSearchIndex();
        Assert.Empty(db.Search("Person"));
        Assert.Empty(db.Search<Actor>().ToList());

        await db.RebuildSearchIndex(TestContext.Current.CancellationToken);

        var dynamicResults = db.Search("Person").ToList();
        Assert.Single(dynamicResults);
        Assert.Equal("Dynamic Doc", dynamicResults[0].RootElement.GetProperty("Name").GetString());

        var typedResults = db.Search<Actor>().ToList();
        Assert.Single(typedResults);
        Assert.Equal("Typed Doc", typedResults[0].DisplayName);
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

        var result = await db.GetAsync("Person", "etag-dyn-save", TestContext.Current.CancellationToken);
        Assert.NotNull(result);
        Assert.NotEmpty(result.GetETag()!);

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

        await db.SaveAsync("Person", result, TestContext.Current.CancellationToken);

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

        var all = await db.GetManyAsync("Person",
            cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Equal(5, all.Count);

        db.ReloadSearcher();
        var searchResults = db.Search("Person").ToList();
        Assert.Equal(5, searchResults.Count);
    }

    // === Dynamic ETag on SaveAsync ===

    [Fact]
    public async Task SaveAsync_Dynamic_SetsETagOnDocument()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "etag-on-save", "Name": "ETagOnSave", "Age": 25 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

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
        using var db = await LottaDBFixture.CreateDbAsync();
        var json = JsonDocument.Parse("""{ "Name": "test" }""");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => db.SaveAsync("NonExistent", json, TestContext.Current.CancellationToken));
    }

    // === Overwrite / Upsert ===

    [Fact]
    public async Task SaveAsync_ExistingDocument_Overwrites()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "overwrite", "Name": "V1", "Age": 10 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        var v2 = JsonDocument.Parse("""{ "Id": "overwrite", "Name": "V2", "Age": 20 }""");
        await db.SaveAsync("Person", v2, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync("Person", "overwrite", TestContext.Current.CancellationToken);
        Assert.Equal("V2", loaded!.RootElement.GetProperty("Name").GetString());
        Assert.Equal(20, loaded.RootElement.GetProperty("Age").GetInt32());
    }

    [Fact]
    public async Task SaveAsync_ReturnsObjectResult()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Name": "Result", "Age": 25 }""");

        var result = await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        Assert.NotEmpty(result.Changes);
        Assert.Equal(ChangeKind.Saved, result.Changes.First().Kind);
        Assert.NotEmpty(result.Changes.First().Key);
    }

    // === Get Edge Cases ===

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        using var db = await CreateDbWithSchema();
        var loaded = await db.GetAsync("Person", "nonexistent-key", TestContext.Current.CancellationToken);
        Assert.Null(loaded);
    }

    // === Delete Edge Cases ===

    [Fact]
    public async Task DeleteAsync_ReturnsObjectResult()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "del-result", "Name": "ToDelete", "Age": 99 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);

        var result = await db.DeleteAsync("Person", "del-result", TestContext.Current.CancellationToken);

        Assert.NotEmpty(result.Changes);
        Assert.Equal(ChangeKind.Deleted, result.Changes.First().Kind);
    }

    [Fact]
    public async Task DeleteAsync_NonExistent_NoError()
    {
        using var db = await CreateDbWithSchema();
        var result = await db.DeleteAsync("Person", "does-not-exist", TestContext.Current.CancellationToken);
        // Should not throw
        Assert.NotNull(result);
    }

    // === Search Visibility ===

    [Fact]
    public async Task Search_EmptyIndex_ReturnsEmpty()
    {
        using var db = await CreateDbWithSchema();
        db.ReloadSearcher();
        var results = db.Search("Person").ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Search_ReflectsSave()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Name": "Visible", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var results = db.Search("Person", "Name:Visible").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task Search_ReflectsUpdate()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Id": "update-vis", "Name": "Before", "Age": 10 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();
        Assert.Single(db.Search("Person", "Name:Before").ToList());

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Id": "update-vis", "Name": "After", "Age": 20 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        Assert.Empty(db.Search("Person", "Name:Before").ToList());
        Assert.Single(db.Search("Person", "Name:After").ToList());
    }

    [Fact]
    public async Task Search_ReflectsDelete()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Id": "del-vis", "Name": "Gone", "Age": 10 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();
        Assert.Single(db.Search("Person", "Name:Gone").ToList());

        await db.DeleteAsync("Person", "del-vis", TestContext.Current.CancellationToken);
        db.ReloadSearcher();
        Assert.Empty(db.Search("Person", "Name:Gone").ToList());
    }

    // === ETag Behavior ===

    [Fact]
    public async Task ETag_UpdatedInPlaceAfterSave()
    {
        using var db = await CreateDbWithSchema();
        var json = JsonDocument.Parse("""{ "Id": "etag-inplace", "Name": "V1", "Age": 10 }""");
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);
        var etagAfterFirst = json.GetETag()!;

        // Save again (conditional write since ETag is present)
        await db.SaveAsync("Person", json, TestContext.Current.CancellationToken);
        var etagAfterSecond = json.GetETag()!;

        Assert.NotEqual(etagAfterFirst, etagAfterSecond);
    }

    [Fact]
    public async Task Search_ThenConditionalSave_Works()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person",
            JsonDocument.Parse("""{ "Id": "search-save", "Name": "Original", "Age": 30 }"""),
            TestContext.Current.CancellationToken);
        db.ReloadSearcher();

        var found = db.Search("Person", "Name:Original").First();
        Assert.NotEmpty(found.GetETag()!);

        // ETag on the search result → conditional save
        await db.SaveAsync("Person", found, TestContext.Current.CancellationToken);

        var loaded = await db.GetAsync("Person", "search-save", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
    }

    // === SaveManyAsync Edge Cases ===

    [Fact]
    public async Task SaveManyAsync_DuplicateKey_AutoFlushes()
    {
        using var db = await CreateDbWithSchema();

        var docs = new[]
        {
            JsonDocument.Parse("""{ "Id": "dup", "Name": "First", "Age": 10 }"""),
            JsonDocument.Parse("""{ "Id": "dup", "Name": "Second", "Age": 20 }"""),
        };

        var result = await db.SaveManyAsync("Person", docs, TestContext.Current.CancellationToken);
        Assert.Equal(2, result.Changes.Count);

        // Last write wins
        var loaded = await db.GetAsync("Person", "dup", TestContext.Current.CancellationToken);
        Assert.Equal("Second", loaded!.RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task SaveManyAsync_Empty_ReturnsEmptyResult()
    {
        using var db = await CreateDbWithSchema();

        var result = await db.SaveManyAsync("Person", Array.Empty<JsonDocument>(), TestContext.Current.CancellationToken);
        Assert.Empty(result.Changes);
    }

    [Fact]
    public async Task SaveManyAsync_Over100_AutoFlushes()
    {
        using var db = await CreateDbWithSchema();

        var docs = Enumerable.Range(1, 150).Select(i =>
            JsonDocument.Parse($$$"""{ "Name": "Item{{{i}}}", "Age": {{{i}}} }""")).ToList();

        var result = await db.SaveManyAsync("Person", docs, TestContext.Current.CancellationToken);
        Assert.Equal(150, result.Changes.Count);

        var all = await db.GetManyAsync("Person",
            cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Equal(150, all.Count);
    }

    // === DeleteManyAsync ===

    [Fact]
    public async Task DeleteManyAsync_NoPredicate_DeletesAll()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "A", "Age": 1 }"""), TestContext.Current.CancellationToken);
        await db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "B", "Age": 2 }"""), TestContext.Current.CancellationToken);
        await db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "C", "Age": 3 }"""), TestContext.Current.CancellationToken);

        var result = await db.DeleteManyAsync("Person", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, result.Changes.Count);
        Assert.All(result.Changes, c => Assert.Equal(ChangeKind.Deleted, c.Kind));

        var remaining = await db.GetManyAsync("Person", cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Empty(remaining);
    }

    [Fact]
    public async Task DeleteManyAsync_WithFilter_DeletesMatching()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "Young", "Age": 20 }"""), TestContext.Current.CancellationToken);
        await db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "Old", "Age": 50 }"""), TestContext.Current.CancellationToken);

        await db.DeleteManyAsync("Person", filter: "Age gt 30", cancellationToken: TestContext.Current.CancellationToken);

        var remaining = await db.GetManyAsync("Person", cancellationToken: TestContext.Current.CancellationToken).ToListAsync();
        Assert.Single(remaining);
        Assert.Equal("Young", remaining[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task DeleteManyAsync_RemovesFromSearch()
    {
        using var db = await CreateDbWithSchema();

        await db.SaveAsync("Person", JsonDocument.Parse("""{ "Name": "SearchDel", "Age": 10 }"""), TestContext.Current.CancellationToken);
        db.ReloadSearcher();
        Assert.Single(db.Search("Person", "Name:SearchDel").ToList());

        await db.DeleteManyAsync("Person", cancellationToken: TestContext.Current.CancellationToken);
        db.ReloadSearcher();
        Assert.Empty(db.Search("Person", "Name:SearchDel").ToList());
    }

    [Fact]
    public async Task DeleteManyAsync_Empty_ReturnsEmptyResult()
    {
        using var db = await CreateDbWithSchema();
        var result = await db.DeleteManyAsync("Person", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Empty(result.Changes);
    }

    // === JsonPath Extraction ===

    [Fact]
    public void GetValue_TopLevelProperty()
    {
        var json = JsonDocument.Parse("""{ "Name": "Alice", "Age": 30 }""").RootElement;
        var prop = new IndexedJsonProperty("Name", typeof(string), true);

        var result = JsonMetadata.GetValue(json, prop);
        Assert.NotNull(result);
        Assert.Equal("Alice", result.Value.GetString());
    }

    [Fact]
    public void GetValue_MissingProperty_ReturnsNull()
    {
        var json = JsonDocument.Parse("""{ "Name": "Alice" }""").RootElement;
        var prop = new IndexedJsonProperty("Missing", typeof(string), true);

        Assert.Null(JsonMetadata.GetValue(json, prop));
    }

    [Fact]
    public void GetValue_DotPath_NestedProperty()
    {
        var json = JsonDocument.Parse("""{ "address": { "city": "Seattle", "zip": "98101" } }""").RootElement;
        var prop = new IndexedJsonProperty("City", typeof(string), true, JsonPath: "$.address.city");

        var result = JsonMetadata.GetValue(json, prop);
        Assert.NotNull(result);
        Assert.Equal("Seattle", result.Value.GetString());
    }

    [Fact]
    public void GetValue_DotPath_DeeplyNested()
    {
        var json = JsonDocument.Parse("""{ "user": { "profile": { "name": "Bob" } } }""").RootElement;
        var prop = new IndexedJsonProperty("UserName", typeof(string), true, JsonPath: "$.user.profile.name");

        var result = JsonMetadata.GetValue(json, prop);
        Assert.NotNull(result);
        Assert.Equal("Bob", result.Value.GetString());
    }

    [Fact]
    public void GetValue_DotPath_MissingIntermediate_ReturnsNull()
    {
        var json = JsonDocument.Parse("""{ "user": { "name": "Alice" } }""").RootElement;
        var prop = new IndexedJsonProperty("City", typeof(string), true, JsonPath: "$.user.address.city");

        Assert.Null(JsonMetadata.GetValue(json, prop));
    }

    [Fact]
    public void GetValue_DotPath_WithoutDollarPrefix()
    {
        var json = JsonDocument.Parse("""{ "address": { "city": "Portland" } }""").RootElement;
        var prop = new IndexedJsonProperty("City", typeof(string), true, JsonPath: "address.city");

        var result = JsonMetadata.GetValue(json, prop);
        Assert.NotNull(result);
        Assert.Equal("Portland", result.Value.GetString());
    }

    [Fact]
    public async Task JsonPath_EndToEnd_IndexAndSearch()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveAsync(new JsonDocumentType
        {
            Name = "Contact",
            Properties = new()
            {
                new() { Name = "Name", Type = "string" },
                new() { Name = "City", Type = "string", JsonPath = "$.address.city" },
            }
        }, TestContext.Current.CancellationToken);

        await db.SaveAsync("Contact",
            JsonDocument.Parse("""{ "Name": "Alice", "address": { "city": "Seattle", "zip": "98101" } }"""),
            TestContext.Current.CancellationToken);
        await db.SaveAsync("Contact",
            JsonDocument.Parse("""{ "Name": "Bob", "address": { "city": "Portland", "zip": "97201" } }"""),
            TestContext.Current.CancellationToken);

        db.ReloadSearcher();

        // Search by the nested field extracted via JsonPath
        var results = db.Search("Contact", "City:Seattle").ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].RootElement.GetProperty("Name").GetString());
    }
}
