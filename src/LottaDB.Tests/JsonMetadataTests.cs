using System.Text.Json;
using Lotta;

namespace Lotta.Tests;

public class JsonMetadataTests : LottaTestBase
{
    private static readonly List<QueryableProperty> PersonProperties = new()
    {
        new() { Name = "Name", Type = "string" },
        new() { Name = "Age", Type = "integer" },
    };

    private async Task<LottaDB> CreateDbWithSchema(CancellationToken ct = default)
    {
        var db = await CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new JsonSchema
        {
            Name = "Person",
            Properties = PersonProperties
        }, ct);
        return db;
    }

    // === Schema as Entity ===

    [Fact]
    public async Task SaveJsonSchema_EnablesDynamicCRUD()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        // Dynamic CRUD should work immediately after saving the schema
        var doc = JsonDocument.Parse("""{ "Name": "Test", "Age": 25 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);

        var loaded = (JsonDocument?)await db.GetAsync(doc.GetKey()!, ct);
        Assert.NotNull(loaded);
        Assert.Equal("Test", loaded.RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task UpdateJsonSchema_NewFieldBecomesSearchable()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        // Create schema with AutoQueryable=false so only explicit properties are indexed
        await db.SaveAsync(new JsonSchema
        {
            Name = "Person",
            AutoQueryable = false,
            Properties = new()
            {
                new() { Name = "Name", Type = "string" },
                new() { Name = "Age", Type = "integer" },
            }
        }, ct);

        // Save a doc with an Email field (not indexed since AutoQueryable is off and Email isn't in schema)
        var emailDoc = JsonDocument.Parse("""{ "Name": "Alice", "Age": 30, "Email": "alice-at-test" }""");
        emailDoc.SetSchema("Person");
        await db.SaveAsync(emailDoc, ct);

        db.ReloadSearcher();

        // Email is not in the schema — searching by it finds nothing
        var beforeResults = db.Search(j => j["Email"] == "alice-at-test").ToList();
        Assert.Empty(beforeResults);

        // Update schema to add Email as queryable (On<JsonSchema> triggers reindex)
        await db.SaveAsync(new JsonSchema
        {
            Name = "Person",
            AutoQueryable = false,
            Properties = new()
            {
                new() { Name = "Name", Type = "string" },
                new() { Name = "Age", Type = "integer" },
                new() { Name = "Email", Type = "string" },
            }
        }, ct);

        // Now Email should be searchable
        var after = db.Search(j => j["Email"] == "alice-at-test").ToList();
        Assert.Single(after);
        Assert.Equal("Alice", after[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task DeleteJsonSchema_RemovesMapper()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        // Save a document
        var doc = JsonDocument.Parse("""{ "Name": "Test", "Age": 25 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);

        // Delete the schema
        await db.DeleteAsync<JsonSchema>("Person", ct);

        // Verify the schema is gone — searching by it should return nothing
        db.ReloadSearcher();
        var results = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task GetManyAsync_JsonSchema_ListsAllSchemas()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema { Name = "Person", Properties = PersonProperties }, ct);
        await db.SaveAsync(new JsonSchema
        {
            Name = "Photo",
            Properties = new() { new() { Name = "Width", Type = "integer" }, new() { Name = "Height", Type = "integer" } }
        }, ct);

        var schemas = await db.GetManyAsync<JsonSchema>(cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(2, schemas.Count);
    }

    // === Schema Parsing ===

    [Fact]
    public void Parse_FromJsonSchema_ExtractsProperties()
    {
        var schema = JsonMetadata.Parse(new JsonSchema
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
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""
        {
            "Name": "Alice",
            "Age": 30,
            "Email": "alice@example.com",
            "Tags": ["dev", "lead"]
        }
        """);
        json.SetSchema("Person");

        var result = await db.SaveAsync(json, ct);
        Assert.NotEmpty(result.Changes);
        var key = result.Changes.First().Key;

        var loaded = (JsonDocument?)await db.GetAsync(key, ct);
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.RootElement.GetProperty("Name").GetString());
        Assert.Equal(30, loaded.RootElement.GetProperty("Age").GetInt32());
        Assert.Equal("alice@example.com", loaded.RootElement.GetProperty("Email").GetString());
        Assert.Equal(2, loaded.RootElement.GetProperty("Tags").GetArrayLength());
    }

    [Fact]
    public async Task SaveAsync_AutoKey_AvailableViaGetKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Name": "Bob", "Age": 25 }""");
        json.SetSchema("Person");

        var result = await db.SaveAsync(json, ct);
        var key = result.Changes.First().Key;

        Assert.Equal(key, json.GetKey());

        var loaded = (JsonDocument?)await db.GetAsync(key, ct);
        Assert.NotNull(loaded);
        Assert.Equal(key, loaded.GetKey());
    }

    [Fact]
    public async Task SaveAsync_ExplicitKey_Uses()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "explicit-123", "Name": "Charlie", "Age": 40 }""");
        json.SetSchema("Person");

        await db.SaveAsync(json, ct);

        var loaded = (JsonDocument?)await db.GetAsync("explicit-123", ct);
        Assert.NotNull(loaded);
        Assert.Equal("Charlie", loaded.RootElement.GetProperty("Name").GetString());
    }

    // === Delete ===

    [Fact]
    public async Task DeleteAsync_RemovesDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Name": "ToDelete", "Age": 99 }""");
        json.SetSchema("Person");

        var result = await db.SaveAsync(json, ct);
        var key = result.Changes.First().Key;

        await db.DeleteAsync(key, ct);

        var loaded = (JsonDocument?)await db.GetAsync(key, ct);
        Assert.Null(loaded);
    }

    // === Search ===

    [Fact]
    public async Task Search_FindsByQueryableField()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "Alice Smith", "Age": 30 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Bob Jones", "Age": 25 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        db.ReloadSearcher();

        var results = db.Search(j => j["Name"].Contains("Alice")).ToList();
        Assert.Single(results);
        Assert.Equal("Alice Smith", results[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Search_NumericRangeQuery()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "Young", "Age": 20 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Old", "Age": 50 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        db.ReloadSearcher();

        var results = db.Search(j => j["Age"] >= 25 && j["Age"] <= 60).ToList();
        Assert.Single(results);
        Assert.Equal("Old", results[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Search_FreeTextOnContent()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "Alice Wonder", "Age": 30 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Bob Builder", "Age": 25 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        db.ReloadSearcher();

        var results = db.Search(j => j["Name"].Contains("Wonder")).ToList();
        Assert.Single(results);
        Assert.Equal("Alice Wonder", results[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Search_NoQuery_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "A", "Age": 1 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "B", "Age": 2 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        db.ReloadSearcher();

        var results = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Equal(2, results.Count);
    }

    // === GetManyAsync ===

    [Fact]
    public async Task GetManyAsync_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "X", "Age": 1 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Y", "Age": 2 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        var results = await db.GetManyAsync(j => j.GetSchema() == "Person",
            cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task GetManyAsync_WithODataFilter()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "Young", "Age": 20 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Old", "Age": 50 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        var results = await db.GetManyAsync(j => j.GetSchema() == "Person" && j["Age"] > 30,
            cancellationToken: ct).ToListAsync(ct);
        Assert.Single(results);
        Assert.Equal("Old", results[0].RootElement.GetProperty("Name").GetString());
    }

    // === RebuildSearchIndex ===

    [Fact]
    public async Task RebuildSearchIndex_ReindexesDynamicDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "Alice Rebuild", "Age": 30 }""");
        doc1.SetSchema("Person");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Bob Rebuild", "Age": 25 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);

        db.ReloadSearcher();
        Assert.Equal(2, db.Search(j => j.GetSchema() == "Person").Count());

        db.DeleteSearchIndex();
        Assert.Empty(db.Search(j => j.GetSchema() == "Person"));

        await db.RebuildSearchIndex(ct);

        var results = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.RootElement.GetProperty("Name").GetString() == "Alice Rebuild");
        Assert.Contains(results, r => r.RootElement.GetProperty("Name").GetString() == "Bob Rebuild");
    }

    [Fact]
    public async Task RebuildSearchIndex_DynamicAndTypedCoexist()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc = JsonDocument.Parse("""{ "Name": "Dynamic Doc", "Age": 40 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);

        await db.SaveAsync(new Actor { Domain = "rebuild.test", Username = "typed-user", DisplayName = "Typed Doc" },
            ct);

        db.DeleteSearchIndex();
        Assert.Empty(db.Search(j => j.GetSchema() == "Person"));
        Assert.Empty(db.Search<Actor>().ToList());

        await db.RebuildSearchIndex(ct);

        var dynamicResults = db.Search(j => j.GetSchema() == "Person").ToList();
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
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "etag-dyn", "Name": "Test", "Age": 25 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);

        var result = (JsonDocument?)await db.GetAsync("etag-dyn", ct);
        Assert.NotNull(result);
        Assert.Equal("Test", result.RootElement.GetProperty("Name").GetString());
        Assert.NotEmpty(result.GetETag()!);
    }

    [Fact]
    public async Task SaveAsync_Dynamic_WithETag_ConditionalWrite_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "etag-dyn-save", "Name": "V1", "Age": 20 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);

        var result = (JsonDocument?)await db.GetAsync("etag-dyn-save", ct);
        Assert.NotNull(result);
        Assert.NotEmpty(result.GetETag()!);

        result.SetSchema("Person");
        await db.SaveAsync(result, ct);

        var loaded = (JsonDocument?)await db.GetAsync("etag-dyn-save", ct);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task SaveAsync_Dynamic_WithStaleETag_ThrowsConcurrencyException()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "etag-dyn-conflict", "Name": "V1", "Age": 20 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);

        var result = (JsonDocument?)await db.GetAsync("etag-dyn-conflict", ct);
        Assert.NotNull(result);
        var staleETag = result.GetETag()!;

        result.SetSchema("Person");
        await db.SaveAsync(result, ct);

        var v3 = JsonDocument.Parse("""{ "Id": "etag-dyn-conflict", "Name": "V3", "Age": 20 }""");
        v3.SetETag(staleETag);
        v3.SetSchema("Person");
        await Assert.ThrowsAsync<ConcurrencyException>(
            () => db.SaveAsync(v3, ct));
    }

    [Fact]
    public async Task Search_ReturnsETags()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var doc = JsonDocument.Parse("""{ "Name": "ETagSearch", "Age": 30 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["Name"] == "ETagSearch").ToList();
        Assert.Single(results);
        Assert.NotEmpty(results[0].GetETag()!);
    }

    // === SaveManyAsync ===

    [Fact]
    public async Task SaveManyAsync_Dynamic_BatchSavesAndIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var docs = Enumerable.Range(1, 5).Select(i =>
        {
            var d = JsonDocument.Parse($$$"""{ "Name": "Batch{{{i}}}", "Age": {{{i * 10}}} }""");
            d.SetSchema("Person");
            return (object)d;
        }).ToList();

        var result = await db.SaveManyAsync(docs, ct);
        Assert.Equal(5, result.Changes.Count);

        var all = await db.GetManyAsync(j => j.GetSchema() == "Person",
            cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(5, all.Count);

        db.ReloadSearcher();
        var searchResults = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Equal(5, searchResults.Count);
    }

    // === Dynamic ETag on SaveAsync ===

    [Fact]
    public async Task SaveAsync_Dynamic_SetsETagOnDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "etag-on-save", "Name": "ETagOnSave", "Age": 25 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);

        Assert.NotNull(json.GetETag());
        Assert.NotEmpty(json.GetETag()!);
    }

    [Fact]
    public async Task Search_AnnotatesETags()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var doc = JsonDocument.Parse("""{ "Name": "SearchETag", "Age": 30 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["Name"] == "SearchETag").ToList();
        Assert.Single(results);
        Assert.NotNull(results[0].GetETag());
        Assert.NotEmpty(results[0].GetETag()!);
    }

    [Fact]
    public async Task GetManyAsync_AnnotatesETags()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var doc = JsonDocument.Parse("""{ "Name": "GetManyETag", "Age": 30 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);

        var results = await db.GetManyAsync(j => j.GetSchema() == "Person",
            cancellationToken: ct).ToListAsync(ct);
        Assert.Single(results);
        Assert.NotNull(results[0].GetETag());
        Assert.NotEmpty(results[0].GetETag()!);
    }

    // === Error Handling ===

    [Fact]
    public async Task UnregisteredSchema_FallsBackToDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);
        var json = JsonDocument.Parse("""{ "Name": "test" }""");

        // SetSchema with an unregistered name falls back to the default schema
        json.SetSchema("NonExistent");
        await db.SaveAsync(json, ct);

        // The doc should be retrievable and saved under the default schema
        var key = json.GetKey()!;
        var loaded = (JsonDocument?)await db.GetAsync(key, ct);
        Assert.NotNull(loaded);
    }

    // === Overwrite / Upsert ===

    [Fact]
    public async Task SaveAsync_ExistingDocument_Overwrites()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "overwrite", "Name": "V1", "Age": 10 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);

        var v2 = JsonDocument.Parse("""{ "Id": "overwrite", "Name": "V2", "Age": 20 }""");
        v2.SetSchema("Person");
        await db.SaveAsync(v2, ct);

        var loaded = (JsonDocument?)await db.GetAsync("overwrite", ct);
        Assert.Equal("V2", loaded!.RootElement.GetProperty("Name").GetString());
        Assert.Equal(20, loaded.RootElement.GetProperty("Age").GetInt32());
    }

    [Fact]
    public async Task SaveAsync_ReturnsObjectResult()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Name": "Result", "Age": 25 }""");
        json.SetSchema("Person");

        var result = await db.SaveAsync(json, ct);

        Assert.NotEmpty(result.Changes);
        Assert.Equal(ChangeKind.Saved, result.Changes.First().Kind);
        Assert.NotEmpty(result.Changes.First().Key);
    }

    // === Get Edge Cases ===

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var loaded = (JsonDocument?)await db.GetAsync("nonexistent-key", ct);
        Assert.Null(loaded);
    }

    // === Delete Edge Cases ===

    [Fact]
    public async Task DeleteAsync_ReturnsObjectResult()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "del-result", "Name": "ToDelete", "Age": 99 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);

        var result = await db.DeleteAsync("del-result", ct);

        Assert.NotEmpty(result.Changes);
        Assert.Equal(ChangeKind.Deleted, result.Changes.First().Kind);
    }

    [Fact]
    public async Task DeleteAsync_NonExistent_NoError()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var result = await db.DeleteAsync("does-not-exist", ct);
        // Should not throw
        Assert.NotNull(result);
    }

    // === Search Visibility ===

    [Fact]
    public async Task Search_EmptyIndex_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        db.ReloadSearcher();
        var results = db.Search(j => j.GetSchema() == "Person").ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Search_ReflectsSave()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc = JsonDocument.Parse("""{ "Name": "Visible", "Age": 30 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        var results = db.Search(j => j["Name"] == "Visible").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task Search_ReflectsUpdate()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc = JsonDocument.Parse("""{ "Id": "update-vis", "Name": "Before", "Age": 10 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();
        Assert.Single(db.Search(j => j["Name"] == "Before").ToList());

        var doc2 = JsonDocument.Parse("""{ "Id": "update-vis", "Name": "After", "Age": 20 }""");
        doc2.SetSchema("Person");
        await db.SaveAsync(doc2, ct);
        db.ReloadSearcher();

        Assert.Empty(db.Search(j => j["Name"] == "Before").ToList());
        Assert.Single(db.Search(j => j["Name"] == "After").ToList());
    }

    [Fact]
    public async Task Search_ReflectsDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc = JsonDocument.Parse("""{ "Id": "del-vis", "Name": "Gone", "Age": 10 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();
        Assert.Single(db.Search(j => j["Name"] == "Gone").ToList());

        await db.DeleteAsync("del-vis", ct);
        db.ReloadSearcher();
        Assert.Empty(db.Search(j => j["Name"] == "Gone").ToList());
    }

    // === ETag Behavior ===

    [Fact]
    public async Task ETag_UpdatedInPlaceAfterSave()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var json = JsonDocument.Parse("""{ "Id": "etag-inplace", "Name": "V1", "Age": 10 }""");
        json.SetSchema("Person");
        await db.SaveAsync(json, ct);
        var etagAfterFirst = json.GetETag()!;

        // Save again (conditional write since ETag is present)
        await db.SaveAsync(json, ct);
        var etagAfterSecond = json.GetETag()!;

        Assert.NotEqual(etagAfterFirst, etagAfterSecond);
    }

    [Fact]
    public async Task Search_ThenConditionalSave_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc = JsonDocument.Parse("""{ "Id": "search-save", "Name": "Original", "Age": 30 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();

        var found = db.Search(j => j["Name"] == "Original").First();
        Assert.NotEmpty(found.GetETag()!);

        // ETag on the search result -> conditional save
        found.SetSchema("Person");
        await db.SaveAsync(found, ct);

        var loaded = (JsonDocument?)await db.GetAsync("search-save", ct);
        Assert.NotNull(loaded);
    }

    // === SaveManyAsync Edge Cases ===

    [Fact]
    public async Task SaveManyAsync_DuplicateKey_AutoFlushes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc1 = JsonDocument.Parse("""{ "Id": "dup", "Name": "First", "Age": 10 }""");
        doc1.SetSchema("Person");
        var doc2 = JsonDocument.Parse("""{ "Id": "dup", "Name": "Second", "Age": 20 }""");
        doc2.SetSchema("Person");
        var docs = new object[] { doc1, doc2 };

        var result = await db.SaveManyAsync(docs, ct);
        Assert.Equal(2, result.Changes.Count);

        // Last write wins
        var loaded = (JsonDocument?)await db.GetAsync("dup", ct);
        Assert.Equal("Second", loaded!.RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task SaveManyAsync_Empty_ReturnsEmptyResult()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var result = await db.SaveManyAsync(Array.Empty<object>(), ct);
        Assert.Empty(result.Changes);
    }

    [Fact]
    public async Task SaveManyAsync_Over100_AutoFlushes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var docs = Enumerable.Range(1, 150).Select(i =>
        {
            var d = JsonDocument.Parse($$$"""{ "Name": "Item{{{i}}}", "Age": {{{i}}} }""");
            d.SetSchema("Person");
            return (object)d;
        }).ToList();

        var result = await db.SaveManyAsync(docs, ct);
        Assert.Equal(150, result.Changes.Count);

        var all = await db.GetManyAsync(j => j.GetSchema() == "Person",
            cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(150, all.Count);
    }

    // === DeleteManyAsync ===

    [Fact]
    public async Task DeleteManyAsync_NoPredicate_DeletesAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var docA = JsonDocument.Parse("""{ "Name": "A", "Age": 1 }""");
        docA.SetSchema("Person");
        await db.SaveAsync(docA, ct);
        var docB = JsonDocument.Parse("""{ "Name": "B", "Age": 2 }""");
        docB.SetSchema("Person");
        await db.SaveAsync(docB, ct);
        var docC = JsonDocument.Parse("""{ "Name": "C", "Age": 3 }""");
        docC.SetSchema("Person");
        await db.SaveAsync(docC, ct);

        var result = await db.DeleteManyAsync(j => j.GetSchema() == "Person", cancellationToken: ct);
        Assert.Equal(3, result.Changes.Count);
        Assert.All(result.Changes, c => Assert.Equal(ChangeKind.Deleted, c.Kind));

        var remaining = await db.GetManyAsync(j => j.GetSchema() == "Person", cancellationToken: ct).ToListAsync(ct);
        Assert.Empty(remaining);
    }

    [Fact]
    public async Task DeleteManyAsync_WithFilter_DeletesMatching()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var docYoung = JsonDocument.Parse("""{ "Name": "Young", "Age": 20 }""");
        docYoung.SetSchema("Person");
        await db.SaveAsync(docYoung, ct);
        var docOld = JsonDocument.Parse("""{ "Name": "Old", "Age": 50 }""");
        docOld.SetSchema("Person");
        await db.SaveAsync(docOld, ct);

        await db.DeleteManyAsync(j => j.GetSchema() == "Person" && j["Age"] > 30, cancellationToken: ct);

        var remaining = await db.GetManyAsync(j => j.GetSchema() == "Person", cancellationToken: ct).ToListAsync(ct);
        Assert.Single(remaining);
        Assert.Equal("Young", remaining[0].RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task DeleteManyAsync_RemovesFromSearch()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);

        var doc = JsonDocument.Parse("""{ "Name": "SearchDel", "Age": 10 }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        db.ReloadSearcher();
        Assert.Single(db.Search(j => j["Name"] == "SearchDel").ToList());

        await db.DeleteManyAsync(j => j.GetSchema() == "Person", cancellationToken: ct);
        db.ReloadSearcher();
        Assert.Empty(db.Search(j => j["Name"] == "SearchDel").ToList());
    }

    [Fact]
    public async Task DeleteManyAsync_Empty_ReturnsEmptyResult()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbWithSchema(ct);
        var result = await db.DeleteManyAsync(j => j.GetSchema() == "Person", cancellationToken: ct);
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
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);

        await db.SaveAsync(new JsonSchema
        {
            Name = "Contact",
            Properties = new()
            {
                new() { Name = "Name", Type = "string" },
                new() { Name = "City", Type = "string", JsonPath = "$.address.city" },
            }
        }, ct);

        var doc1 = JsonDocument.Parse("""{ "Name": "Alice", "address": { "city": "Seattle", "zip": "98101" } }""");
        doc1.SetSchema("Contact");
        await db.SaveAsync(doc1, ct);
        var doc2 = JsonDocument.Parse("""{ "Name": "Bob", "address": { "city": "Portland", "zip": "97201" } }""");
        doc2.SetSchema("Contact");
        await db.SaveAsync(doc2, ct);

        db.ReloadSearcher();

        // Search by the nested field extracted via JsonPath
        var results = db.Search(j => j["City"] == "Seattle").ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].RootElement.GetProperty("Name").GetString());
    }
}
