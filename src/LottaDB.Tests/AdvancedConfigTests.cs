using Lucene.Net.Linq.Mapping;
using System.Runtime.CompilerServices;

namespace Lotta.Tests;

/// <summary>
/// Verifies that [Tag], [Field], .AddTag(), and .AddField() work independently
/// as fine-grained alternatives to [Queryable] / .AddQueryable().
/// </summary>
public class AdvancedConfigTests
{
    // === Models with [Tag] only (Table Storage column, no Lucene index) ===

    public class TagOnlyModel
    {
        [Key]
        public string Id { get; set; } = "";

        [Tag]
        public string Category { get; set; } = "";

        public string Description { get; set; } = "";
    }

    // === Models with [Field] only (Lucene index, no Table Storage column) ===

    public class FieldOnlyModel
    {
        [Key]
        public string Id { get; set; } = "";

        [Field(IndexMode.NotAnalyzed)]
        public string Status { get; set; } = "";

        [Field]
        public string Body { get; set; } = "";
    }

    // === Model with both [Tag] and [Field] on different properties ===

    public class MixedModel
    {
        [Key]
        public string Id { get; set; } = "";

        [Tag]
        public string Category { get; set; } = "";

        [Field]
        public string Body { get; set; } = "";
    }

    // === Bare model for fluent tests ===

    public class BareModel
    {
        public string Id { get; set; } = "";
        public string Category { get; set; } = "";
        public string Body { get; set; } = "";
    }

    private static async Task<LottaDB> CreateDbAsync(Action<ILottaConfiguration> configAction,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null)
    {
        testName = String.Join(String.Empty, testName!.Where(char.IsLetterOrDigit).Take(60));

        var catalog = new LottaCatalog(testName!);
        catalog.ConfigureTestStorage();
        return await catalog.GetDatabaseAsync("default", config =>
        {
            configAction?.Invoke(config);
        }, cancellationToken);
    }

    // === [Tag] attribute tests ===

    [Fact]
    public async Task Tag_Attribute_QueryFilterWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config => config.Store<TagOnlyModel>(), ct);

        await db.SaveAsync(new TagOnlyModel { Id = "1", Category = "A", Description = "first" }, ct);
        await db.SaveAsync(new TagOnlyModel { Id = "2", Category = "B", Description = "second" }, ct);

        var results = await db.GetManyAsync<TagOnlyModel>(cancellationToken: ct)
            .Where(x => x.Category == "A")
            .ToListAsync(ct);
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Tag_Attribute_NotInLuceneIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config => config.Store<TagOnlyModel>(), ct);

        await db.SaveAsync(new TagOnlyModel { Id = "1", Category = "A", Description = "first" }, ct);

        // Search returns the object (via _json), but Category is not an indexed field
        var all = db.Search<TagOnlyModel>().ToList();
        Assert.Single(all);
        Assert.Equal("A", all[0].Category); // deserialized from _json
    }

    // === [Field] attribute tests ===

    [Fact]
    public async Task Field_Attribute_SearchFilterWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config => config.Store<FieldOnlyModel>(), ct);

        await db.SaveAsync(new FieldOnlyModel { Id = "1", Status = "active", Body = "hello world" }, ct);
        await db.SaveAsync(new FieldOnlyModel { Id = "2", Status = "archived", Body = "goodbye moon" }, ct);

        var results = db.Search<FieldOnlyModel>()
            .Where(x => x.Status == "active")
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Field_Attribute_AnalyzedSearchWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config => config.Store<FieldOnlyModel>(), ct);

        await db.SaveAsync(new FieldOnlyModel { Id = "1", Status = "active", Body = "hello world" }, ct);
        await db.SaveAsync(new FieldOnlyModel { Id = "2", Status = "archived", Body = "goodbye moon" }, ct);

        var results = db.Search<FieldOnlyModel>()
            .Where(x => x.Body.Contains("hello"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Field_Attribute_NotATableStorageTag()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config => config.Store<FieldOnlyModel>(), ct);

        await db.SaveAsync(new FieldOnlyModel { Id = "1", Status = "active", Body = "test" }, ct);

        // Query returns the object (via _json), but Status is not a table storage tag,
        // so it's only filterable client-side not server-side
        var all = await db.GetManyAsync<FieldOnlyModel>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(all);
        Assert.Equal("active", all[0].Status);
    }

    // === Mixed [Tag] + [Field] on different properties ===

    [Fact]
    public async Task Mixed_TagQueryAndFieldSearch()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config => config.Store<MixedModel>(), ct);

        await db.SaveAsync(new MixedModel { Id = "1", Category = "news", Body = "breaking news today" }, ct);
        await db.SaveAsync(new MixedModel { Id = "2", Category = "sports", Body = "big game tonight" }, ct);

        // Query by tag
        var byCategory = await db.GetManyAsync<MixedModel>(x => x.Category == "news", cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(byCategory);
        Assert.Equal("1", byCategory[0].Id);

        // Search by field
        var byBody = db.Search<MixedModel>()
            .Where(x => x.Body.Contains("game"))
            .ToList();
        Assert.Single(byBody);
        Assert.Equal("2", byBody[0].Id);
    }

    // === Fluent .AddTag() tests ===

    [Fact]
    public async Task Fluent_AddTag_QueryFilterWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddTag(x => x.Category);
            });
        }, ct);

        await db.SaveAsync(new BareModel { Id = "1", Category = "A" }, ct);
        await db.SaveAsync(new BareModel { Id = "2", Category = "B" }, ct);

        var results = await db.GetManyAsync<BareModel>(x => x.Category == "A", cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    // === Fluent .AddField() tests ===

    [Fact]
    public async Task Fluent_AddField_SearchFilterWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddField(x => x.Body);
            });
        }, ct);

        await db.SaveAsync(new BareModel { Id = "1", Body = "hello world" }, ct);
        await db.SaveAsync(new BareModel { Id = "2", Body = "goodbye moon" }, ct);

        var results = db.Search<BareModel>()
            .Where(x => x.Body.Contains("hello"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Fluent_AddField_NotAnalyzed_ExactMatchWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddField(x => x.Category).NotAnalyzed();
            });
        }, ct);

        await db.SaveAsync(new BareModel { Id = "1", Category = "active" }, ct);
        await db.SaveAsync(new BareModel { Id = "2", Category = "archived" }, ct);

        var results = db.Search<BareModel>()
            .Where(x => x.Category == "active")
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    // === Fluent .AddTag() + .AddField() together ===

    [Fact]
    public async Task Fluent_AddTag_And_AddField_Together()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateDbAsync(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddTag(x => x.Category);
                s.AddField(x => x.Body);
            });
        }, ct);

        await db.SaveAsync(new BareModel { Id = "1", Category = "news", Body = "breaking news today" }, ct);
        await db.SaveAsync(new BareModel { Id = "2", Category = "sports", Body = "big game tonight" }, ct);

        // Query by tag
        var byCategory = await db.GetManyAsync<BareModel>(x => x.Category == "news", cancellationToken: ct)
            .ToListAsync(ct);
        Assert.Single(byCategory);

        // Search by field
        var byBody = db.Search<BareModel>()
            .Where(x => x.Body.Contains("game"))
            .ToList();
        Assert.Single(byBody);
    }
}
