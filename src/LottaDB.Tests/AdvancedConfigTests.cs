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

    private static LottaDB CreateDb(Action<ILottaConfiguration> configAction,
        [CallerMemberName] string? testName = null)
    {
        testName = String.Join(String.Empty, testName!.Where(char.IsLetterOrDigit).Take(60));

        return new LottaDB(testName!, "UseDevelopmentStorage=true", config =>
        {
            config.ConfigureTestStorage();

            configAction?.Invoke(config);
        });
    }

    // === [Tag] attribute tests ===

    [Fact]
    public async Task Tag_Attribute_QueryFilterWorks()
    {
        using var db = CreateDb(config => config.Store<TagOnlyModel>());

        await db.SaveAsync(new TagOnlyModel { Id = "1", Category = "A", Description = "first" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new TagOnlyModel { Id = "2", Category = "B", Description = "second" }, TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync<TagOnlyModel>(cancellationToken: TestContext.Current.CancellationToken)
            .Where(x => x.Category == "A")
            .ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Tag_Attribute_NotInLuceneIndex()
    {
        using var db = CreateDb(config => config.Store<TagOnlyModel>());

        await db.SaveAsync(new TagOnlyModel { Id = "1", Category = "A", Description = "first" }, TestContext.Current.CancellationToken);

        // Search returns the object (via _json), but Category is not an indexed field
        var all = db.Search<TagOnlyModel>().ToList();
        Assert.Single(all);
        Assert.Equal("A", all[0].Category); // deserialized from _json
    }

    // === [Field] attribute tests ===

    [Fact]
    public async Task Field_Attribute_SearchFilterWorks()
    {
        using var db = CreateDb(config => config.Store<FieldOnlyModel>());

        await db.SaveAsync(new FieldOnlyModel { Id = "1", Status = "active", Body = "hello world" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new FieldOnlyModel { Id = "2", Status = "archived", Body = "goodbye moon" }, TestContext.Current.CancellationToken);

        var results = db.Search<FieldOnlyModel>()
            .Where(x => x.Status == "active")
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Field_Attribute_AnalyzedSearchWorks()
    {
        using var db = CreateDb(config => config.Store<FieldOnlyModel>());

        await db.SaveAsync(new FieldOnlyModel { Id = "1", Status = "active", Body = "hello world" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new FieldOnlyModel { Id = "2", Status = "archived", Body = "goodbye moon" }, TestContext.Current.CancellationToken);

        var results = db.Search<FieldOnlyModel>()
            .Where(x => x.Body.Contains("hello"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Field_Attribute_NotATableStorageTag()
    {
        using var db = CreateDb(config => config.Store<FieldOnlyModel>());

        await db.SaveAsync(new FieldOnlyModel { Id = "1", Status = "active", Body = "test" }, TestContext.Current.CancellationToken);

        // Query returns the object (via _json), but Status is not a table storage tag,
        // so it's only filterable client-side not server-side
        var all = await db.GetManyAsync<FieldOnlyModel>(cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(all);
        Assert.Equal("active", all[0].Status);
    }

    // === Mixed [Tag] + [Field] on different properties ===

    [Fact]
    public async Task Mixed_TagQueryAndFieldSearch()
    {
        using var db = CreateDb(config => config.Store<MixedModel>());

        await db.SaveAsync(new MixedModel { Id = "1", Category = "news", Body = "breaking news today" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new MixedModel { Id = "2", Category = "sports", Body = "big game tonight" }, TestContext.Current.CancellationToken);

        // Query by tag
        var byCategory = await db.GetManyAsync<MixedModel>(x => x.Category == "news")
            .ToListAsync(TestContext.Current.CancellationToken);
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
        using var db = CreateDb(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddTag(x => x.Category);
            });
        });

        await db.SaveAsync(new BareModel { Id = "1", Category = "A" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareModel { Id = "2", Category = "B" }, TestContext.Current.CancellationToken);

        var results = await db.GetManyAsync<BareModel>(x => x.Category == "A")
            .ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    // === Fluent .AddField() tests ===

    [Fact]
    public async Task Fluent_AddField_SearchFilterWorks()
    {
        using var db = CreateDb(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddField(x => x.Body);
            });
        });

        await db.SaveAsync(new BareModel { Id = "1", Body = "hello world" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareModel { Id = "2", Body = "goodbye moon" }, TestContext.Current.CancellationToken);

        var results = db.Search<BareModel>()
            .Where(x => x.Body.Contains("hello"))
            .ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Fluent_AddField_NotAnalyzed_ExactMatchWorks()
    {
        using var db = CreateDb(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddField(x => x.Category).NotAnalyzed();
            });
        });

        await db.SaveAsync(new BareModel { Id = "1", Category = "active" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareModel { Id = "2", Category = "archived" }, TestContext.Current.CancellationToken);

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
        using var db = CreateDb(config =>
        {
            config.Store<BareModel>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddTag(x => x.Category);
                s.AddField(x => x.Body);
            });
        });

        await db.SaveAsync(new BareModel { Id = "1", Category = "news", Body = "breaking news today" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BareModel { Id = "2", Category = "sports", Body = "big game tonight" }, TestContext.Current.CancellationToken);

        // Query by tag
        var byCategory = await db.GetManyAsync<BareModel>(x => x.Category == "news")
            .ToListAsync(TestContext.Current.CancellationToken);
        Assert.Single(byCategory);

        // Search by field
        var byBody = db.Search<BareModel>()
            .Where(x => x.Body.Contains("game"))
            .ToList();
        Assert.Single(byBody);
    }
}
