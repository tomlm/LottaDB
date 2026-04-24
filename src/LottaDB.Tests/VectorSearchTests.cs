using ElBruno.LocalEmbeddings;
using ElBruno.LocalEmbeddings.Options;
using Lotta.Internal;
using Lucene.Net.Linq;
using Microsoft.Extensions.AI;

#pragma warning disable xUnit1051

namespace Lotta.Tests;

public class VectorSearchTests
{
    // Shared embedding generator — loaded once across all similarity tests
    private static readonly Lazy<IEmbeddingGenerator<string, Embedding<float>>> _generator =
        new(() => new LocalEmbeddingGenerator(new LocalEmbeddingsOptions
        {
            ModelName = "SmartComponents/bge-micro-v2",
            PreferQuantized = true
        }));

    private static Task<LottaDB> CreateVectorDbAsync([System.Runtime.CompilerServices.CallerMemberName] string? testName = null)
    {
        return LottaDBFixture.CreateDbAsync(config =>
        {
            config.EmbeddingGenerator = _generator.Value;
        }, testName: testName);
    }

    // =====================================================================
    // Metadata / Configuration (no embedding generator needed)
    // =====================================================================

    [Fact]
    public void QueryableMode_Vector_SetsIsVectorField()
    {
        var meta = TypeMetadata.Build<VectorNote>(null);
        var titleProp = meta.IndexedProperties.First(p => p.Property.Name == "Title");
        Assert.True(titleProp.IsVectorField);
        Assert.False(titleProp.IsNotAnalyzed); // vector fields are also analyzed
    }

    [Fact]
    public void QueryableMode_Vector_CategoryIsNotVector()
    {
        var meta = TypeMetadata.Build<VectorNote>(null);
        var catProp = meta.IndexedProperties.First(p => p.Property.Name == "Category");
        Assert.False(catProp.IsVectorField);
    }

    [Fact]
    public void FluentConfig_Vector_SetsIsVectorField()
    {
        var config = new StorageConfiguration<BareVectorNote>();
        config.SetKey(x => x.Id);
        config.AddQueryable(x => x.Title).Vector();
        config.AddQueryable(x => x.Category);

        var meta = TypeMetadata.Build<BareVectorNote>(config);
        var titleProp = meta.IndexedProperties.First(p => p.Property.Name == "Title");
        Assert.True(titleProp.IsVectorField);

        var catProp = meta.IndexedProperties.First(p => p.Property.Name == "Category");
        Assert.False(catProp.IsVectorField);
    }

    [Fact]
    public void LuceneLinq_VectorFieldAttribute_SetsIsVectorField()
    {
        var meta = TypeMetadata.Build<LuceneVectorDoc>(null);
        var titleProp = meta.IndexedProperties.First(p => p.Property.Name == "Title");
        Assert.True(titleProp.IsVectorField);

        var catProp = meta.IndexedProperties.First(p => p.Property.Name == "Category");
        Assert.False(catProp.IsVectorField);
    }

    [Fact]
    public void FluentConfig_AddField_Vector_SetsIsVectorField()
    {
        var config = new StorageConfiguration<BareVectorNote>();
        config.SetKey(x => x.Id);
        config.AddField(x => x.Title).Vector();

        var meta = TypeMetadata.Build<BareVectorNote>(config);
        var titleProp = meta.IndexedProperties.First(p => p.Property.Name == "Title");
        Assert.True(titleProp.IsVectorField);
    }

    // =====================================================================
    // Vector fields without embedding generator — no crash, just no vectors
    // =====================================================================

    [Fact]
    public async Task VectorField_WithoutEmbeddingGenerator_StillIndexesAndSearches()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "hello world", Category = "tech" });

        var results = db.Search<VectorNote>("hello").ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    // =====================================================================
    // Property-level .Similar() search
    // =====================================================================

    [Fact]
    public async Task Search_Similar_OnProperty_ReturnsResults()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Search_Similar_RanksSemanticallySimilarHigher()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" });
        await db.SaveAsync(new VectorNote { Id = "4", Title = "the big friendly bear eats honey in the forest", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "5", Title = "machine learning and neural network training", Category = "science" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
        // "a small kitten sleeping on a warm blanket" should rank first
        Assert.Equal("2", results[0].Id);
    }

    [Fact]
    public async Task Search_Similar_ScienceQueryRanksScienceHigher()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" });
        await db.SaveAsync(new VectorNote { Id = "4", Title = "machine learning and neural network training", Category = "science" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("deep learning artificial intelligence")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("science", results[0].Category);
    }

    [Fact]
    public async Task Search_Similar_WithFilter_OnlyMatchingResults()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics research", Category = "science" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("furry animals in nature") && n.Category == "animals").ToList();
        Assert.NotEmpty(results);
        Assert.All(results, r => Assert.Equal("animals", r.Category));
    }

    [Fact]
    public async Task Search_Similar_WithTake_LimitsResults()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "first document about cats", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "second document about dogs", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "third document about birds", Category = "animals" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("pets")).Take(2).ToList();
        Assert.True(results.Count <= 2);
    }

    // =====================================================================
    // Content-level .Similar() on object
    // =====================================================================

    [Fact]
    public async Task Search_Similar_OnObject_ReturnsResults()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping on a warm blanket" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory research paper" });

        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Search_Similar_OnObject_RanksSemanticallySimilarHigher()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog in the park" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping peacefully on a warm soft blanket" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory and particle research" });
        await db.SaveAsync(new VectorNote { Id = "4", Title = "machine learning", Category = "science", Body = "neural network training with large datasets" });

        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping on a bed")).ToList();
        Assert.NotEmpty(results);
        // The kitten sleeping document should rank first
        Assert.Equal("2", results[0].Id);
    }

    [Fact]
    public async Task Search_Similar_OnObject_WithTake()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "first", Body = "about cats" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "second", Body = "about dogs" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "third", Body = "about birds" });

        var results = db.Search<VectorNote>(n => n.Similar("pets")).Take(2).ToList();
        Assert.True(results.Count <= 2);
    }

    // =====================================================================
    // Existing search still works alongside vector fields
    // =====================================================================

    [Fact]
    public async Task Search_FreeText_StillWorksWithVectorFields()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "Lucene indexes documents", Category = "tech" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "Azure tables store rows", Category = "tech" });

        var results = db.Search<VectorNote>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Search_Predicate_StillWorksWithVectorFields()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "hello world", Category = "greetings" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "goodbye world", Category = "farewells" });

        var results = db.Search<VectorNote>(n => n.Category == "greetings").ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task SaveAndGet_WithVectorFields_PreservesData()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "test title", Category = "test", Body = "test body" });

        var result = await db.GetAsync<VectorNote>("1");
        Assert.NotNull(result);
        Assert.Equal("test title", result.Title);
        Assert.Equal("test", result.Category);
        Assert.Equal("test body", result.Body);
    }
}
