using System.Text.Json.Serialization;
using ElBruno.LocalEmbeddings;
using Lucene.Net.Linq;
using Lucene.Net.Linq.Mapping;
using Microsoft.Extensions.AI;

namespace Lotta.Tests;

/// <summary>A searchable document that receives each hit's relevance score.</summary>
public class ScoredNote
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string Category { get; set; } = "";

    [Queryable(Vector = true)]
    public string Title { get; set; } = "";

    /// <summary>Set on search results only; not persisted.</summary>
    [QueryScore, JsonIgnore]
    public float Score { get; set; }
}

/// <summary>Same as <see cref="ScoredNote"/>, with a <c>double</c> score property.</summary>
public class DoubleScoredNote
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable]
    public string Title { get; set; } = "";

    [QueryScore, JsonIgnore]
    public double Score { get; set; }
}

public abstract class QueryScoreTestBase : LottaTestBase
{
    // Shared embedding generator, loaded once. Uses the vendored model (see LocalModel).
    private static readonly Lazy<IEmbeddingGenerator<string, Embedding<float>>> _generator =
        new(() => new LocalEmbeddingGenerator(LocalModel.Options()));

    protected QueryScoreTestBase(Action<LottaCatalog> config) : base(config) { Catalog.EmbeddingGenerator = _generator.Value; }

    private async Task<LottaDB> SeedAsync(CancellationToken ct, [System.Runtime.CompilerServices.CallerMemberName] string? testName = null)
    {
        var db = await CreateDbAsync(c => c.Store<ScoredNote>(), cancellationToken: ct, testName: testName);
        await db.SaveManyAsync(
        [
            new ScoredNote { Id = "1", Category = "animals", Title = "a small kitten sleeping on a warm blanket" },
            new ScoredNote { Id = "2", Category = "animals", Title = "the quick brown fox jumps over the lazy dog" },
            new ScoredNote { Id = "3", Category = "science", Title = "quantum physics and string theory research" },
            new ScoredNote { Id = "4", Category = "animals", Title = "a kitten and another kitten playing" },
        ], ct);
        return db;
    }

    [Fact]
    public async Task TextQuery_FillsScoreInDescendingOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var results = db.Search<ScoredNote>("kitten").ToList();

        Assert.Equal(["1", "4"], results.Select(r => r.Id).Order());
        Assert.All(results, r => Assert.True(r.Score > 0, $"score {r.Score}"));
        Assert.Equal(results.Select(r => r.Score).OrderDescending(), results.Select(r => r.Score));
    }

    [Fact]
    public async Task HybridSimilarQuery_FillsNormalizedCosine()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var results = db.Search<ScoredNote>(n => n.Category == "animals" && n.Title.Similar("a cute cat napping")).ToList();

        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.InRange(r.Score, 0f, 1f));
        Assert.Equal("1", results[0].Id);
        Assert.True(results[0].Score > results[^1].Score);
    }

    [Fact]
    public async Task DoubleScoreProperty_IsFilled()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(c => c.Store<DoubleScoredNote>(), cancellationToken: ct);
        await db.SaveAsync(new DoubleScoredNote { Id = "1", Title = "kitten" }, ct);

        Assert.True(Assert.Single(db.Search<DoubleScoredNote>("kitten").ToList()).Score > 0);
    }

    [Fact]
    public async Task TypesWithoutScoreProperty_AreUnaffected()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { Domain = "score.test", NoteId = "n1", AuthorId = "alice", Content = "kitten facts", Published = DateTimeOffset.UtcNow }, ct);

        Assert.Single(db.Search<Note>(n => n.Content.Contains("kitten")).ToList());
    }

    [Fact]
    public async Task GetAsync_DoesNotSetOrPersistScore()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        var hit = db.Search<ScoredNote>("kitten").ToList().First();
        Assert.True(hit.Score > 0);

        await db.SaveAsync(hit, ct);
        var loaded = await db.GetAsync<ScoredNote>(hit.Id, ct);

        Assert.NotNull(loaded);
        Assert.Equal(0f, loaded.Score);
    }
}
