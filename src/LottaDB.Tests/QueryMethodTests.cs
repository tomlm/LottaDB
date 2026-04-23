using ElBruno.LocalEmbeddings;
using ElBruno.LocalEmbeddings.Options;
using Lucene.Net.Linq;
using Microsoft.Extensions.AI;

#pragma warning disable xUnit1051

namespace Lotta.Tests;

/// <summary>
/// Tests for LuceneMethods.Query() and LuceneMethods.Similar() via Search predicates,
/// verifying both default-property (object-level) and named-property usage.
/// </summary>
public class QueryMethodTests
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
    // .Query() on object — default property (_content_) query
    // =====================================================================

    [Fact]
    public async Task Query_OnObject_DefaultProperty_MatchesContent()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene indexes documents" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure tables store rows" });

        // t.Query("lucene") should search the default _content_ field
        var results = db.Search<Note>(n => n.Query("lucene")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Query_OnObject_DefaultProperty_NoMatch_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello world" });

        var results = db.Search<Note>(n => n.Query("banana")).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Query_OnObject_DefaultProperty_Wildcard()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene search engine" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure storage" });

        var results = db.Search<Note>(n => n.Query("luc*")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    // =====================================================================
    // .Query() on property — named field query
    // =====================================================================

    [Fact]
    public async Task Query_OnProperty_MatchesNamedField()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "bob wrote something" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "alice wrote something" });

        // n.Content.Query("bob") targets the Content field specifically
        var results = db.Search<Note>(n => n.Content.Query("bob")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Query_OnProperty_DoesNotMatchOtherFields()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello world" });

        // "alice" is in AuthorId but not in Content — property query on Content should miss it
        var results = db.Search<Note>(n => n.Content.Query("alice")).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Query_OnProperty_Wildcard()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new FeedEntry { Id = "f1", NoteViewId = "nv1", Title = "Introduction to Lucene" });
        await db.SaveAsync(new FeedEntry { Id = "f2", NoteViewId = "nv2", Title = "Azure storage overview" });

        var results = db.Search<FeedEntry>(f => f.Title.Query("intro*")).ToList();
        Assert.Single(results);
        Assert.Equal("f1", results[0].Id);
    }

    // =====================================================================
    // .Similar() on object — default property (_content_) similarity
    // =====================================================================

    [Fact]
    public async Task Similar_OnObject_DefaultProperty_ReturnsResults()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping on a warm blanket" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory research paper" });

        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Similar_OnObject_DefaultProperty_RanksSemantically()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog in the park" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping peacefully on a warm soft blanket" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory and particle research" });

        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping on a bed")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("2", results[0].Id);
    }

    // =====================================================================
    // .Similar() on property — named field similarity
    // =====================================================================

    [Fact]
    public async Task Similar_OnProperty_ReturnsResults()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Similar_OnProperty_RanksSemantically()
    {
        using var db = await CreateVectorDbAsync();
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" });
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" });

        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("2", results[0].Id);
    }
}
