using ElBruno.LocalEmbeddings;
using ElBruno.LocalEmbeddings.Options;
using Lucene.Net.Linq;
using Microsoft.Extensions.AI;

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

    private static Task<LottaDB> CreateVectorDbAsync(CancellationToken cancellationToken = default, [System.Runtime.CompilerServices.CallerMemberName] string? testName = null)
    {
        return LottaDBFixture.CreateDbAsync(
            configureCatalog: catalog => catalog.EmbeddingGenerator = _generator.Value,
            cancellationToken: cancellationToken,
            testName: testName);
    }

    // =====================================================================
    // .Query() on object — default property (_content_) query
    // =====================================================================

    [Fact]
    public async Task Query_OnObject_DefaultProperty_MatchesContent()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene indexes documents" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure tables store rows" }, ct);

        // t.Query("lucene") should search the default _content_ field
        var results = db.Search<Note>(n => n.Query("lucene")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Query_OnObject_DefaultProperty_NoMatch_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello world" }, ct);

        var results = db.Search<Note>(n => n.Query("banana")).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Query_OnObject_DefaultProperty_Wildcard()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene search engine" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure storage" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "bob wrote something" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "alice wrote something" }, ct);

        // n.Content.Query("bob") targets the Content field specifically
        var results = db.Search<Note>(n => n.Content.Query("bob")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task Query_OnProperty_DoesNotMatchOtherFields()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello world" }, ct);

        // "alice" is in AuthorId but not in Content — property query on Content should miss it
        var results = db.Search<Note>(n => n.Content.Query("alice")).ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Query_OnProperty_Wildcard()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new FeedEntry { Id = "f1", NoteViewId = "nv1", Title = "Introduction to Lucene" }, ct);
        await db.SaveAsync(new FeedEntry { Id = "f2", NoteViewId = "nv2", Title = "Azure storage overview" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping on a warm blanket" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory research paper" }, ct);

        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Similar_OnObject_DefaultProperty_RanksSemantically()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog in the park" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping peacefully on a warm soft blanket" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory and particle research" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" }, ct);

        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Similar_OnProperty_RanksSemantically()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" }, ct);

        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("2", results[0].Id);
    }

    // =====================================================================
    // [DefaultSearch] — user-defined default search property
    // =====================================================================

    [Fact]
    public async Task DefaultSearch_SearchString_UsesUserDefinedProperty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Article { Id = "a1", Title = "Lucene", Body = "full-text search engine" }, ct);
        await db.SaveAsync(new Article { Id = "a2", Title = "Azure", Body = "cloud platform" }, ct);

        // Search("Lucene") should target the Content property (which is Title + Body)
        var results = db.Search<Article>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("a1", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_QueryOnObject_UsesUserDefinedProperty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Article { Id = "a1", Title = "Lucene", Body = "full-text search engine" }, ct);
        await db.SaveAsync(new Article { Id = "a2", Title = "Azure", Body = "cloud platform" }, ct);

        // t.Query("...") on object should target Content property
        var results = db.Search<Article>(a => a.Query("lucene")).ToList();
        Assert.Single(results);
        Assert.Equal("a1", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_QueryOnProperty_StillWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Article { Id = "a1", Title = "Lucene search", Body = "engine" }, ct);
        await db.SaveAsync(new Article { Id = "a2", Title = "Azure cloud", Body = "Lucene compatible" }, ct);

        // Property-level query targets Title specifically, not Content
        var results = db.Search<Article>(a => a.Title.Query("lucene*")).ToList();
        Assert.Single(results);
        Assert.Equal("a1", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_ComposedContent_SearchesAcrossFields()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Article { Id = "a1", Title = "Intro", Body = "Lucene is a search engine" }, ct);

        // "lucene" is only in Body, but Content = Title + Body, so it should match
        var results = db.Search<Article>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("a1", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_Fluent_UsesUserDefinedProperty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config =>
        {
            config.Store<BareArticle>(s =>
            {
                s.SetKey(x => x.Id);
                s.AddQueryable(x => x.Title);
                s.AddQueryable(x => x.Body);
                s.AddQueryable(x => x.Content);
                s.DefaultSearch(x => x.Content);
            });
        }, cancellationToken: ct);
        await db.SaveAsync(new BareArticle { Id = "a1", Title = "Lucene", Body = "search engine" }, ct);
        await db.SaveAsync(new BareArticle { Id = "a2", Title = "Azure", Body = "cloud platform" }, ct);

        var results = db.Search<BareArticle>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("a1", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_Similar_OnObject_UsesUserDefinedProperty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new Article { Id = "a1", Title = "the quick brown fox", Body = "jumps over the lazy dog" }, ct);
        await db.SaveAsync(new Article { Id = "a2", Title = "a small kitten", Body = "sleeping on a warm blanket" }, ct);
        await db.SaveAsync(new Article { Id = "a3", Title = "quantum physics", Body = "string theory research" }, ct);

        // a.Similar("...") should target Content (the DefaultSearch property)
        var results = db.Search<Article>(a => a.Similar("cute cat napping")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("a2", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_Similar_OnProperty_StillWorks()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new Article { Id = "a1", Title = "the quick brown fox jumps over the lazy dog", Body = "nature documentary" }, ct);
        await db.SaveAsync(new Article { Id = "a2", Title = "a small kitten sleeping on a warm blanket", Body = "pet care" }, ct);
        await db.SaveAsync(new Article { Id = "a3", Title = "quantum physics and string theory research", Body = "science journal" }, ct);

        // a.Title.Similar("...") targets Title specifically, not Content
        var results = db.Search<Article>(a => a.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("a2", results[0].Id);
    }

    [Fact]
    public async Task DefaultSearch_InvalidProperty_ThrowsAtInit()
    {
        var ct = TestContext.Current.CancellationToken;
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            using var db = await LottaDBFixture.CreateDbAsync(config =>
            {
                config.Store<BadDefaultSearch>();
            }, cancellationToken: ct);
        });

        // The InvalidOperationException may be wrapped in TargetInvocationException
        // due to reflection-based initialization
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException! : ex;
        Assert.IsType<InvalidOperationException>(inner);
        Assert.Contains("NotIndexed", inner.Message);
    }
}
