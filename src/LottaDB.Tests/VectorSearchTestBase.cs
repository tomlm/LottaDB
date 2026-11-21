using ElBruno.LocalEmbeddings;
using ElBruno.LocalEmbeddings.Options;
using Lotta.Internal;
using Lucene.Net.Linq;
using Microsoft.Extensions.AI;

namespace Lotta.Tests;

public abstract class VectorSearchTestBase : LottaTestBase
{
    // Shared embedding generator — loaded once across all similarity tests.
    // Uses the model vendored at /models/bge-micro-v2 so no download happens. See LocalModel.
    private static readonly Lazy<IEmbeddingGenerator<string, Embedding<float>>> _generator =
        new(() => new LocalEmbeddingGenerator(LocalModel.Options()));

    protected VectorSearchTestBase(Action<LottaCatalog> config) : base(config) { Catalog.EmbeddingGenerator = _generator.Value; }

    private async Task<LottaDB> CreateVectorDbAsync(CancellationToken cancellationToken = default, [System.Runtime.CompilerServices.CallerMemberName] string? testName = null)
    {
        return await CreateDbAsync(config => config.QueueMode = QueueMode.Manual, cancellationToken: cancellationToken, testName: testName);
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
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(cancellationToken: ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "hello world", Category = "tech" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Search_Similar_RanksSemanticallySimilarHigher()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" }, ct);
        await db.SaveAsync(new VectorNote { Id = "4", Title = "the big friendly bear eats honey in the forest", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "5", Title = "machine learning and neural network training", Category = "science" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.NotEmpty(results);
        // "a small kitten sleeping on a warm blanket" should rank first
        Assert.Equal("2", results[0].Id);
    }

    [Fact]
    public async Task Search_Similar_ScienceQueryRanksScienceHigher()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" }, ct);
        await db.SaveAsync(new VectorNote { Id = "4", Title = "machine learning and neural network training", Category = "science" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Title.Similar("deep learning artificial intelligence")).ToList();
        Assert.NotEmpty(results);
        Assert.Equal("science", results[0].Category);
    }

    [Fact]
    public async Task Search_Similar_WithFilter_OnlyMatchingResults()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics research", Category = "science" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Title.Similar("furry animals in nature") && n.Category == "animals").ToList();
        Assert.NotEmpty(results);
        Assert.All(results, r => Assert.Equal("animals", r.Category));
    }

    [Fact]
    public async Task Search_Similar_WithTake_LimitsResults()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "first document about cats", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "second document about dogs", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "third document about birds", Category = "animals" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Title.Similar("pets")).Take(2).ToList();
        Assert.True(results.Count <= 2);
    }

    // =====================================================================
    // Content-level .Similar() on object
    // =====================================================================

    [Fact]
    public async Task Search_Similar_OnObject_ReturnsResults()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping on a warm blanket" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory research paper" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping")).ToList();
        Assert.NotEmpty(results);
    }

    [Fact]
    public async Task Search_Similar_OnObject_RanksSemanticallySimilarHigher()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox", Category = "animals", Body = "jumps over the lazy dog in the park" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "animals", Body = "sleeping peacefully on a warm soft blanket" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science", Body = "string theory and particle research" }, ct);
        await db.SaveAsync(new VectorNote { Id = "4", Title = "machine learning", Category = "science", Body = "neural network training with large datasets" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Similar("cute cat napping on a bed")).ToList();
        Assert.NotEmpty(results);
        // The kitten sleeping document should rank first
        Assert.Equal("2", results[0].Id);
    }

    [Fact]
    public async Task Search_Similar_OnObject_WithTake()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "first", Body = "about cats" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "second", Body = "about dogs" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "third", Body = "about birds" }, ct);

        await db.DrainAsync(cancellationToken: ct);
        var results = db.Search<VectorNote>(n => n.Similar("pets")).Take(2).ToList();
        Assert.True(results.Count <= 2);
    }

    // =====================================================================
    // Existing search still works alongside vector fields
    // =====================================================================

    [Fact]
    public async Task Search_FreeText_StillWorksWithVectorFields()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(cancellationToken: ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "Lucene indexes documents", Category = "tech" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "Azure tables store rows", Category = "tech" }, ct);

        var results = db.Search<VectorNote>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task Search_Predicate_StillWorksWithVectorFields()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(cancellationToken: ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "hello world", Category = "greetings" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "goodbye world", Category = "farewells" }, ct);

        var results = db.Search<VectorNote>(n => n.Category == "greetings").ToList();
        Assert.Single(results);
        Assert.Equal("1", results[0].Id);
    }

    [Fact]
    public async Task SaveAndGet_WithVectorFields_PreservesData()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateVectorDbAsync(cancellationToken: ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "test title", Category = "test", Body = "test body" }, ct);

        var result = await db.GetAsync<VectorNote>("1", ct);
        Assert.NotNull(result);
        Assert.Equal("test title", result.Title);
        Assert.Equal("test", result.Category);
        Assert.Equal("test body", result.Body);
    }

    // =====================================================================
    // Vectors stored on the row: the model is called only for new text
    // =====================================================================

    /// <summary>The shared generator, counting the texts it is asked to embed.</summary>
    private sealed class CountingGenerator(IEmbeddingGenerator<string, Embedding<float>> inner)
        : DelegatingEmbeddingGenerator<string, Embedding<float>>(inner)
    {
        private int _texts, _calls;
        public int Texts => _texts;
        public int Calls => _calls;

        /// <summary>Run once, during the next call: what happens while a batch is being embedded.</summary>
        public Func<Task>? During;

        /// <summary>Set to make every call fail.</summary>
        public bool Fail;

        public override async Task<GeneratedEmbeddings<Embedding<float>>> GenerateAsync(
            IEnumerable<string> values, EmbeddingGenerationOptions? options = null, CancellationToken cancellationToken = default)
        {
            var list = values.ToList();
            Interlocked.Add(ref _texts, list.Count);
            Interlocked.Increment(ref _calls);
            if (Fail)
                throw new InvalidOperationException("The model is not there.");
            if (Interlocked.Exchange(ref During, null) is { } during)
                await during();
            return await base.GenerateAsync(list, options, cancellationToken);
        }
    }

    private CountingGenerator CountEmbeddings()
    {
        var counting = new CountingGenerator(_generator.Value);
        Catalog.EmbeddingGenerator = counting;
        return counting;
    }

    [Fact]
    public async Task ASave_DoesNotCallTheModel_TheEmbedRuleDoes()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);

        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals", Body = "it sleeps on a blanket" }, ct);

        // Saved, and found by its words, before it is embedded.
        Assert.Equal(0, model.Texts);
        Assert.Equal("1", db.Search<VectorNote>("kitten").Single().Id);
        Assert.Empty(db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).ToList());
        var beforeDrain = model.Texts;   // the search embedded its query text

        await db.DrainAsync(cancellationToken: ct);

        // Title, Body and the _content field, in one run of the rule.
        Assert.Equal(beforeDrain + 3, model.Texts);
        Assert.Equal("1", db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).Single().Id);
        // The rule's own write leaves nothing more to do.
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    // =====================================================================
    // The embed rule runs in batches
    // =====================================================================

    private Task<LottaDB> CreateBatchDbAsync(CancellationToken ct, int maxAttempts = 5, [System.Runtime.CompilerServices.CallerMemberName] string? testName = null) =>
        CreateDbAsync(config =>
        {
            config.QueueMode = QueueMode.Manual;
            // One run at a time, so the batch is all of them.
            config.MaxConcurrency = 1;
            config.RuleMaxAttempts = maxAttempts;
        }, cancellationToken: ct, testName: testName);

    private static async Task SaveNotesAsync(LottaDB db, int count, CancellationToken ct)
    {
        for (var i = 1; i <= count; i++)
            await db.SaveAsync(new VectorNote { Id = $"{i}", Title = $"kitten number {i} sleeps", Category = "animals" }, ct);
    }

    private static async Task<RuleTypeStatus> StatusOf<T>(LottaDB db, CancellationToken ct) =>
        (await db.Rules.StatusAsync(ct)).Types.Single(t => t.Type == typeof(T).FullName);

    [Fact]
    public async Task ManySaves_AreEmbeddedInOneCall()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateBatchDbAsync(ct);
        await SaveNotesAsync(db, 10, ct);

        // One run of the rule per object, counted as ten, in one call to the model.
        Assert.Equal(10, await db.DrainAsync(cancellationToken: ct));
        Assert.Equal(1, model.Calls);
        Assert.Equal(20, model.Texts);   // Title and _content, for each
        Assert.Equal(10, db.Search<VectorNote>(n => n.Title.Similar("a sleeping cat")).Take(20).ToList().Count);
        Assert.Equal(0, await db.DrainAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task TheSameTextInABatch_IsEmbeddedOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateBatchDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten", Category = "pets" }, ct);

        await db.DrainAsync(cancellationToken: ct);

        // The two titles are one text; the _content fields differ.
        Assert.Equal(3, model.Texts);
        Assert.Equal(2, db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).ToList().Count);
    }

    [Fact]
    public async Task AnObjectSavedWhileItsBatchIsEmbedded_KeepsTheChange_AndAllGetTheirVectors()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateBatchDbAsync(ct);
        await SaveNotesAsync(db, 3, ct);
        // Saved by someone else meanwhile: the batch's write of all three is refused, and each is written by itself.
        model.During = () => db.ChangeAsync<VectorNote>("2", n => n.Category = "pets", ct);

        while (await db.DrainAsync(cancellationToken: ct) > 0) { }

        Assert.Equal("pets", (await db.GetAsync<VectorNote>("2", ct))!.Category);
        Assert.Equal(3, db.Search<VectorNote>(n => n.Title.Similar("a sleeping cat")).ToList().Count);
        var status = await StatusOf<VectorNote>(db, ct);
        Assert.Equal(0, status.Waiting);
        Assert.Equal(0, status.Poisoned);
    }

    [Fact]
    public async Task AnObjectDeletedBeforeItsBatch_IsDropped_AndTheRestEmbedded()
    {
        var ct = TestContext.Current.CancellationToken;
        CountEmbeddings();
        var db = await CreateBatchDbAsync(ct);
        await SaveNotesAsync(db, 3, ct);
        await db.DeleteAsync<VectorNote>("2", ct);

        Assert.Equal(2, await db.DrainAsync(cancellationToken: ct));

        Assert.Equal(["1", "3"], db.Search<VectorNote>(n => n.Title.Similar("a sleeping cat")).Select(n => n.Id).ToList().Order());
        Assert.Equal(0, (await StatusOf<VectorNote>(db, ct)).Waiting);
    }

    [Fact]
    public async Task AFailedBatch_IsAFailureOfEachObject_SetAsideOneByOne()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        model.Fail = true;
        var db = await CreateBatchDbAsync(ct, maxAttempts: 1);
        await SaveNotesAsync(db, 3, ct);

        Assert.Equal(3, await db.DrainAsync(cancellationToken: ct));

        Assert.Equal(1, model.Calls);
        var status = await StatusOf<VectorNote>(db, ct);
        Assert.Equal(0, status.Waiting);
        Assert.Equal(3, status.Poisoned);
        Assert.Equal(3, (await db.Rules.StatusAsync(ct)).Rules.Single(r => r.Rule.EndsWith("VectorNote/embed")).Poisoned);
    }

    [Fact]
    public async Task ADrainWithALimit_EmbedsOneObjectARun()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateBatchDbAsync(ct);
        await SaveNotesAsync(db, 3, ct);

        Assert.Equal(2, await db.DrainAsync(max: 2, cancellationToken: ct));
        Assert.Equal(2, model.Calls);
        Assert.Equal(1, (await StatusOf<VectorNote>(db, ct)).Waiting);
    }

    // =====================================================================
    // GetVector: the stored vector of an object, to search near it
    // =====================================================================

    private static double Cosine(float[] a, float[] b)
    {
        double dot = 0, x = 0, y = 0;
        for (var i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            x += a[i] * a[i];
            y += b[i] * b[i];
        }
        return dot / Math.Sqrt(x * y);
    }

    [Fact]
    public async Task GetVector_IsTheStoredVectorOfTheTextAsItIsNow()
    {
        var ct = TestContext.Current.CancellationToken;
        CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);

        // Not embedded yet.
        Assert.Null(db.GetVector((await db.GetAsync<VectorNote>("1", ct))!, nameof(VectorNote.Title)));

        await db.DrainAsync(cancellationToken: ct);
        var note = (await db.GetAsync<VectorNote>("1", ct))!;
        var title = db.GetVector(note, nameof(VectorNote.Title));
        var alone = (await Catalog.EmbeddingGenerator!.GenerateAsync(["a small kitten"], cancellationToken: ct))[0].Vector.ToArray();
        Assert.NotNull(title);
        // The vector of the title's text. Not equal to the last bit: the rule embedded the title in one
        // call with the _content text, and a quantized model (as the tests use) scales by the batch.
        Assert.True(Cosine(alone, title) > 0.95, $"cosine {Cosine(alone, title)}");
        // The _content field, which VectorNote has as it has no default search property.
        Assert.NotNull(db.GetVector(note));
        // Read by a bulk read, too.
        Assert.Equal(title, db.GetVector(await db.GetManyAsync<VectorNote>(cancellationToken: ct).SingleAsync(ct), nameof(VectorNote.Title)));

        // Edited in hand: the stored vector is no longer that of the text.
        note.Title = "quantum physics";
        Assert.Null(db.GetVector(note, nameof(VectorNote.Title)));
        // Not a vector field, or not one of ours.
        Assert.Null(db.GetVector(note, nameof(VectorNote.Category)));
        Assert.Null(db.GetVector(new VectorNote { Id = "2", Title = "a small kitten" }, nameof(VectorNote.Title)));
    }

    [Fact]
    public async Task SimilarToAStoredVector_FindsLikeObjects_WithoutCallingTheModel()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a sleepy cat", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics", Category = "science" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var kitten = db.GetVector((await db.GetAsync<VectorNote>("1", ct))!, nameof(VectorNote.Title))!;
        var calls = model.Calls;

        var near = db.Search<VectorNote>(n => n.Title.Similar(kitten)).Select(n => n.Id).ToList();

        Assert.Equal(calls, model.Calls);   // nothing embedded: the query is the stored vector
        Assert.Equal("1", near[0]);          // itself first
        Assert.Equal("2", near[1]);          // then the cat, before physics
    }

    [Fact]
    public async Task SavingUnchangedText_DoesNotEmbedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var afterFirstEmbedding = model.Texts;

        // The object read back carries its vectors.
        var loaded = await db.GetAsync<VectorNote>("1", ct);
        await db.SaveAsync(loaded!, ct);
        // A new object with the same text does not: its row is read for them.
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);
        // Nor does one that came from a search.
        var found = db.Search<VectorNote>(n => n.Category == "animals").Single();
        await db.SaveAsync(found, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(afterFirstEmbedding, model.Texts);
        Assert.Equal("1", db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).Single().Id);
    }

    [Fact]
    public async Task SavingTheSameObjectAgain_AfterTheQueueEmbeddedIt_DoesNotEmbedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        // The save works out that the object has no vector yet; the embed rule then embeds its own copy.
        var note = new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" };
        await db.SaveAsync(note, ct);
        await db.DrainAsync(cancellationToken: ct);
        var afterFirstEmbedding = model.Texts;

        // The object in hand still knows only that it had no vector: the row is read for the one
        // stored since. (SaveAsync would refuse it: the embed rule's write changed the ETag.)
        await db.SaveManyAsync([note], ct);
        var queued = await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(afterFirstEmbedding, model.Texts);
        Assert.Equal(0, queued);
        Assert.Equal("1", db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).Single().Id);
    }

    [Fact]
    public async Task ChangingText_EmbedsOnlyTheFieldsWhoseTextChanged()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var afterFirstEmbedding = model.Texts;

        // Category is not a vector field, but it is part of _content; Title is unchanged.
        await db.ChangeAsync<VectorNote>("1", n => { n.Category = "pets"; }, ct);
        await db.DrainAsync(cancellationToken: ct);

        Assert.Equal(afterFirstEmbedding + 1, model.Texts);
        // The unchanged Title kept its vector through the change.
        Assert.Equal("1", db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).Single().Id);
    }

    [Fact]
    public async Task TextChangedBeforeItsEmbeddingRan_IsEmbeddedAsItIsNow()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "quantum physics", Category = "science" }, ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "a small kitten", Category = "animals" }, ct);

        await db.DrainAsync(cancellationToken: ct);

        // Only the text the object has now: the run of the first save finds the newer version.
        Assert.Equal(2, model.Texts);
        Assert.Equal("1", db.Search<VectorNote>(n => n.Title.Similar("a cute cat")).Single().Id);
    }

    [Fact]
    public async Task ADerivedType_IsEmbeddedByItsBaseTypesRule_Once()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);

        // BaseEntity, Person and Employee are all stored, and each has a _content vector.
        var rules = (await db.Rules.StatusAsync(ct)).Rules.Select(r => r.Rule).ToList();
        Assert.Contains(rules, r => r.EndsWith("BaseEntity/embed"));
        Assert.DoesNotContain(rules, r => r.EndsWith("Person/embed") || r.EndsWith("Employee/embed"));

        await db.SaveAsync(new Employee { Id = "e1", Name = "Ada", Email = "ada@example.com", Department = "kittens" }, ct);
        Assert.Equal(1, await db.DrainAsync(cancellationToken: ct));

        // Its own fields' text: the base type's rule embedded Employee's _content, Department included.
        Assert.Equal(1, model.Texts);
        Assert.Equal("e1", db.Search<Employee>(e => e.Similar("kittens")).Single().Id);
    }

    [Fact]
    public async Task Rebuild_CopiesStoredVectors_WithoutCallingTheModel()
    {
        var ct = TestContext.Current.CancellationToken;
        var model = CountEmbeddings();
        var db = await CreateVectorDbAsync(ct);
        await db.SaveAsync(new VectorNote { Id = "1", Title = "the quick brown fox jumps over the lazy dog", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "2", Title = "a small kitten sleeping on a warm blanket", Category = "animals" }, ct);
        await db.SaveAsync(new VectorNote { Id = "3", Title = "quantum physics and string theory research", Category = "science" }, ct);
        await db.DrainAsync(cancellationToken: ct);
        var beforeRebuild = model.Texts;

        await db.RebuildSearchIndex(ct);

        Assert.Equal(beforeRebuild, model.Texts);
        var results = db.Search<VectorNote>(n => n.Title.Similar("a cute cat napping")).ToList();
        Assert.Equal("2", results[0].Id);
    }
}

