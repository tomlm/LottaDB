using System.Runtime.CompilerServices;
using Lotta.Internal;
using Lucene.Net.Linq;

namespace Lotta.Tests;

/// <summary>
/// Facet counts (<c>.Facets(...)</c> from Lucene.Net.Linq) on <c>db.Search&lt;T&gt;()</c> for
/// properties declared with <c>[Queryable(Facet = true)]</c>, <c>[Field(Facet = true)]</c> or the
/// fluent <c>.Facet()</c>.
/// </summary>
public abstract class FacetTestBase : LottaTestBase
{
    protected FacetTestBase(Action<LottaCatalog> config) : base(config) { }

    private async Task<LottaDB> SeedAsync(CancellationToken ct, [CallerMemberName] string? testName = null)
    {
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct, testName: testName);
        await db.SaveManyAsync(FacetJoinData.Gadgets, ct);
        await db.SaveManyAsync(FacetJoinData.Shops, ct);
        return db;
    }

    private static string[] Show(IEnumerable<FacetValue> values) => values.Select(v => $"{v.Label}={v.Count}").ToArray();

    [Fact]
    public async Task Facets_CountNotAnalyzedString()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var all = db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Category)).ToList();

        Assert.Equal(5, all.Count);
        // By count descending, then by value.
        Assert.Equal(["laptop=2", "phone=2", "tablet=1"], Show(facets.For(g => g.Category)));
    }

    [Fact]
    public async Task Facets_AnalyzedString_CountsTheValueAsWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Brand)).ToList();

        // The field is tokenized and lowercased for search; the facet label is not.
        Assert.Equal(["Acme Corp=2", "Zen=2", "acme corp=1"], Show(facets.For(g => g.Brand)));

        // The property is still searched as before.
        Assert.Equal(3, db.Search<Gadget>().Where(g => g.Brand == "acme").Count());
    }

    [Fact]
    public async Task Facets_CountStringCollection()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Tags)).ToList();

        Assert.Equal(["red=3", "small=2", "big=1"], Show(facets.For(g => g.Tags)));

        // The collection is still searched as before.
        Assert.Equal(3, db.Search<Gadget>().Where(g => g.Tags.Contains("red")).Count());
    }

    [Fact]
    public async Task Facets_CountNumberAndBool()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Stars).Count(g => g.InStock)).ToList();

        Assert.Equal(["5=2", "1=1", "3=1", "4=1"], Show(facets.For(g => g.Stars)));

        var inStock = facets.For(g => g.InStock).ToDictionary(v => v.Label.ToLowerInvariant(), v => v.Count);
        Assert.Equal(3, inStock["true"]);
        Assert.Equal(2, inStock["false"]);
    }

    [Fact]
    public async Task Facets_NumericRanges()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        db.Search<Gadget>().Facets(out var facets, f => f
            .Ranges(g => g.Price, [new FacetRange("cheap", null, 100.0), new FacetRange("mid", 100.0, 1000.0), new FacetRange("high", 1000.0, null)])
            .Ranges(g => g.Stars, [new FacetRange("low", null, 4), new FacetRange("top", 4, null)])).ToList();

        Assert.Equal(["cheap=2", "mid=2", "high=1"], Show(facets.For(g => g.Price)));
        Assert.Equal(["low=2", "top=3"], Show(facets.For(g => g.Stars)));
    }

    [Fact]
    public async Task Facets_DateRanges()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var ranges = FacetRange.Yearly(new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2025, 12, 31, 0, 0, 0, DateTimeKind.Utc));
        db.Search<Gadget>().Facets(out var facets, f => f.Ranges(g => g.Released, ranges)).ToList();

        // 2023, 2024, 2025
        Assert.Equal([1, 2, 2], facets.For(g => g.Released).Select(v => v.Count).ToArray());

        // Date queries are unchanged by the facet.
        Assert.Equal(2, db.Search<Gadget>().Where(g => g.Released >= new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc)).Count());
    }

    [Fact]
    public async Task Facets_CoverEveryMatch_NotJustThePage()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var page = db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Category)).OrderBy(g => g.Rank).Take(1).ToList();

        Assert.Equal("g1", Assert.Single(page).GadgetId);
        Assert.Equal(5, facets.For(g => g.Category).Sum(v => v.Count));
    }

    [Fact]
    public async Task Facets_FollowWhereAndQueryString()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        db.Search<Gadget>().Where(g => g.Category == "phone").Facets(out var byWhere, f => f.Count(g => g.Tags)).ToList();
        Assert.Equal(["red=2", "small=1"], Show(byWhere.For(g => g.Tags)));

        db.Search<Gadget>(g => g.Stars == 5).Facets(out var byPredicate, f => f.Count(g => g.Category)).ToList();
        Assert.Equal(["laptop=1", "phone=1"], Show(byPredicate.For(g => g.Category)));

        db.Search<Gadget>("Zen").Facets(out var byText, f => f.Count(g => g.Category)).ToList();
        Assert.Equal(["laptop=1", "tablet=1"], Show(byText.For(g => g.Category)));
    }

    [Fact]
    public async Task Facets_DeclaredWithFieldAttributeAndFluentApi()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Colour).Count(g => g.Material).Count(g => g.Finish)).ToList();

        Assert.Equal(["Red=3", "Blue=1", "Green=1"], Show(facets.For(g => g.Colour)));      // [Field(Facet = true)]
        Assert.Equal(["steel=2", "wood=2", "glass=1"], Show(facets.For(g => g.Material)));  // AddQueryable(...).Facet()
        Assert.Equal(["matte=3", "gloss=2"], Show(facets.For(g => g.Finish)));              // AddField(...).Facet()
    }

    [Fact]
    public async Task Facets_FollowUpdatesAndDeletes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var g1 = (await db.GetAsync<Gadget>("g1", ct))!;
        g1.Category = "tablet";
        await db.SaveAsync(g1, ct);
        await db.DeleteAsync<Gadget>("g2", ct);

        db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Category)).ToList();

        Assert.Equal(["laptop=2", "tablet=2"], Show(facets.For(g => g.Category)));
    }

    [Fact]
    public async Task Facets_OnABaseType_IncludeDerivedTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct);
        await db.SaveAsync(new Vehicle { Id = "v1", Maker = "Volvo" }, ct);
        await db.SaveAsync(new Truck { Id = "t1", Maker = "Volvo", Axles = 3 }, ct);
        await db.SaveAsync(new Truck { Id = "t2", Maker = "Scania", Axles = 2 }, ct);

        db.Search<Vehicle>().Facets(out var all, f => f.Count(v => v.Maker)).ToList();
        Assert.Equal(["Volvo=2", "Scania=1"], Show(all.For(v => v.Maker)));

        db.Search<Truck>().Facets(out var trucks, f => f.Count(v => v.Maker)).ToList();
        Assert.Equal(["Scania=1", "Volvo=1"], Show(trucks.For(v => v.Maker)));
    }

    [Fact]
    public async Task Facets_NothingStored_GivesEmptyCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await CreateDbAsync(FacetJoinData.Configure, cancellationToken: ct);

        var rows = db.Search<Gadget>().Facets(out var facets, f => f.Count(g => g.Category)).ToList();

        Assert.Empty(rows);
        Assert.Empty(facets.For(g => g.Category));
    }

    [Fact]
    public async Task Facets_PropertyWithoutFacet_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.ThrowsAny<Exception>(() => db.Search<Gadget>().Facets(out _, f => f.Count(g => g.Rank)).ToList());
    }
}

/// <summary>Declaring a facet changes the schema, which is what makes LottaDB rebuild the index.</summary>
public class FacetSchemaTests
{
    public class Plain
    {
        [Key]
        public string Id { get; set; } = "";

        // No attribute: the fluent API configures a property only when it has none.
        public string Kind { get; set; } = "";
    }

    [Fact]
    public void SchemaWithoutFacets_KeepsItsHash()
    {
        // Pinned from 5.2.0. If this changes, every existing index is rebuilt on upgrade.
        Assert.Equal("BB5839682DFE0F47E86D85B1DEB0FC51D24E6E72932905B29CF1A1AEB2827594", Hash(s => s.AddQueryable(p => p.Kind).NotAnalyzed()));
    }

    private static string Hash(Action<IStorageConfiguration<Plain>>? configure)
    {
        var config = new StorageConfiguration<Plain>();
        configure?.Invoke(config);
        return TypeMetadata.ComputeSchemaJson([TypeMetadata.Build<Plain>(config, null)]);
    }

    [Fact]
    public void DeclaringAFacet_ChangesTheSchemaHash()
    {
        // The two configurations differ only in the facet.
        var plain = Hash(s => s.AddQueryable(p => p.Kind).NotAnalyzed());
        var faceted = Hash(s => s.AddQueryable(p => p.Kind).NotAnalyzed().Facet());

        Assert.NotEqual(plain, faceted);
        Assert.Equal(faceted, Hash(s => s.AddQueryable(p => p.Kind).NotAnalyzed().Facet()));
    }

    [Fact]
    public void FacetFlag_ReachesTheMetadata()
    {
        var config = new StorageConfiguration<Gadget>();
        config.AddQueryable(g => g.Material).NotAnalyzed().Facet();
        var meta = TypeMetadata.Build<Gadget>(config, null);

        bool IsFacet(string name) => meta.IndexedProperties.Single(p => p.Property.Name == name).IsFacet;

        Assert.True(IsFacet(nameof(Gadget.Category)));   // [Queryable(Facet = true)]
        Assert.True(IsFacet(nameof(Gadget.Colour)));     // [Field(Facet = true)]
        Assert.True(IsFacet(nameof(Gadget.Material)));   // fluent
        Assert.False(IsFacet(nameof(Gadget.Rank)));
    }
}
