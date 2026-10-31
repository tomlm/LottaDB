namespace Lotta.Tests;

/// <summary>Document with multi-valued string fields of every supported collection shape.</summary>
public class LinkedDoc
{
    [Key]
    public string Id { get; set; } = "";

    /// <summary>Entity ids: matched verbatim, case-sensitive.</summary>
    [Queryable(QueryableMode.NotAnalyzed)]
    public string[] Links { get; set; } = [];

    [Queryable(QueryableMode.NotAnalyzed)]
    public List<string> Tags { get; set; } = [];

    [Queryable]
    public IEnumerable<string> Aliases { get; set; } = [];
}

public abstract class MultiValuedFieldTestBase : LottaTestBase
{
    protected MultiValuedFieldTestBase(Action<LottaCatalog> config) : base(config) { }

    private async Task<LottaDB> SeedAsync(CancellationToken ct, [System.Runtime.CompilerServices.CallerMemberName] string? testName = null)
    {
        var db = await CreateDbAsync(c => c.Store<LinkedDoc>(), cancellationToken: ct, testName: testName);
        await db.SaveManyAsync(
        [
            new LinkedDoc { Id = "1", Links = ["01JA2XYZ", "01JC9ABC"], Tags = ["family", "Iowa Property"], Aliases = ["Leslie", "my wife"] },
            new LinkedDoc { Id = "2", Links = ["01JC9ABC"], Tags = ["work"], Aliases = ["Les"] },
            new LinkedDoc { Id = "3", Links = [], Tags = [], Aliases = [] },
        ], ct);
        return db;
    }

    private static string[] Ids(IEnumerable<LinkedDoc> docs) => docs.Select(d => d.Id).Order().ToArray();

    [Fact]
    public async Task StringArray_Contains_MatchesElement()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.Equal(["1", "2"], Ids(db.Search<LinkedDoc>(d => d.Links.Contains("01JC9ABC"))));
        Assert.Equal(["1"], Ids(db.Search<LinkedDoc>(d => d.Links.Contains("01JA2XYZ"))));
        Assert.Empty(db.Search<LinkedDoc>(d => d.Links.Contains("01JD4NOPE")));
    }

    [Fact]
    public async Task StringArray_NotAnalyzed_IsExact()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        // Not analyzed: no lowercasing, no tokenizing.
        Assert.Empty(db.Search<LinkedDoc>(d => d.Links.Contains("01jc9abc")));
    }

    [Fact]
    public async Task StringList_Contains_MatchesWholeValue()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.Equal(["1"], Ids(db.Search<LinkedDoc>(d => d.Tags.Contains("Iowa Property"))));
        Assert.Equal(["2"], Ids(db.Search<LinkedDoc>(d => d.Tags.Contains("work"))));
        Assert.Empty(db.Search<LinkedDoc>(d => d.Tags.Contains("Iowa")));
    }

    [Fact]
    public async Task StringEnumerable_Analyzed_MatchesToken()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.Equal(["1"], Ids(db.Search<LinkedDoc>(d => d.Aliases.Contains("wife"))));
        Assert.Equal(["1"], Ids(db.Search<LinkedDoc>("Aliases:lesley~")));
    }

    [Fact]
    public async Task MultiValued_CombinesWithOtherFilters()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        Assert.Equal(["2"], Ids(db.Search<LinkedDoc>(d => d.Links.Contains("01JC9ABC") && d.Tags.Contains("work"))));
    }

    [Fact]
    public async Task In_ArrayOfValuesContainsKey()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);
        var ids = new[] { "1", "3" };

        Assert.Equal(["1", "3"], Ids(db.Search<LinkedDoc>(d => ids.Contains(d.Id))));
    }

    [Fact]
    public async Task MultiValued_RoundTripsCollectionTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        var db = await SeedAsync(ct);

        var doc = Assert.Single(db.Search<LinkedDoc>(d => d.Id == "1"));
        Assert.Equal(["01JA2XYZ", "01JC9ABC"], doc.Links);
        Assert.Equal(["family", "Iowa Property"], doc.Tags);
        Assert.Equal(["Leslie", "my wife"], doc.Aliases);
    }
}
