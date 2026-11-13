using Lucene.Net.Linq.Mapping;

namespace Lotta.Tests;

/// <summary>The "one" side of the join tests: gadgets point at a shop by its key.</summary>
public class Shop
{
    [Key]
    public string ShopId { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string Country { get; set; } = "";

    [Queryable]
    public string Name { get; set; } = "";
}

/// <summary>Carries one property of each kind that can be counted with <c>.Facets(...)</c>.</summary>
public class Gadget
{
    [Key]
    public string GadgetId { get; set; } = "";

    /// <summary>Foreign key to <see cref="Shop.ShopId"/>, indexed as one exact term.</summary>
    [Queryable(QueryableMode.NotAnalyzed)]
    public string SoldBy { get; set; } = "";

    /// <summary>The same foreign key, analyzed (the default for strings).</summary>
    [Queryable]
    public string SoldByText { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed, Facet = true)]
    public string Category { get; set; } = "";

    /// <summary>Analyzed for search, counted as written.</summary>
    [Queryable(Facet = true)]
    public string Brand { get; set; } = "";

    [Queryable(Facet = true)]
    public List<string> Tags { get; set; } = new();

    [Queryable(Facet = true)]
    public int Stars { get; set; }

    [Queryable(Facet = true)]
    public double Price { get; set; }

    [Queryable(Facet = true)]
    public bool InStock { get; set; }

    [Queryable(Facet = true)]
    public DateTime Released { get; set; }

    /// <summary>Declared with Lucene.Net.Linq's own attribute.</summary>
    [Field(IndexMode.NotAnalyzed, Facet = true)]
    public string Colour { get; set; } = "";

    /// <summary>Facet declared with the fluent API (AddQueryable) in the tests.</summary>
    public string Material { get; set; } = "";

    /// <summary>Facet declared with the fluent API (AddField) in the tests.</summary>
    public string Finish { get; set; } = "";

    [Queryable]
    public int Rank { get; set; }
}

public class Vehicle
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed, Facet = true)]
    public string Maker { get; set; } = "";
}

public class Truck : Vehicle
{
    [Queryable]
    public int Axles { get; set; }
}

internal static class FacetJoinData
{
    public static readonly Shop[] Shops =
    [
        new Shop { ShopId = "s1", Country = "DE", Name = "Alpha" },
        new Shop { ShopId = "s2", Country = "US", Name = "Beta" },
        new Shop { ShopId = "s3", Country = "DE", Name = "Gamma" },   // sells nothing
    ];

    public static readonly Gadget[] Gadgets =
    [
        new Gadget { GadgetId = "g1", SoldBy = "s1", SoldByText = "s1", Category = "phone", Brand = "Acme Corp", Tags = ["red", "small"], Stars = 5, Price = 99.5, InStock = true, Released = Utc(2024, 1, 15), Colour = "Red", Material = "steel", Finish = "matte", Rank = 1 },
        new Gadget { GadgetId = "g2", SoldBy = "s1", SoldByText = "s1", Category = "phone", Brand = "Acme Corp", Tags = ["red"], Stars = 4, Price = 250, InStock = true, Released = Utc(2024, 3, 2), Colour = "Blue", Material = "steel", Finish = "gloss", Rank = 2 },
        new Gadget { GadgetId = "g3", SoldBy = "s2", SoldByText = "s2", Category = "laptop", Brand = "Zen", Tags = ["big", "red"], Stars = 5, Price = 1200, InStock = false, Released = Utc(2025, 6, 10), Colour = "Red", Material = "wood", Finish = "matte", Rank = 3 },
        new Gadget { GadgetId = "g4", SoldBy = "s2", SoldByText = "s2", Category = "laptop", Brand = "acme corp", Tags = [], Stars = 3, Price = 800, InStock = true, Released = Utc(2025, 7, 1), Colour = "Green", Material = "wood", Finish = "matte", Rank = 4 },
        new Gadget { GadgetId = "g5", SoldBy = "s9", SoldByText = "s9", Category = "tablet", Brand = "Zen", Tags = ["small"], Stars = 1, Price = 20, InStock = false, Released = Utc(2023, 12, 31), Colour = "Red", Material = "glass", Finish = "gloss", Rank = 5 },   // its shop does not exist
    ];

    private static DateTime Utc(int year, int month, int day) => new(year, month, day, 12, 0, 0, DateTimeKind.Utc);

    public static void Configure(ILottaConfiguration config)
    {
        config.Store<Shop>();
        config.Store<Gadget>(s =>
        {
            s.AddQueryable(g => g.Material).NotAnalyzed().Facet();
            s.AddField(g => g.Finish).NotAnalyzed().Facet();
        });
        config.Store<Vehicle>();
        config.Store<Truck>();
    }
}
