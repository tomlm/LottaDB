namespace Lotta.Tests;

public class StoreRegistrationTests
{
    [Fact]
    public async Task Store_WithAttributes_ExtractsKey()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var actor = new Actor { Username = "alice", DisplayName = "Alice" };
        var result = await db.SaveAsync(actor, ct);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Store_WithAttributes_CanGetByKey()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var actor = new Actor { Username = "alice", DisplayName = "Alice" };
        await db.SaveAsync(actor, ct);
        var loaded = await db.GetAsync<Actor>("alice", ct);
        Assert.NotNull(loaded);
        Assert.Equal("alice", loaded.Username);
    }

    [Fact]
    public async Task Store_WithAttributes_ExtractsTags()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        var aliceOnly = await db.GetManyAsync<Actor>(cancellationToken: ct)
            .Where(a => a.DisplayName == "Alice")
            .ToListAsync(ct);
        Assert.Single(aliceOnly);
        Assert.Equal("alice", aliceOnly[0].Username);
    }

    [Fact]
    public async Task Store_Fluent_SetKey_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => a.Username);
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);
        var loaded = await db.GetAsync<Actor>("bob", ct);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task Store_Fluent_AddQueryable_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => a.Username);
                s.AddQueryable(a => a.DisplayName);
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        var results = await db.GetManyAsync<Actor>(cancellationToken: ct)
            .Where(a => a.DisplayName == "Alice")
            .ToListAsync(ct);
        Assert.Single(results);
    }

    [Fact]
    public async Task Store_DefaultTableName_WorksForMultipleTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Published = DateTimeOffset.UtcNow }, ct);

        var actor = await db.GetAsync<Actor>("alice", ct);
        Assert.NotNull(actor);

        var notes = await db.GetManyAsync<Note>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(notes);
    }

    [Fact]
    public async Task Store_UnregisteredType_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        // Create a DB without registering Actor
        // deliberately NOT registering Actor
        var catalog = new LottaCatalog("StoreUnregisteredTypeThrows");
        catalog.ConfigureTestStorage();
        using var db = await catalog.GetDatabaseAsync(cancellationToken: ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveAsync(new Actor { Username = "alice" }, ct));
    }

    [Fact]
    public async Task Store_UnregisteredType_BulkSave_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        var catalog = new LottaCatalog("UnregisteredBulkThrows");
        catalog.ConfigureTestStorage();
        using var db = await catalog.GetDatabaseAsync(cancellationToken: ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveManyAsync(new[] { new Actor { Username = "alice" } }, ct));
    }

    [Fact]
    public async Task Store_AutoKey_GeneratesUlid()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // LogEntry has [Key(Mode = KeyMode.Auto)] — Id is generated on save
        var entry = new LogEntry { Message = "auto key test", Timestamp = DateTimeOffset.UtcNow };
        Assert.Equal("", entry.Id);

        await db.SaveAsync(entry, ct);

        // Id should now be populated with a ULID
        Assert.NotEmpty(entry.Id);

        // Should be retrievable by the generated key
        var loaded = await db.GetAsync<LogEntry>(entry.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal("auto key test", loaded.Message);
    }

    [Fact]
    public async Task Store_AutoKey_MultipleObjects_UniqueKeys()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var entry1 = new LogEntry { Message = "first" };
        var entry2 = new LogEntry { Message = "second" };
        await db.SaveAsync(entry1, ct);
        await db.SaveAsync(entry2, ct);

        Assert.NotEqual(entry1.Id, entry2.Id);

        var all = await db.GetManyAsync<LogEntry>(cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task Store_AutoKey_ExistingValue_NotOverwritten()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        // If Id is already set, Auto mode should use it (upsert)
        var entry = new LogEntry { Id = "my-custom-id", Message = "explicit" };
        await db.SaveAsync(entry, ct);
        Assert.Equal("my-custom-id", entry.Id);

        var loaded = await db.GetAsync<LogEntry>("my-custom-id", ct);
        Assert.NotNull(loaded);
        Assert.Equal("explicit", loaded.Message);
    }

    [Fact]
    public async Task Store_Fluent_CustomKey()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => $"{a.Domain}-{a.Username}");
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Domain = "test.com", Username = "alice", DisplayName = "Alice" }, ct);

        var loaded = await db.GetAsync<Actor>("test.com-alice", ct);
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.DisplayName);
    }

    // =====================================================================
    // Int key
    // =====================================================================

    [Fact]
    public async Task Store_IntKey_SaveAndGet()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Product { ProductId = 42, Name = "Widget", Price = 9.99m }, ct);

        var loaded = await db.GetAsync<Product>("42", ct);
        Assert.NotNull(loaded);
        Assert.Equal(42, loaded.ProductId);
        Assert.Equal("Widget", loaded.Name);
        Assert.Equal(9.99m, loaded.Price);
    }

    [Fact]
    public async Task Store_IntKey_Upsert()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Product { ProductId = 1, Name = "Before", Price = 5m }, ct);
        await db.SaveAsync(new Product { ProductId = 1, Name = "After", Price = 10m }, ct);

        var all = await db.GetManyAsync<Product>(cancellationToken: ct).ToListAsync(ct);
        Assert.Single(all);
        Assert.Equal("After", all[0].Name);
    }

    [Fact]
    public async Task Store_IntKey_Delete()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Product { ProductId = 99, Name = "Gone" }, ct);
        Assert.NotNull(await db.GetAsync<Product>("99", ct));

        await db.DeleteAsync<Product>("99", ct);
        Assert.Null(await db.GetAsync<Product>("99", ct));
    }

    [Fact]
    public async Task Store_IntKey_Search()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Product { ProductId = 1, Name = "Lucene Widget" }, ct);
        await db.SaveAsync(new Product { ProductId = 2, Name = "Azure Gadget" }, ct);

        var results = db.Search<Product>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal(1, results[0].ProductId);
    }

    [Fact]
    public async Task Store_IntKey_MultipleObjects()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 1; i <= 5; i++)
            await db.SaveAsync(new Product { ProductId = i, Name = $"Product {i}" }, ct);

        var all = await db.GetManyAsync<Product>(cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(5, all.Count);
    }

    // =====================================================================

    [Fact]
    public async Task Store_MixedAttributeAndFluent_FluentWins()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => $"custom-{a.Username}");
            });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        // Fluent key should win
        var loaded = await db.GetAsync<Actor>("custom-alice", ct);
        Assert.NotNull(loaded);

        // Attribute-derived key should NOT work
        var notFound = await db.GetAsync<Actor>("alice", ct);
        Assert.Null(notFound);
    }
}
