namespace Lotta.Tests;

public class StoreRegistrationTests
{
    [Fact]
    public async Task Store_WithAttributes_ExtractsKey()
    {
        var db = LottaDBFixture.CreateDb();
        var actor = new Actor { Username = "alice", DisplayName = "Alice" };
        var result = await db.SaveAsync(actor);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Store_WithAttributes_CanGetByKey()
    {
        var db = LottaDBFixture.CreateDb();
        var actor = new Actor { Username = "alice", DisplayName = "Alice" };
        await db.SaveAsync(actor);
        var loaded = await db.GetAsync<Actor>("alice");
        Assert.NotNull(loaded);
        Assert.Equal("alice", loaded.Username);
    }

    [Fact]
    public async Task Store_WithAttributes_ExtractsTags()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var aliceOnly = db.Query<Actor>()
            .Where(a => a.DisplayName == "Alice")
            .ToList();
        Assert.Single(aliceOnly);
        Assert.Equal("alice", aliceOnly[0].Username);
    }

    [Fact]
    public async Task Store_Fluent_SetKey_Works()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => a.Username);
            });
        });

        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });
        var loaded = await db.GetAsync<Actor>("bob");
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task Store_Fluent_AddQueryable_Works()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => a.Username);
                s.AddQueryable(a => a.DisplayName);
            });
        });

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var results = db.Query<Actor>()
            .Where(a => a.DisplayName == "Alice")
            .ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task Store_DefaultTableName_WorksForMultipleTypes()
    {
        var db = LottaDBFixture.CreateDb();
        await db.SaveAsync(new Actor { Username = "alice" });
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Published = DateTimeOffset.UtcNow });

        var actor = await db.GetAsync<Actor>("alice");
        Assert.NotNull(actor);

        var notes = db.Query<Note>().ToList();
        Assert.Single(notes);
    }

    [Fact]
    public void Store_UnregisteredType_Throws()
    {
        // Create a DB without registering Actor
        var tableClient = LottaDBFixture.CreateInMemoryTableServiceClient();
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());
        var options = new LottaConfiguration();
        // deliberately NOT registering Actor
        var db = new LottaDB("test", tableClient, directory, options);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveAsync(new Actor { Username = "alice" }));
    }

    [Fact]
    public async Task Store_AutoKey_GeneratesUlid()
    {
        var db = LottaDBFixture.CreateDb();

        // LogEntry has [Key(Mode = KeyMode.Auto)] — Id is generated on save
        var entry = new LogEntry { Message = "auto key test", Timestamp = DateTimeOffset.UtcNow };
        Assert.Equal("", entry.Id);

        await db.SaveAsync(entry);

        // Id should now be populated with a ULID
        Assert.NotEmpty(entry.Id);

        // Should be retrievable by the generated key
        var loaded = await db.GetAsync<LogEntry>(entry.Id);
        Assert.NotNull(loaded);
        Assert.Equal("auto key test", loaded.Message);
    }

    [Fact]
    public async Task Store_AutoKey_MultipleObjects_UniqueKeys()
    {
        var db = LottaDBFixture.CreateDb();

        var entry1 = new LogEntry { Message = "first" };
        var entry2 = new LogEntry { Message = "second" };
        await db.SaveAsync(entry1);
        await db.SaveAsync(entry2);

        Assert.NotEqual(entry1.Id, entry2.Id);

        var all = db.Query<LogEntry>().ToList();
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task Store_AutoKey_ExistingValue_NotOverwritten()
    {
        var db = LottaDBFixture.CreateDb();

        // If Id is already set, Auto mode should use it (upsert)
        var entry = new LogEntry { Id = "my-custom-id", Message = "explicit" };
        await db.SaveAsync(entry);
        Assert.Equal("my-custom-id", entry.Id);

        var loaded = await db.GetAsync<LogEntry>("my-custom-id");
        Assert.NotNull(loaded);
        Assert.Equal("explicit", loaded.Message);
    }

    [Fact]
    public async Task Store_Fluent_CustomKey()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => $"{a.Domain}/{a.Username}");
            });
        });

        await db.SaveAsync(new Actor { Domain = "test.com", Username = "alice", DisplayName = "Alice" });

        var loaded = await db.GetAsync<Actor>("test.com/alice");
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.DisplayName);
    }

    [Fact]
    public async Task Store_MixedAttributeAndFluent_FluentWins()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => $"custom-{a.Username}");
            });
        });

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

        // Fluent key should win
        var loaded = await db.GetAsync<Actor>("custom-alice");
        Assert.NotNull(loaded);

        // Attribute-derived key should NOT work
        var notFound = await db.GetAsync<Actor>("alice");
        Assert.Null(notFound);
    }
}
