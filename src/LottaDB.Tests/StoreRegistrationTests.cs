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
    public async Task Store_Fluent_AddTag_Works()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.Store<Actor>(s =>
            {
                s.SetKey(a => a.Username);
                s.AddTag(a => a.DisplayName);
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
    public async Task Store_Fluent_SetKey_WithDescendingTime()
    {
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.Store<Note>(s =>
            {
                s.SetKey(KeyStrategy.DescendingTime);
                s.AddTag(n => n.AuthorId);
            });
        });

        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "first", Published = DateTimeOffset.UtcNow.AddHours(-1) });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "alice", Content = "second", Published = DateTimeOffset.UtcNow });

        var notes = db.Query<Note>().ToList();
        Assert.Equal(2, notes.Count);
        Assert.Contains(notes, n => n.NoteId == "n1");
        Assert.Contains(notes, n => n.NoteId == "n2");
    }

    [Fact]
    public async Task Store_Fluent_SetRowKey_WithAscendingTime()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new LogEntry { LogId = "L1", Message = "first", Timestamp = DateTimeOffset.UtcNow.AddHours(-1) });
        await db.SaveAsync(new LogEntry { LogId = "L2", Message = "second", Timestamp = DateTimeOffset.UtcNow });

        var logs = db.Query<LogEntry>().ToList();
        Assert.Equal(2, logs.Count);
        Assert.Contains(logs, l => l.LogId == "L1");
        Assert.Contains(logs, l => l.LogId == "L2");
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
