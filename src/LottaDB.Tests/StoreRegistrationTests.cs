namespace LottaDB.Tests;

public class StoreRegistrationTests
{
    [Fact]
    public void Store_WithAttributes_ExtractsPartitionKey()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        // Actor has [PartitionKey] on Domain — saving should work without explicit keys
        var actor = new Actor { Domain = "example.com", Username = "alice", DisplayName = "Alice" };
        // If PK extraction fails, SaveAsync will throw
        var result = db.SaveAsync(actor).Result;
        Assert.NotNull(result);
    }

    [Fact]
    public void Store_WithAttributes_ExtractsRowKey()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        var actor = new Actor { Domain = "example.com", Username = "alice", DisplayName = "Alice" };
        db.SaveAsync(actor).Wait();
        // If RK extraction works, we can get by PK + RK
        var loaded = db.GetAsync<Actor>("example.com", "alice").Result;
        Assert.NotNull(loaded);
        Assert.Equal("alice", loaded.Username);
    }

    [Fact]
    public void Store_WithAttributes_ExtractsTags()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        var note1 = new Note { Domain = "example.com", NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow };
        var note2 = new Note { Domain = "example.com", NoteId = "n2", AuthorId = "bob", Content = "World", Published = DateTimeOffset.UtcNow };
        db.SaveAsync(note1).Wait();
        db.SaveAsync(note2).Wait();

        // Tag on AuthorId should allow server-side filtering via QueryAsync
        var aliceNotes = db.Query<Note>()
            .Where(n => n.AuthorId == "alice")
            .ToList();
        Assert.Single(aliceNotes);
        Assert.Equal("n1", aliceNotes[0].NoteId);
    }

    [Fact]
    public void Store_Fluent_SetPartitionKey_Works()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Actor>(s =>
            {
                s.SetPartitionKey(a => a.Domain);
                s.SetRowKey(a => a.Username);
            });
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        var actor = new Actor { Domain = "test.com", Username = "bob" };
        db.SaveAsync(actor).Wait();
        var loaded = db.GetAsync<Actor>("test.com", "bob").Result;
        Assert.NotNull(loaded);
    }

    [Fact]
    public void Store_Fluent_AddTag_Works()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Actor>(s =>
            {
                s.SetPartitionKey(a => a.Domain);
                s.SetRowKey(a => a.Username);
                s.AddTag(a => a.DisplayName);
            });
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        db.SaveAsync(new Actor { Domain = "test.com", Username = "alice", DisplayName = "Alice" }).Wait();
        db.SaveAsync(new Actor { Domain = "test.com", Username = "bob", DisplayName = "Bob" }).Wait();

        var results = db.Query<Actor>()
            .Where(a => a.DisplayName == "Alice")
            .ToList();
        Assert.Single(results);
    }

    [Fact]
    public void Store_DefaultTableName_IsLowercasedType()
    {
        // This is an internal detail — we verify it works by successfully saving
        // types without explicit SetTableName. If the table name were wrong or colliding,
        // saving different types would fail or overwrite each other.
        var db = TestLottaDBFactory.CreateWithBuilders();

        db.SaveAsync(new Actor { Domain = "a.com", Username = "alice" }).Wait();
        db.SaveAsync(new Note { Domain = "a.com", NoteId = "n1", AuthorId = "alice", Published = DateTimeOffset.UtcNow }).Wait();

        // Both should be independently retrievable
        var actor = db.GetAsync<Actor>("a.com", "alice").Result;
        Assert.NotNull(actor);

        // Note uses descending time RowKey, so we query instead of get
        var notes = db.Query<Note>().ToList();
        Assert.Single(notes);
    }

    [Fact]
    public void Store_UnregisteredType_Throws()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            // Deliberately NOT registering Actor
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.SaveAsync(new Actor { Domain = "a.com", Username = "alice" }));
    }

    [Fact]
    public void Store_Fluent_SetRowKey_WithDescendingTime()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Note>(s =>
            {
                s.SetPartitionKey(n => n.Domain);
                s.SetRowKey(RowKeyStrategy.DescendingTime);
                s.AddTag(n => n.AuthorId);
            });
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        var older = new Note { Domain = "a.com", NoteId = "n1", AuthorId = "alice", Content = "first", Published = DateTimeOffset.UtcNow.AddHours(-1) };
        var newer = new Note { Domain = "a.com", NoteId = "n2", AuthorId = "alice", Content = "second", Published = DateTimeOffset.UtcNow };
        db.SaveAsync(older).Wait();
        db.SaveAsync(newer).Wait();

        // Both notes should be stored
        var notes = db.Query<Note>().ToList();
        Assert.Equal(2, notes.Count);
        // Both notes should be retrievable
        Assert.Contains(notes, n => n.NoteId == "n1");
        Assert.Contains(notes, n => n.NoteId == "n2");
    }

    [Fact]
    public async Task Store_Fluent_SetRowKey_WithAscendingTime()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();

        var older = new LogEntry { Source = "app", LogId = "L1", Message = "first", Timestamp = DateTimeOffset.UtcNow.AddHours(-1) };
        var newer = new LogEntry { Source = "app", LogId = "L2", Message = "second", Timestamp = DateTimeOffset.UtcNow };
        await db.SaveAsync(older);
        await db.SaveAsync(newer);

        // Ascending time: oldest first
        var logs = db.Query<LogEntry>().ToList();
        Assert.Equal(2, logs.Count);
        Assert.Equal("L1", logs[0].LogId);
        Assert.Equal("L2", logs[1].LogId);
    }

    [Fact]
    public async Task Store_Fluent_CustomRowKey()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Actor>(s =>
            {
                s.SetPartitionKey(a => a.Domain);
                s.SetRowKey(a => $"{a.Username}-custom");
            });
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        await db.SaveAsync(new Actor { Domain = "custom.test", Username = "alice", DisplayName = "Alice" });

        // The custom row key should be "alice-custom"
        var loaded = await db.GetAsync<Actor>("custom.test", "alice-custom");
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.DisplayName);
    }

    [Fact]
    public async Task Store_MixedAttributeAndFluent_FluentWins()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            // Actor has [PartitionKey] on Domain, but fluent overrides it
            opts.Store<Actor>(s =>
            {
                s.SetPartitionKey(a => $"override:{a.Domain}");
                s.SetRowKey(a => a.Username);
            });
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        await db.SaveAsync(new Actor { Domain = "mixed.test", Username = "alice", DisplayName = "Alice" });

        // Fluent partition key should win: "override:mixed.test"
        var loaded = await db.GetAsync<Actor>("override:mixed.test", "alice");
        Assert.NotNull(loaded);
        Assert.Equal("Alice", loaded.DisplayName);

        // Original attribute-derived key should NOT work
        var notFound = await db.GetAsync<Actor>("mixed.test", "alice");
        Assert.Null(notFound);
    }
}
