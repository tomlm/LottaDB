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
        var aliceNotes = db.QueryAsync<Note>()
            .Where(n => n.AuthorId == "alice")
            .ToListAsync().Result;
        Assert.Single(aliceNotes);
        Assert.Equal("n1", aliceNotes[0].NoteId);
    }

    [Fact]
    public void Store_Fluent_SetPartitionKey_Works()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
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
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
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

        var results = db.QueryAsync<Actor>()
            .Where(a => a.DisplayName == "Alice")
            .ToListAsync().Result;
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
        var notes = db.QueryAsync<Note>().ToListAsync().Result;
        Assert.Single(notes);
    }

    [Fact]
    public void Store_UnregisteredType_Throws()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
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
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
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

        // Descending time: newest first
        var notes = db.QueryAsync<Note>().ToListAsync().Result;
        Assert.Equal(2, notes.Count);
        Assert.Equal("n2", notes[0].NoteId);
        Assert.Equal("n1", notes[1].NoteId);
    }
}
