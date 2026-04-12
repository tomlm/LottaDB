namespace LottaDB.Tests;

public class ObserveTests
{
    [Fact]
    public async Task Observe_ReceivesSavedNotification()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        ObjectChange<Actor>? received = null;
        using var sub = db.Observe<Actor>(change =>
        {
            received = change;
            return Task.CompletedTask;
        });

        await db.SaveAsync(new Actor { Domain = "obs.test", Username = "saved", DisplayName = "Saved" });

        Assert.NotNull(received);
        Assert.Equal(ChangeKind.Saved, received.Kind);
        Assert.Equal("Saved", received.Object?.DisplayName);
    }

    [Fact]
    public async Task Observe_ReceivesDeletedNotification()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        ObjectChange<Actor>? received = null;
        var actor = new Actor { Domain = "obs.test", Username = "deleted", DisplayName = "Gone" };
        await db.SaveAsync(actor);

        using var sub = db.Observe<Actor>(change =>
        {
            if (change.Kind == ChangeKind.Deleted)
                received = change;
            return Task.CompletedTask;
        });

        await db.DeleteAsync(actor);

        Assert.NotNull(received);
        Assert.Equal(ChangeKind.Deleted, received.Kind);
    }

    [Fact]
    public async Task Observe_ReceivesDerivedObjectChanges()
    {
        var db = TestLottaDBFactory.CreateWithBuilders(opts =>
            opts.AddBuilder<Note, NoteView, NoteViewExplicitBuilder>());

        ObjectChange<NoteView>? received = null;
        using var sub = db.Observe<NoteView>(change =>
        {
            received = change;
            return Task.CompletedTask;
        });

        await db.SaveAsync(new Actor { Domain = "obs.test", Username = "author", DisplayName = "Author" });
        await db.SaveAsync(new Note { Domain = "obs.test", NoteId = "obs-n", AuthorId = "author", Content = "Observable", Published = DateTimeOffset.UtcNow });

        Assert.NotNull(received);
        Assert.Equal("obs-n", received.Object?.NoteId);
    }

    [Fact]
    public async Task Observe_MultipleObservers_AllFired()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        int count = 0;

        using var sub1 = db.Observe<Actor>(_ => { Interlocked.Increment(ref count); return Task.CompletedTask; });
        using var sub2 = db.Observe<Actor>(_ => { Interlocked.Increment(ref count); return Task.CompletedTask; });

        await db.SaveAsync(new Actor { Domain = "obs.test", Username = "multi", DisplayName = "Multi" });

        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Observe_Dispose_StopsNotifications()
    {
        var db = TestLottaDBFactory.CreateWithBuilders();
        int count = 0;

        var sub = db.Observe<Actor>(_ => { Interlocked.Increment(ref count); return Task.CompletedTask; });
        await db.SaveAsync(new Actor { Domain = "obs.test", Username = "before", DisplayName = "Before" });
        Assert.Equal(1, count);

        sub.Dispose();
        await db.SaveAsync(new Actor { Domain = "obs.test", Username = "after", DisplayName = "After" });
        Assert.Equal(1, count); // Should not have incremented
    }

    [Fact]
    public async Task Observe_RegisteredInConfig_Works()
    {
        ObjectChange<Actor>? received = null;
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSingleton<Azure.Data.Tables.TableServiceClient>(LottaDBFixture.CreateInMemoryTableServiceClient());
        services.AddLottaDB(opts =>
        {
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Actor>();
            opts.Observe<Actor>(change =>
            {
                received = change;
                return Task.CompletedTask;
            });
        });
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        await db.SaveAsync(new Actor { Domain = "obs.test", Username = "config", DisplayName = "Config" });

        Assert.NotNull(received);
    }
}
