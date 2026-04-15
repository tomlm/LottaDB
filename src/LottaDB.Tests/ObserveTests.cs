namespace Lotta.Tests;

public class ObserveTests
{
    [Fact]
    public async Task On_ReceivesSavedNotification()
    {
        Actor? received = null;
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.On<Actor>(async (actor, kind, db) => { received = actor; });
        });

        await db.SaveAsync(new Actor { Username = "saved", DisplayName = "Saved" }, TestContext.Current.CancellationToken);
        Assert.NotNull(received);
        Assert.Equal("Saved", received.DisplayName);
    }

    [Fact]
    public async Task On_ReceivesDeletedNotification()
    {
        TriggerKind? receivedKind = null;
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.On<Actor>(async (actor, kind, db) => { receivedKind = kind; });
        });

        var actor = new Actor { Username = "deleted", DisplayName = "Gone" };
        await db.SaveAsync(actor, TestContext.Current.CancellationToken);
        await db.DeleteAsync(actor, TestContext.Current.CancellationToken);
        Assert.Equal(TriggerKind.Deleted, receivedKind);
    }

    [Fact]
    public async Task On_RuntimeRegistration_Works()
    {
        var db = LottaDBFixture.CreateDb();
        Actor? received = null;

        using var handle = db.On<Actor>(async (actor, kind, db) => { received = actor; });
        await db.SaveAsync(new Actor { Username = "runtime", DisplayName = "Runtime" }, TestContext.Current.CancellationToken);

        Assert.NotNull(received);
    }

    [Fact]
    public async Task On_Dispose_StopsNotifications()
    {
        var db = LottaDBFixture.CreateDb();
        int count = 0;

        var handle = db.On<Actor>(async (actor, kind, db) => { Interlocked.Increment(ref count); });
        await db.SaveAsync(new Actor { Username = "before" }, TestContext.Current.CancellationToken);
        Assert.Equal(1, count);

        handle.Dispose();
        await db.SaveAsync(new Actor { Username = "after" }, TestContext.Current.CancellationToken);
        Assert.Equal(1, count); // Should not have incremented
    }

    [Fact]
    public async Task On_MultipleHandlers_AllFired()
    {
        int count = 0;
        var db = LottaDBFixture.CreateDb(opts =>
        {
            opts.On<Actor>(async (a, k, d) => { Interlocked.Increment(ref count); });
            opts.On<Actor>(async (a, k, d) => { Interlocked.Increment(ref count); });
        });

        await db.SaveAsync(new Actor { Username = "multi" }, TestContext.Current.CancellationToken);
        Assert.Equal(2, count);
    }
}
