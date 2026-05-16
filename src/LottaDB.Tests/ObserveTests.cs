namespace Lotta.Tests;

public class ObserveTests
{
    [Fact]
    public async Task On_ReceivesSavedNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        Actor? received = null;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Actor>(async (actor, kind, db, _) => { received = actor; });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "saved", DisplayName = "Saved" }, ct);
        Assert.NotNull(received);
        Assert.Equal("Saved", received.DisplayName);
    }

    [Fact]
    public async Task On_ReceivesDeletedNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        TriggerKind? receivedKind = null;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Actor>(async (actor, kind, db, _) => { receivedKind = kind; });
        }, cancellationToken: ct);

        var actor = new Actor { Username = "deleted", DisplayName = "Gone" };
        await db.SaveAsync(actor, ct);
        await db.DeleteAsync(actor, ct);
        Assert.Equal(TriggerKind.Deleted, receivedKind);
    }

    [Fact]
    public async Task On_RuntimeRegistration_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        Actor? received = null;

        using var handle = db.On<Actor>(async (actor, kind, db, _) => { received = actor; });
        await db.SaveAsync(new Actor { Username = "runtime", DisplayName = "Runtime" }, ct);

        Assert.NotNull(received);
    }

    [Fact]
    public async Task On_Dispose_StopsNotifications()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        int count = 0;

        var handle = db.On<Actor>(async (actor, kind, db, _) => { Interlocked.Increment(ref count); });
        await db.SaveAsync(new Actor { Username = "before" }, ct);
        Assert.Equal(1, count);

        handle.Dispose();
        await db.SaveAsync(new Actor { Username = "after" }, ct);
        Assert.Equal(1, count); // Should not have incremented
    }

    [Fact]
    public async Task On_MultipleHandlers_AllFired()
    {
        var ct = TestContext.Current.CancellationToken;
        int count = 0;
        using var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Actor>(async (a, k, d, _) => { Interlocked.Increment(ref count); });
            opts.On<Actor>(async (a, k, d, _) => { Interlocked.Increment(ref count); });
        }, cancellationToken: ct);

        await db.SaveAsync(new Actor { Username = "multi" }, ct);
        Assert.Equal(2, count);
    }
}
