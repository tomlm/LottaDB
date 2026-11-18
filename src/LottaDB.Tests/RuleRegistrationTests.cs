using Lotta.Internal;

namespace Lotta.Tests;

/// <summary>
/// Registering rules: checks made when a rule is built, before any database is opened, so they
/// need no storage provider.
/// </summary>
public class RuleRegistrationTests
{
    private static Task Handle(Actor actor, LottaDB db, CancellationToken cancellationToken) => Task.CompletedTask;
    private static Task HandleAsync(Actor actor, LottaDB db, CancellationToken cancellationToken) => Task.CompletedTask;
    private static Task Generic<T>(T item, LottaDB db, CancellationToken cancellationToken) => Task.CompletedTask;

    // === Default queue names ===

    [Theory]
    [InlineData("Summarize", "summarize")]
    [InlineData("NotifyFollowers", "notify-followers")]
    [InlineData("NotifyFollowersAsync", "notify-followers")]
    [InlineData("handlers.NotifyFollowersAsync", "notify-followers")]
    [InlineData("this.handlers.Describe", "describe")]
    [InlineData("notifyFollowers", "notify-followers")]
    [InlineData("notify_followers", "notify-followers")]
    [InlineData("HTMLParser", "html-parser")]
    [InlineData("Summarize2Text", "summarize2-text")]
    [InlineData("@Summarize", "summarize")]
    [InlineData("Handle<Note>", "handle")]
    [InlineData("handlers.Handle<Dictionary<string, Note>>", "handle")]
    [InlineData("Async", "async")]
    public void ADefaultQueueName_IsTheHandlersLastIdentifier_InKebabCase(string expression, string name)
    {
        Assert.Equal(name, AsyncQueueNames.HandlerNameFromExpression(expression));
    }

    [Theory]
    [InlineData("(actor, db, ct) => Task.CompletedTask")]
    [InlineData("async (a, _, _) => await Task.Yield()")]
    [InlineData("MakeHandler()")]
    [InlineData("(RuleHandler<Actor>)Handle")]
    [InlineData("handlers[0]")]
    [InlineData("")]
    [InlineData(null)]
    public void ADefaultQueueName_NeedsAMethodName(string? expression)
    {
        var e = Assert.Throws<ArgumentException>(() => AsyncQueueNames.HandlerNameFromExpression(expression));
        Assert.Contains(".Queue(\"queue-name\", handler)", e.Message);
    }

    [Theory]
    [InlineData("Go")]                // too short
    [InlineData("SetAsidePoison")]    // ends in -poison
    public void ADefaultQueueName_MustBeAValidQueueName(string expression)
    {
        Assert.Throws<ArgumentException>(() => AsyncQueueNames.HandlerNameFromExpression(expression));
    }

    [Fact]
    public void Queue_WithAMethodGroup_IsNamedAfterIt()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>().Queue(Handle).Queue(Generic<Actor>);
        config.Rules.When<Note>().Queue(Generic<Note>, "Summarize");   // an explicit expression is used as given
        Assert.Equal(["handle", "generic", "summarize"], config.AsyncRegistrations.Select(r => r.Name));
    }

    [Fact]
    public void Queue_WithALambdaAndNoName_Throws()
    {
        var config = new LottaConfiguration();
        var e = Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Queue((_, _, _) => Task.CompletedTask));
        Assert.Contains("is not a method name", e.Message);
    }

    [Fact]
    public void ADuplicateDefaultName_Throws()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>().Queue(Handle);
        // HandleAsync loses its suffix and becomes "handle" too.
        Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Queue(HandleAsync));
        Assert.Throws<ArgumentException>(() => config.Rules.When<Note>().Queue("handle", (_, _, _) => Task.CompletedTask));
    }

    // === Names and modifiers ===

    [Theory]
    [InlineData("ab")]
    [InlineData("Describe")]
    [InlineData("has space")]
    [InlineData("-lead")]
    [InlineData("trail-")]
    [InlineData("double--hyphen")]
    [InlineData("describe-poison")]
    [InlineData("a-name-that-is-longer-than-forty-characters-x")]
    public void AQueueName_MustBeUsableAsAQueueName(string name)
    {
        var config = new LottaConfiguration();
        Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Queue(name, (_, _, _) => Task.CompletedTask));
    }

    [Fact]
    public void TwoQueues_CannotShareAName_AndModifierValuesAreChecked()
    {
        var config = new LottaConfiguration();
        var queued = config.Rules.When<Actor>().Queue("describe-mail", (_, _, _) => Task.CompletedTask);

        Assert.Throws<ArgumentException>(() => config.Rules.When<Note>().Queue("describe-mail", (_, _, _) => Task.CompletedTask));
        Assert.Throws<ArgumentOutOfRangeException>(() => queued.Retry(0, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => queued.Retry(3, TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => queued.Timeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => queued.Timeout(TimeSpan.FromSeconds(-1)));
        queued.Retry(1, TimeSpan.Zero);   // zero delay is allowed
    }

    [Fact]
    public void Modifiers_SetTheQueuesOptions_AndOnlyThoseOfTheQueueTheyFollow()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>()
            .Queue("first", (_, _, _) => Task.CompletedTask)
            .Queue("second", (_, _, _) => Task.CompletedTask)
                .Priority(10).EverySave().Retry(3, TimeSpan.FromSeconds(10)).Timeout(TimeSpan.FromMinutes(2));

        var first = config.AsyncRegistrations.Single(r => r.Name == "first").Options;
        var second = config.AsyncRegistrations.Single(r => r.Name == "second").Options;
        Assert.Equal((0, true, 5, TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(2)),
            (first.Priority, first.Latest, first.MaxAttempts, first.Timeout, first.RetryDelay));
        Assert.Equal((10, false, 3, TimeSpan.FromMinutes(2), TimeSpan.FromSeconds(10)),
            (second.Priority, second.Latest, second.MaxAttempts, second.Timeout, second.RetryDelay));
    }
}
