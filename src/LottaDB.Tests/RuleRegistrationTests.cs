using Lotta.Internal;
using static Lotta.Rule;

namespace Lotta.Tests;

/// <summary>
/// Registering rules: checks made when a rule is built, before any database is opened, so they
/// need no storage provider.
/// </summary>
public class RuleRegistrationTests
{
    private static Task Handle(Actor actor, RuleContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    private static Task HandleAsync(Actor actor, RuleContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    private static Task Generic<T>(T item, RuleContext context, CancellationToken cancellationToken) => Task.CompletedTask;

    // === Default rule names ===

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
    public void ADefaultRuleName_IsTheHandlersLastIdentifier_InKebabCase(string expression, string name)
    {
        Assert.Equal(name, RuleQueueNames.RuleNameFromExpression(expression, "Then"));
    }

    [Theory]
    [InlineData("(actor, context, ct) => Task.CompletedTask")]
    [InlineData("async (a, _, _) => await Task.Yield()")]
    [InlineData("MakeHandler()")]
    [InlineData("(RuleHandler<Actor>)Handle")]
    [InlineData("handlers[0]")]
    [InlineData("")]
    [InlineData(null)]
    public void ADefaultRuleName_NeedsAMethodName(string? expression)
    {
        var e = Assert.Throws<ArgumentException>(() => RuleQueueNames.RuleNameFromExpression(expression, "Update"));
        Assert.Contains(".Update(\"rule-name\", handler)", e.Message);
    }

    [Fact]
    public void ADefaultRuleName_MustBeAValidRuleName()
    {
        Assert.Throws<ArgumentException>(() => RuleQueueNames.RuleNameFromExpression("Go", "Then"));   // too short
    }

    [Fact]
    public void ThenAndUpdate_WithAMethodGroup_AreNamedAfterIt_AndTheirType()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>().Then(Handle).Update(Generic<Actor>);
        config.Rules.When<Note>().Then(Generic<Note>, "Summarize");   // an explicit expression is used as given
        Assert.Equal(["Actor/handle", "Actor/generic", "Note/summarize"], config.QueuedRules.Select(r => r.Id));
        Assert.Equal(["handle", "generic", "summarize"], config.QueuedRules.Select(r => r.Name));
        Assert.Equal([RuleVerb.Then, RuleVerb.Update, RuleVerb.Then], config.QueuedRules.Select(r => r.Verb));
    }

    [Fact]
    public void ThenAndUpdate_WithALambdaAndNoName_Throw()
    {
        var config = new LottaConfiguration();
        var e = Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Then((_, _, _) => Task.CompletedTask));
        Assert.Contains("is not a method name", e.Message);
        Assert.Contains(".Then(\"rule-name\", handler)", e.Message);
        e = Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Update((_, _, _) => Task.CompletedTask));
        Assert.Contains(".Update(\"rule-name\", handler)", e.Message);
        Assert.Empty(config.QueuedRules);
    }

    [Fact]
    public void ARuleName_IsUniqueWithinItsType_WhateverTheVerb()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>().Then(Handle);
        // HandleAsync loses its suffix and becomes "handle" too.
        Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Then(HandleAsync));
        var e = Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Update("handle", (_, _, _) => Task.CompletedTask));
        Assert.Contains("Actor/handle", e.Message);

        // Another type is another rule.
        config.Rules.When<Note>().Then("handle", (_, _, _) => Task.CompletedTask);
        Assert.Equal(["Actor/handle", "Note/handle"], config.QueuedRules.Select(r => r.Id));
    }

    [Theory]
    [InlineData("ab")]
    [InlineData("Describe")]
    [InlineData("has space")]
    [InlineData("-lead")]
    [InlineData("trail-")]
    [InlineData("double--hyphen")]
    [InlineData("with/slash")]
    [InlineData("a-name-that-is-longer-than-forty-characters-x")]
    public void ARuleName_IsLowerCaseLettersDigitsAndSingleHyphens(string name)
    {
        var config = new LottaConfiguration();
        Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Then(name, (_, _, _) => Task.CompletedTask));
        Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>().Update(name, (_, _, _) => Task.CompletedTask));
    }

    // === Changed ===

    [Fact]
    public void Changed_NamesTheInputsOfARule_AndLeavesTheRestOfTheFilterForLaterChecks()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>(a => Changed(a.DisplayName, a.AvatarUrl) && a.Counter > 0).Update("describe", (_, _, _) => Task.CompletedTask);
        config.Rules.When<Actor>(a => Changed(a.Counter)).Then("counted", (_, _, _) => Task.CompletedTask);
        config.Rules.When<Actor>(a => a.Counter > 0).Then("plain", (_, _, _) => Task.CompletedTask);

        var describe = config.QueuedRules[0];
        Assert.True(describe.UsesChanged);
        Assert.Equal(2, describe.Inputs.Length);
        var alice = new Actor { DisplayName = "Alice", AvatarUrl = "a.png" };
        Assert.Equal(new object?[] { "Alice", "a.png" }, describe.Inputs.Select(input => input(alice)).ToArray());
        // With Changed taken to be true, the counter is still checked.
        Assert.True(describe.Rest!(new Actor { Counter = 1 }));
        Assert.False(describe.Rest!(new Actor { Counter = 0 }));

        // Nothing but Changed: nothing is left to check later.
        Assert.Null(config.QueuedRules[1].Rest);
        Assert.False(config.QueuedRules[2].UsesChanged);
        Assert.Empty(config.QueuedRules[2].Inputs);
        Assert.Contains(typeof(Actor), config.ChangedTypes);
    }

    [Fact]
    public void Changed_UnderANot_OrInAnOr_LeavesAFilterThatLetsTheRuleThrough()
    {
        var config = new LottaConfiguration();
        config.Rules.When<Actor>(a => !Changed(a.Counter) && a.Counter > 0).Then("unchanged", (_, _, _) => Task.CompletedTask);
        config.Rules.When<Actor>(a => Changed(a.Counter) || a.Counter > 0).Then("either", (_, _, _) => Task.CompletedTask);

        // "Not changed" cannot be asked later either, and is taken to be true.
        Assert.True(config.QueuedRules[0].Rest!(new Actor { Counter = 1 }));
        Assert.False(config.QueuedRules[0].Rest!(new Actor { Counter = 0 }));
        // Changed or anything: always through.
        Assert.Null(config.QueuedRules[1].Rest);
    }

    [Fact]
    public void Changed_MustBeGivenValues_AndOnlyMeansSomethingInAFilter()
    {
        var config = new LottaConfiguration();
        Assert.Throws<ArgumentException>(() => config.Rules.When<Actor>(a => Changed()));
        // A delete has no newer version to compare with.
        Assert.Throws<ArgumentException>(() => config.Rules.WhenDeleted<Actor>(a => Changed(a.Counter)));
        Assert.Throws<InvalidOperationException>(() => Changed(1));
    }

    // === Settings ===

    [Fact]
    public void TheSettingsOfQueuedRules_AreChecked()
    {
        var config = new LottaConfiguration();
        Assert.Equal((4, 5, TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(5), 16),
            (config.MaxConcurrency, config.RuleMaxAttempts, config.RuleRetryDelay, config.RuleTimeout, config.MaxQueueHops));

        Assert.Throws<ArgumentOutOfRangeException>(() => config.MaxConcurrency = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => config.RuleMaxAttempts = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => config.RuleRetryDelay = TimeSpan.FromSeconds(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => config.RuleTimeout = TimeSpan.Zero);
        Assert.Throws<ArgumentOutOfRangeException>(() => config.RuleTimeout = TimeSpan.FromSeconds(-1));
        config.RuleRetryDelay = TimeSpan.Zero;   // no wait is allowed
    }
}
