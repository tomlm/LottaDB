using System.Runtime.CompilerServices;
using LottaDB;

namespace LottaDB.Tests;

public class NoteViewExplicitBuilder : IBuilder<Note, NoteView>
{
    public async IAsyncEnumerable<BuildResult<NoteView>> BuildAsync(
        Note note, TriggerKind trigger, LottaDB db,
        [EnumeratorCancellation] CancellationToken ct)
    {
        if (trigger == TriggerKind.Deleted)
            yield break;

        var author = await db.GetAsync<Actor>(note.AuthorId, ct);

        yield return new BuildResult<NoteView>
        {
            Object = new NoteView
            {
                Domain = note.Domain,
                NoteId = note.NoteId,
                AuthorUsername = author?.Username ?? "",
                AuthorDisplay = author?.DisplayName ?? "",
                AvatarUrl = author?.AvatarUrl ?? "",
                Content = note.Content,
                Published = note.Published,
                Tags = note.Tags?.ToArray() ?? Array.Empty<string>(),
            }
        };
    }
}

public class FailingBuilder : IBuilder<Note, ModerationView>
{
    public async IAsyncEnumerable<BuildResult<ModerationView>> BuildAsync(
        Note note, TriggerKind trigger, LottaDB db,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.CompletedTask;
        throw new InvalidOperationException("Builder failed intentionally");
#pragma warning disable CS0162 // Unreachable code
        yield break;
#pragma warning restore CS0162
    }
}

public class MultiResultBuilder : IBuilder<Note, NoteView>
{
    public async IAsyncEnumerable<BuildResult<NoteView>> BuildAsync(
        Note note, TriggerKind trigger, LottaDB db,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.CompletedTask;
        if (trigger == TriggerKind.Deleted)
            yield break;

        yield return new BuildResult<NoteView>
        {
            Object = new NoteView { Domain = note.Domain, NoteId = $"{note.NoteId}-view1", Content = note.Content }
        };
        yield return new BuildResult<NoteView>
        {
            Object = new NoteView { Domain = note.Domain, NoteId = $"{note.NoteId}-view2", Content = note.Content }
        };
    }
}

// Cycle: saving CycleA produces CycleB, saving CycleB produces CycleA
public class CycleABuilder : IBuilder<CycleA, CycleB>
{
    public async IAsyncEnumerable<BuildResult<CycleB>> BuildAsync(
        CycleA entity, TriggerKind trigger, LottaDB db,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.CompletedTask;
        if (trigger == TriggerKind.Deleted) yield break;

        yield return new BuildResult<CycleB>
        {
            Object = new CycleB { Id = entity.Id, Value = $"from-a-{entity.Value}" }
        };
    }
}

public class CycleBBuilder : IBuilder<CycleB, CycleA>
{
    public async IAsyncEnumerable<BuildResult<CycleA>> BuildAsync(
        CycleB entity, TriggerKind trigger, LottaDB db,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.CompletedTask;
        if (trigger == TriggerKind.Deleted) yield break;

        yield return new BuildResult<CycleA>
        {
            Object = new CycleA { Id = entity.Id, Value = $"from-b-{entity.Value}" }
        };
    }
}

// For IBuilderFailureSink testing
public class TestBuilderFailureSink : IBuilderFailureSink
{
    public List<BuilderError> ReportedErrors { get; } = new();

    public Task ReportAsync(BuilderError error, CancellationToken ct = default)
    {
        ReportedErrors.Add(error);
        return Task.CompletedTask;
    }
}
