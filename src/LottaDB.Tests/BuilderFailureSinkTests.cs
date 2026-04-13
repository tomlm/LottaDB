namespace LottaDB.Tests;

public class BuilderFailureSinkTests
{
    private (ILottaDB db, TestBuilderFailureSink sink) CreateDbWithFailingSink()
    {
        var sink = new TestBuilderFailureSink();
        var tableClient = LottaDBFixture.CreateInMemoryTableServiceClient();
        var directory = new Lucene.Net.Store.RAMDirectory();
        directory.SetLockFactory(Lucene.Net.Store.NoLockFactory.GetNoLockFactory());

        var options = new LottaDBOptions();
        options.Store<Note>();
        options.Store<ModerationView>();
        options.AddBuilder<Note, ModerationView, FailingBuilder>();

        var db = new LottaDB($"test{Guid.NewGuid():N}", tableClient, directory, options, sink);
        return (db, sink);
    }

    [Fact]
    public async Task FailureSink_ReceivesBuilderError()
    {
        var (db, sink) = CreateDbWithFailingSink();
        await db.SaveAsync(new Note { NoteId = "s1", AuthorId = "alice", Content = "Fail", Published = DateTimeOffset.UtcNow });
        Assert.NotEmpty(sink.ReportedErrors);
        Assert.Contains(sink.ReportedErrors, e => e.BuilderName.Contains("FailingBuilder"));
    }

    [Fact]
    public async Task FailureSink_ErrorHasCorrectMetadata()
    {
        var (db, sink) = CreateDbWithFailingSink();
        await db.SaveAsync(new Note { NoteId = "s2", AuthorId = "bob", Content = "Meta", Published = DateTimeOffset.UtcNow });

        var error = sink.ReportedErrors.First();
        Assert.Equal("FailingBuilder", error.BuilderName);
        Assert.Equal("Note", error.TriggerTypeName);
        Assert.NotEmpty(error.TriggerKey);
        Assert.IsType<InvalidOperationException>(error.Exception);
    }

    [Fact]
    public async Task FailureSink_SourceSaveSucceeds_DespiteFailure()
    {
        var (db, sink) = CreateDbWithFailingSink();
        var note = new Note { NoteId = "s3", AuthorId = "alice", Content = "Saved", Published = DateTimeOffset.UtcNow };
        var result = await db.SaveAsync(note);

        Assert.Contains(result.Changes, c => c.TypeName == "Note" && c.Kind == ChangeKind.Saved);
        Assert.NotEmpty(result.Errors);
        Assert.NotEmpty(sink.ReportedErrors);
    }
}
