namespace LottaDB.Tests;

public class BuilderFailureSinkTests
{
    [Fact]
    public async Task FailureSink_ReceivesBuilderError()
    {
        var sink = new TestBuilderFailureSink();
        var services = new ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Note>();
            opts.Store<ModerationView>();
            opts.AddBuilder<Note, ModerationView, FailingBuilder>();
        });
        services.AddSingleton<IBuilderFailureSink>(sink);
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        await db.SaveAsync(new Note { Domain = "sink.test", NoteId = "s1", AuthorId = "alice", Content = "Fail", Published = DateTimeOffset.UtcNow });

        Assert.NotEmpty(sink.ReportedErrors);
        Assert.Contains(sink.ReportedErrors, e => e.BuilderName.Contains("FailingBuilder"));
    }

    [Fact]
    public async Task FailureSink_ErrorHasCorrectMetadata()
    {
        var sink = new TestBuilderFailureSink();
        var services = new ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Note>();
            opts.Store<ModerationView>();
            opts.AddBuilder<Note, ModerationView, FailingBuilder>();
        });
        services.AddSingleton<IBuilderFailureSink>(sink);
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        await db.SaveAsync(new Note { Domain = "sink.test", NoteId = "s2", AuthorId = "bob", Content = "Meta", Published = DateTimeOffset.UtcNow });

        var error = sink.ReportedErrors.First();
        Assert.Equal("FailingBuilder", error.BuilderName);
        Assert.Equal("Note", error.TriggerTypeName);
        Assert.NotEmpty(error.TriggerKey);
        Assert.IsType<InvalidOperationException>(error.Exception);
    }

    [Fact]
    public async Task FailureSink_SourceSaveSucceeds_DespiteFailure()
    {
        var sink = new TestBuilderFailureSink();
        var services = new ServiceCollection();
        services.AddLottaDB(opts =>
        {
            opts.UseInMemoryTables();
            opts.UseLuceneDirectory(new RAMDirectoryProvider());
            opts.Store<Note>();
            opts.Store<ModerationView>();
            opts.AddBuilder<Note, ModerationView, FailingBuilder>();
        });
        services.AddSingleton<IBuilderFailureSink>(sink);
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<ILottaDB>();

        var note = new Note { Domain = "sink.test", NoteId = "s3", AuthorId = "alice", Content = "Saved", Published = DateTimeOffset.UtcNow };
        var result = await db.SaveAsync(note);

        // Source saved despite builder failure
        Assert.Contains(result.Changes, c => c.TypeName == "Note" && c.Kind == ChangeKind.Saved);
        // Error captured
        Assert.NotEmpty(result.Errors);
        // Sink received the error too
        Assert.NotEmpty(sink.ReportedErrors);
    }
}
