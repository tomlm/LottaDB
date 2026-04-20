namespace Lotta.Tests;

/// <summary>
/// Tests for bulk SaveAsync(IEnumerable) and DeleteAsync(IEnumerable) operations.
/// </summary>
public class BatchTests : IClassFixture<LottaDBFixture>
{
    [Fact]
    public async Task SaveAsync_Bulk_AllEntitiesPersist()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actors = Enumerable.Range(1, 5).Select(i => new Actor
        {
            Username = $"bulk-{i}",
            DisplayName = $"Actor {i}",
            Domain = "batch.test"
        });

        var result = await db.SaveManyAsync(actors, TestContext.Current.CancellationToken);

        Assert.Equal(5, result.Changes.Count);
        Assert.All(result.Changes, c => Assert.Equal(ChangeKind.Saved, c.Kind));

        for (int i = 1; i <= 5; i++)
        {
            var loaded = await db.GetAsync<Actor>($"bulk-{i}", TestContext.Current.CancellationToken);
            Assert.NotNull(loaded);
            Assert.Equal($"Actor {i}", loaded.DisplayName);
        }
    }

    [Fact]
    public async Task SaveAsync_Bulk_LuceneSearchReflectsAll()
    {
        var db = await LottaDBFixture.CreateDbAsync();

        var results = db.Search<Actor>().ToList();
        Assert.Empty(results);

        var actors = Enumerable.Range(1, 3).Select(i => new Actor
        {
            Username = $"search-bulk-{i}",
            DisplayName = $"Searchable{i}",
            Domain = "batch.test"
        });

        await db.SaveManyAsync(actors, TestContext.Current.CancellationToken);

        results = db.Search<Actor>().ToList();
        Assert.Equal(3, results.Count());
    }

    [Fact]
    public async Task SaveAsync_Bulk_DuplicateKey_AutoFlushes()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        // Save same key twice — first should be flushed, second overwrites
        var entities = new[]
        {
            new Actor { Username = "dup-key", DisplayName = "V1", Domain = "batch.test" },
            new Actor { Username = "dup-key", DisplayName = "V2", Domain = "batch.test" },
        };

        var result = await db.SaveManyAsync(entities, TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Changes.Count);
        var loaded = await db.GetAsync<Actor>("dup-key", TestContext.Current.CancellationToken);
        Assert.NotNull(loaded);
        Assert.Equal("V2", loaded.DisplayName);
    }

    [Fact]
    public async Task SaveAsync_Bulk_Over100_AutoFlushes()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var actors = Enumerable.Range(1, 150).Select(i => new Actor
        {
            Username = $"batch100-{i}",
            DisplayName = $"Actor {i}",
            Domain = "batch.test"
        });

        var result = await db.SaveManyAsync(actors, TestContext.Current.CancellationToken);

        Assert.Equal(150, result.Changes.Count);

        // Spot-check first, 100th, and last
        Assert.NotNull(await db.GetAsync<Actor>("batch100-1", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<Actor>("batch100-100", TestContext.Current.CancellationToken));
        Assert.NotNull(await db.GetAsync<Actor>("batch100-150", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SaveAsync_Bulk_OnHandlers_RunInline()
    {
        var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView
                {
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    Content = note.Content,
                });
            });
        });

        var notes = Enumerable.Range(1, 3).Select(i => new Note
        {
            NoteId = $"bn-{i}",
            AuthorId = "alice",
            Content = $"Bulk note {i}",
            Published = DateTimeOffset.UtcNow,
        });

        var result = await db.SaveManyAsync(notes, TestContext.Current.CancellationToken);

        // 3 notes + 3 views = 6 changes
        Assert.Equal(6, result.Changes.Count);

        for (int i = 1; i <= 3; i++)
        {
            var view = await db.GetAsync<NoteView>($"nv-bn-{i}", TestContext.Current.CancellationToken);
            Assert.NotNull(view);
            Assert.Equal($"Bulk note {i}", view.Content);
        }
    }

    [Fact]
    public async Task SaveAsync_Bulk_OnHandlers_ShareLuceneSession()
    {
        var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Note>(async (note, kind, db) =>
            {
                if (kind == TriggerKind.Deleted) return;
                await db.SaveAsync(new NoteView
                {
                    Id = $"nv-{note.NoteId}",
                    NoteId = note.NoteId,
                    Content = note.Content,
                });
            });
        });

        var notes = Enumerable.Range(1, 3).Select(i => new Note
        {
            NoteId = $"search-bn-{i}",
            AuthorId = "alice",
            Content = $"Searchable bulk {i}",
            Published = DateTimeOffset.UtcNow,
        });

        await db.SaveManyAsync(notes, TestContext.Current.CancellationToken);

        // Handler-created NoteViews should also be in Lucene
        var views = db.Search<NoteView>().ToList();
        Assert.Equal(3, views.Count(v => v.NoteId.StartsWith("search-bn-")));
    }

    [Fact]
    public async Task SaveAsync_Bulk_Empty_ReturnsEmptyResult()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var result = await db.SaveManyAsync(Array.Empty<Actor>(), TestContext.Current.CancellationToken);
        Assert.Empty(result.Changes);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task DeleteAsync_Bulk_RemovesAll()
    {
        var db = await LottaDBFixture.CreateDbAsync();

        // Seed data
        var actors = Enumerable.Range(1, 5).Select(i => new Actor { Username = $"del-bulk-{i}", DisplayName = $"Actor {i}" }).ToList();
        await db.SaveManyAsync(actors, TestContext.Current.CancellationToken);

        var keys = Enumerable.Range(1, 5).Select(i => $"del-bulk-{i}").ToList();
        var result = await db.DeleteManyAsync(keys, TestContext.Current.CancellationToken);

        Assert.Equal(5, result.Changes.Count);
        Assert.All(result.Changes, c => Assert.Equal(ChangeKind.Deleted, c.Kind));

        for (int i = 1; i <= 5; i++)
            Assert.Null(await db.GetAsync<Actor>($"del-bulk-{i}", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeleteAsync_Bulk_RemovesFromLucene()
    {
        var db = await LottaDBFixture.CreateDbAsync();

        await db.SaveManyAsync(Enumerable.Range(1, 3).Select(i => new Actor { Username = $"del-search-{i}", DisplayName = $"Actor {i}" }), TestContext.Current.CancellationToken);

        Assert.Equal(3, db.Search<Actor>().Count(a => a.Username.StartsWith("del-search-")));

        var keys = Enumerable.Range(1, 3).Select(i => $"del-search-{i}");
        await db.DeleteManyAsync(keys, TestContext.Current.CancellationToken);

        Assert.Equal(0, db.Search<Actor>().Count(a => a.Username.StartsWith("del-search-")));
    }

    [Fact]
    public async Task DeleteAsync_Bulk_OnHandlers_RunInline()
    {
        int deleteCount = 0;
        var db = await LottaDBFixture.CreateDbAsync(opts =>
        {
            opts.On<Actor>(async (actor, kind, db) =>
            {
                if (kind == TriggerKind.Deleted)
                    Interlocked.Increment(ref deleteCount);
            });
        });


        await db.SaveManyAsync(Enumerable.Range(1, 3).Select(i => new Actor { Username = $"del-handler-{i}", DisplayName = $"Actor {i}" }), TestContext.Current.CancellationToken);

        var keys = Enumerable.Range(1, 3).Select(i => $"del-handler-{i}");
        await db.DeleteManyAsync(keys, TestContext.Current.CancellationToken);

        Assert.Equal(3, deleteCount);
    }

    [Fact]
    public async Task DeleteAsync_Bulk_Empty_ReturnsEmptyResult()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var result = await db.DeleteManyAsync(Array.Empty<string>(), TestContext.Current.CancellationToken);
        Assert.Empty(result.Changes);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task DeleteAsync_Bulk_NonExistent_NoError()
    {
        var db = await LottaDBFixture.CreateDbAsync();
        var result = await db.DeleteManyAsync(new[] { "no-exist-1", "no-exist-2" }, TestContext.Current.CancellationToken);
        Assert.Equal(2, result.Changes.Count);
        Assert.All(result.Changes, c => Assert.Null(c.Object));
    }
}
