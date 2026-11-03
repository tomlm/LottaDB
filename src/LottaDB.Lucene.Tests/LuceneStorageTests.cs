using Lotta.Tests;
using Lucene.Net.Index;

namespace Lotta.Lucene.Tests;

/// <summary>
/// Behavior specific to UseLucene(), where the search index is the storage.
/// </summary>
public class LuceneStorageTests
{
    public class TaggedItem
    {
        [Key]
        public string Id { get; set; } = "";

        [Tag]
        public string Category { get; set; } = "";

        [Queryable]
        public string Title { get; set; } = "";
    }

    private static LottaCatalog MemoryCatalog(string name) => new(name, c => c.UseLucene());

    private static Task<LottaDB> OpenAsync(LottaCatalog catalog, Action<ILottaConfiguration>? configure = null)
        => catalog.GetDatabaseAsync("db", config =>
        {
            config.Store<Actor>();
            config.Store<TaggedItem>();
            configure?.Invoke(config);
        }, TestContext.Current.CancellationToken);

    /// <summary>Live documents in the database's index, committed or not.</summary>
    private static int CountDocuments(LottaDB db)
    {
        db.ReloadSearcher();
        using var reader = DirectoryReader.Open(db.GetLuceneDirectory());
        return reader.NumDocs;
    }

    [Fact]
    public async Task Save_WritesOneDocumentPerObject()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(Save_WritesOneDocumentPerObject));
        var db = await OpenAsync(catalog);
        var before = CountDocuments(db); // JsonSchema rows etc.

        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice 2" }, ct);
        await db.ChangeAsync<Actor>("alice", a => a.DisplayName = "Alice 3", ct);

        Assert.Equal(before + 1, CountDocuments(db));
        var found = Assert.Single(db.Search<Actor>().ToList());
        Assert.Equal("Alice 3", found.DisplayName);
    }

    [Fact]
    public async Task ComputedKey_ResaveReplacesDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(ComputedKey_ResaveReplacesDocument));
        var db = await OpenAsync(catalog, c => c.Store<Actor>(s => s.SetKey(a => $"{a.Domain}-{a.Username}")));

        await db.SaveAsync(new Actor { Domain = "x.com", Username = "bob", DisplayName = "Bob" }, ct);
        await db.SaveAsync(new Actor { Domain = "x.com", Username = "bob", DisplayName = "Bobby" }, ct);

        Assert.Equal("Bobby", (await db.GetAsync<Actor>("x.com-bob", ct))!.DisplayName);
        Assert.Single(db.Search<Actor>().ToList());
    }

    [Fact]
    public async Task TagColumns_AreStoredAndQueryable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(TagColumns_AreStoredAndQueryable));
        var db = await OpenAsync(catalog);

        await db.SaveAsync(new TaggedItem { Id = "1", Category = "books", Title = "Dune" }, ct);
        await db.SaveAsync(new TaggedItem { Id = "2", Category = "music", Title = "Blue" }, ct);

        // [Tag] is a table column only — the query is answered from the stored row columns.
        var books = await db.GetManyAsync<TaggedItem>(t => t.Category == "books", cancellationToken: ct).ToListAsync(ct);
        Assert.Equal("1", Assert.Single(books).Id);
    }

    [Fact]
    public async Task Reads_SeeUncommittedWrites()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(Reads_SeeUncommittedWrites));
        var db = await OpenAsync(catalog, c => c.AutoCommitDelay = 60_000);

        await db.SaveAsync(new Actor { Username = "carol", DisplayName = "Carol" }, ct);

        Assert.Equal("Carol", (await db.GetAsync<Actor>("carol", ct))!.DisplayName);
        Assert.Single(await db.GetManyAsync<Actor>(a => a.DisplayName == "Carol", cancellationToken: ct).ToListAsync(ct));
    }

    [Fact]
    public async Task StaleETag_ThrowsConcurrencyException()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(StaleETag_ThrowsConcurrencyException));
        var db = await OpenAsync(catalog);
        await db.SaveAsync(new Actor { Username = "dave", DisplayName = "Dave" }, ct);

        var first = (await db.GetAsync<Actor>("dave", ct))!;
        var second = (await db.GetAsync<Actor>("dave", ct))!;
        first.DisplayName = "First";
        await db.SaveAsync(first, ct);

        second.DisplayName = "Second";
        await Assert.ThrowsAsync<ConcurrencyException>(() => db.SaveAsync(second, ct));
        Assert.Equal("First", (await db.GetAsync<Actor>("dave", ct))!.DisplayName);
    }

    [Fact]
    public async Task DeleteSchema_KeepsDocumentsAsUnsearchableRows()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(DeleteSchema_KeepsDocumentsAsUnsearchableRows));
        var db = await OpenAsync(catalog);
        await db.SaveAsync(new JsonSchema { Name = "Person" }, ct);
        var doc = System.Text.Json.JsonDocument.Parse("""{ "Name": "Gina" }""");
        doc.SetSchema("Person");
        await db.SaveAsync(doc, ct);
        var key = doc.GetKey()!;

        await db.DeleteAsync<JsonSchema>("Person", ct);
        db.ReloadSearcher();

        // Out of search, as with table storage — but the row itself is still stored.
        Assert.Empty(db.Search(j => j.GetSchema() == "Person").ToList());
        Assert.DoesNotContain(db.Search<object>().ToList(), o => o is System.Text.Json.JsonDocument);
        var loaded = (System.Text.Json.JsonDocument?)await db.GetAsync(key, ct);
        Assert.Equal("Gina", loaded!.RootElement.GetProperty("Name").GetString());
    }

    [Fact]
    public async Task Transaction_MappingFailure_WritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(Transaction_MappingFailure_WritesNothing));
        var db = await OpenAsync(catalog);
        var (adapter, table) = db.GetTableForTesting();

        // A good row, then one whose JSON cannot be mapped to its type.
        var good = adapter.CreateUpsertAction("good", new Actor { Username = "good", DisplayName = "Good" }, db.GetMeta(typeof(Actor)));
        var bad = new Azure.Data.Tables.TableEntity(adapter.PartitionKey, "bad")
        {
            ["_type"] = typeof(Actor).FullName,
            ["_object"] = "{ not json"u8.ToArray(),
        };
        var actions = new[] { good, new Azure.Data.Tables.TableTransactionAction(Azure.Data.Tables.TableTransactionActionType.UpsertReplace, bad) };

        await Assert.ThrowsAnyAsync<Exception>(() => catalog.GetTableClient().SubmitTransactionAsync(actions, ct));

        Assert.Null(await db.GetAsync<Actor>("good", ct));
        Assert.Empty(await adapter.GetManyAsync(table, cancellationToken: ct).ToListAsync(ct));
    }

    [Fact]
    public async Task RebuildSearchIndex_KeepsData()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(RebuildSearchIndex_KeepsData));
        var db = await OpenAsync(catalog);
        await db.SaveManyAsync(Enumerable.Range(1, 3).Select(i => new Actor { Username = $"u{i}", DisplayName = $"User {i}" }), ct);
        var etag = (await db.GetAsync<Actor>("u1", ct))!.GetETag();

        await db.RebuildSearchIndex(ct);

        Assert.Equal(3, db.Search<Actor>().Count());
        var u1 = await db.GetAsync<Actor>("u1", ct);
        Assert.Equal("User 1", u1!.DisplayName);
        Assert.Equal(etag, u1.GetETag());
    }

    [Fact]
    public async Task FileSystem_DataAndCatalogSurviveReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Path.Combine(TestRun.GetTempFolder(), nameof(FileSystem_DataAndCatalogSurviveReopen), Guid.NewGuid().ToString("N"));
        const string name = "reopen";

        using (var catalog = new LottaCatalog(name, c => c.UseLucene(LuceneDirectories.FileSystem(root))))
        {
            var db = await OpenAsync(catalog);
            await db.SaveAsync(new Actor { Username = "erin", DisplayName = "Erin" }, ct);
        }

        using (var catalog = new LottaCatalog(name, c => c.UseLucene(LuceneDirectories.FileSystem(root))))
        {
            Assert.Contains("db", await catalog.ListAsync(ct));
            var db = await OpenAsync(catalog);
            Assert.Equal("Erin", (await db.GetAsync<Actor>("erin", ct))!.DisplayName);
            Assert.Single(db.Search<Actor>().ToList());
        }
    }

    [Fact]
    public async Task FileSystem_DeleteCatalogRemovesData()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Path.Combine(TestRun.GetTempFolder(), nameof(FileSystem_DeleteCatalogRemovesData), Guid.NewGuid().ToString("N"));
        const string name = "deleted";

        using (var catalog = new LottaCatalog(name, c => c.UseLucene(LuceneDirectories.FileSystem(root))))
        {
            var db = await OpenAsync(catalog);
            await db.SaveAsync(new Actor { Username = "frank", DisplayName = "Frank" }, ct);
            await catalog.DeleteAsync(ct);
        }

        using (var catalog = new LottaCatalog(name, c => c.UseLucene(LuceneDirectories.FileSystem(root))))
        {
            Assert.Empty(await catalog.ListAsync(ct));
            var db = await OpenAsync(catalog);
            Assert.Null(await db.GetAsync<Actor>("frank", ct));
        }
    }

    [Fact]
    public async Task FileSystem_BlobsLiveInBlobsDirectoryAndSurviveReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Path.Combine(TestRun.GetTempFolder(), nameof(FileSystem_BlobsLiveInBlobsDirectoryAndSurviveReopen), Guid.NewGuid().ToString("N"));
        const string name = "blobreopen";

        using (var catalog = new LottaCatalog(name, c => c.UseLucene(LuceneDirectories.FileSystem(root))))
        {
            var db = await OpenAsync(catalog);
            await db.UploadBlobAsync("Docs/Readme.txt", "upper", cancellationToken: ct);
            await db.UploadBlobAsync("docs/readme.txt", "lower", cancellationToken: ct); // differs only by case
        }

        Assert.NotEmpty(System.IO.Directory.GetFiles(Path.Combine(root, name, "blobs"), "*.blob"));

        using (var catalog = new LottaCatalog(name, c => c.UseLucene(LuceneDirectories.FileSystem(root))))
        {
            var db = await OpenAsync(catalog);
            Assert.Equal("upper", await db.DownloadBlobStringAsync("Docs/Readme.txt", ct));
            Assert.Equal("lower", await db.DownloadBlobStringAsync("docs/readme.txt", ct));
            Assert.Equal(["Docs/Readme.txt", "docs/readme.txt"], await db.ListBlobsAsync(cancellationToken: ct));
        }
    }

    [Fact]
    public async Task Blob_FailedOverwrite_KeepsPreviousContent()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(Blob_FailedOverwrite_KeepsPreviousContent));
        var db = await OpenAsync(catalog);
        await db.UploadBlobAsync("a.txt", "original", cancellationToken: ct);

        await Assert.ThrowsAsync<IOException>(() => db.UploadBlobAsync("a.txt", new FailingStream(), cancellationToken: ct));

        Assert.Equal("original", await db.DownloadBlobStringAsync("a.txt", ct));
        Assert.Equal(["a.txt"], await db.ListBlobsAsync(cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_NoOverwrite_RejectsExisting()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(Blob_NoOverwrite_RejectsExisting));
        var db = await OpenAsync(catalog);
        await db.UploadBlobAsync("a.txt", "first", overwrite: false, cancellationToken: ct);

        var ex = await Assert.ThrowsAsync<Azure.RequestFailedException>(() => db.UploadBlobAsync("a.txt", "second", overwrite: false, cancellationToken: ct));
        Assert.Equal(409, ex.Status);
        Assert.Equal("first", await db.DownloadBlobStringAsync("a.txt", ct));
    }

    /// <summary>Yields some bytes, then fails mid-upload.</summary>
    private sealed class FailingStream : Stream
    {
        private bool _sent;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_sent) throw new IOException("connection lost");
            _sent = true;
            buffer[offset] = (byte)'x';
            return 1;
        }
    }

    [Fact]
    public async Task Queries_ByTypeAndByKey_SeeCommittedAndRecentRows()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = MemoryCatalog(nameof(Queries_ByTypeAndByKey_SeeCommittedAndRecentRows));
        var db = await OpenAsync(catalog);
        await db.SaveManyAsync(Enumerable.Range(1, 5).Select(i => new Actor { Username = $"a{i}", DisplayName = $"Actor {i}" }), ct);
        await db.SaveAsync(new TaggedItem { Id = "t1", Category = "books", Title = "Dune" }, ct);
        db.ReloadSearcher(); // commit: the rows above are now in the index proper

        // Recent writes, not yet in any refreshed searcher.
        await db.SaveAsync(new Actor { Username = "a6", DisplayName = "Actor 6" }, ct);
        await db.DeleteAsync<Actor>("a1", ct);

        var actors = await db.GetManyAsync<Actor>(cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(["a2", "a3", "a4", "a5", "a6"], actors.Select(a => a.Username).Order());

        var (adapter, table) = db.GetTableForTesting();
        var byKey = await adapter.GetManyAsync(table, ["a1", "a3", "a6", "t1"], cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(3, byKey.Count);
    }

    [Theory]
    [InlineData("PartitionKey eq 'db'", "db", null, null)]
    [InlineData("PartitionKey eq 'db' and (RowKey eq 'a' or RowKey eq 'b')", "db", "a,b", null)]
    [InlineData("PartitionKey eq 'o''neil' and _type eq 'X'", "o'neil", null, "X")]
    [InlineData("PartitionKey eq 'db' and (_type eq 'X' or _type eq 'Y') and (Name eq 'z')", "db", null, "X,Y")]
    [InlineData("RowKey eq 'a' and PartitionKey eq 'db'", "db", "a", null)]
    [InlineData("PartitionKey eq 'db' and Name eq 'or'", "db", null, null)]
    [InlineData("PartitionKey eq 'db' or PartitionKey eq 'other'", null, null, null)]
    [InlineData("PartitionKey eq 'db' and RowKey eq 'a' or RowKey eq 'b'", null, null, null)]
    [InlineData("PartitionKey eq 'db' and (RowKey eq 'a' or _type eq 'X')", "db", null, null)]
    [InlineData("(PartitionKey eq 'db') and (RowKey eq 'a')", "db", "a", null)]
    public void ScopeOf_FindsWhatTheFilterPinsDown(string filter, string? partition, string? rowKeys, string? types)
    {
        var scope = LuceneTableClient.ScopeOf(filter);
        Assert.Equal(partition, scope.PartitionKey);
        Assert.Equal(rowKeys, scope.RowKeys == null ? null : string.Join(",", scope.RowKeys.Order()));
        Assert.Equal(types, scope.Types == null ? null : string.Join(",", scope.Types.Order()));
    }
}
