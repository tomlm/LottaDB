using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public abstract class BlobOnUploadTestBase : LottaTestBase
{
    protected BlobOnUploadTestBase(Action<LottaCatalog> config) : base(config) { }
    // === OnUpload handler integration tests ===

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_ReturnsMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload(); // default handler
        }, ct);

        var meta = await db.UploadBlobAsync("docs/readme.txt", "Hello world", cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.IsType<BlobFile>(meta);
        Assert.Equal("docs/readme.txt", meta.Path);
        Assert.Equal("readme.txt", meta.Name);
        Assert.Equal("docs", meta.FolderPath);
        Assert.Equal("text/plain", meta.MediaType);
        Assert.Equal("Hello world", meta.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_TextContent_Searchable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("notes/meeting.md", "We discussed the quarterly revenue forecast", cancellationToken: ct);
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("quarterly revenue").ToList();
        Assert.Single(results);
        Assert.Equal("notes/meeting.md", results[0].Path);
    }

    [Fact]
    public async Task Blob_OnUpload_DefaultHandler_BinaryFile_CorrectType()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("photos/test.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/jpeg", meta.MediaType);
        Assert.Null(meta.Content); // binary, no text extraction
    }

    [Fact]
    public async Task Blob_OnUpload_MetadataPersistedAndRetrievable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("docs/notes.txt", "Some important notes", cancellationToken: ct);

        var loaded = await db.GetAsync<BlobFile>("docs/notes.txt", cancellationToken: ct);
        Assert.NotNull(loaded);
        Assert.Equal("notes.txt", loaded.Name);
        Assert.Equal("docs", loaded.FolderPath);
        Assert.Equal("text/plain", loaded.MediaType);
        Assert.Equal("Some important notes", loaded.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterUpload()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("test.txt", "content", cancellationToken: ct);
        Assert.NotNull(meta);

        // Database should be set — DownloadAsync should work
        var stream = await meta.DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Blob_OnUpload_DatabasePropertySet_AfterGetAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("test.txt", "content", cancellationToken: ct);

        var loaded = await db.GetAsync<BlobFile>("test.txt", cancellationToken: ct);
        Assert.NotNull(loaded);

        var stream = await loaded.DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("content", await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Blob_OnUpload_DeleteAsync_CascadesMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("file.txt", "some content", cancellationToken: ct);

        // Metadata exists
        var before = await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct);
        Assert.NotNull(before);

        // Delete via blob API
        await db.DeleteBlobAsync("file.txt", cancellationToken: ct);

        // Both blob and metadata are gone
        Assert.Null(await db.DownloadBlobStringAsync("file.txt", cancellationToken: ct));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_OnUpload_BlobFileDeleteAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("file.txt", "some content", cancellationToken: ct);
        Assert.NotNull(meta);

        // Delete via BlobFile convenience method
        var deleted = await meta.DeleteAsync(ct);
        Assert.True(deleted);

        Assert.Null(await db.DownloadBlobStringAsync("file.txt", cancellationToken: ct));
        Assert.Null(await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_OnUpload_NoHandler_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct); // no OnUpload

        var meta = await db.UploadBlobAsync("test.txt", "content", cancellationToken: ct);

        Assert.Null(meta);
        // Blob still uploaded
        Assert.Equal("content", await db.DownloadBlobStringAsync("test.txt", cancellationToken: ct));
    }

    [Fact]
    public async Task Blob_OnUpload_Overwrite_UpdatesMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("file.txt", "version 1", cancellationToken: ct);
        await db.UploadBlobAsync("file.txt", "version 2", cancellationToken: ct);

        var meta = await db.GetAsync<BlobFile>("file.txt", cancellationToken: ct);
        Assert.NotNull(meta);
        Assert.Equal("version 2", meta.Content);
    }

    [Fact]
    public async Task Blob_OnUpload_ExplicitContentType_Used()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var meta = await db.UploadBlobAsync("data.bin", new byte[] { 1, 2, 3 }, contentType: "image/png", cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.IsType<BlobPhoto>(meta);
        Assert.Equal("image/png", meta.MediaType);
    }

    [Fact]
    public async Task Blob_OnUpload_Search_DatabasePropertySet()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("notes/search-test.txt", "findable content here", cancellationToken: ct);
        db.ReloadSearcher();

        var results = db.Search<BlobFile>("findable").ToList();
        Assert.Single(results);

        // Database should be set on Search results — DownloadAsync should work
        var stream = await results[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("findable content here", await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Blob_OnUpload_StreamOverload_ReturnsMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("stream content"));
        var meta = await db.UploadBlobAsync("stream.txt", stream, cancellationToken: ct);

        Assert.NotNull(meta);
        Assert.Equal("stream.txt", meta.Name);
        Assert.Equal("text/plain", meta.MediaType);
        Assert.Equal("stream content", meta.Content);
    }

    // === On<T> polymorphic handler dispatch ===

    [Fact]
    public async Task On_BaseTypeHandler_FiresForDerivedType()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var firedTypes = new List<string>();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db, _) =>
            {
                firedTypes.Add("BlobFile");
            });
            config.On<BlobPhoto>(async (photo, kind, db, _) =>
            {
                firedTypes.Add("BlobPhoto");
            });
        }, ct);

        // Upload a .jpg → creates BlobPhoto
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: ct);

        // Both On<BlobPhoto> and On<BlobFile> should fire
        Assert.Contains("BlobPhoto", firedTypes);
        Assert.Contains("BlobFile", firedTypes);
    }

    [Fact]
    public async Task On_BaseTypeHandler_ReceivesDerivedInstance()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        BlobFile? received = null;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobFile>(async (file, kind, db, _) =>
            {
                received = file;
            });
        }, ct);

        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: ct);

        Assert.NotNull(received);
        Assert.IsType<BlobPhoto>(received); // receives the actual derived type
    }

    [Fact]
    public async Task On_UnrelatedTypeHandler_DoesNotFire()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var fired = false;
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();

            config.On<BlobMusic>(async (music, kind, db, _) =>
            {
                fired = true;
            });
        }, ct);

        // Upload a .jpg → BlobPhoto, not BlobMusic
        await db.UploadBlobAsync("test.jpg", new byte[] { 0xFF, 0xD8 }, cancellationToken: ct);

        Assert.False(fired);
    }

    [Fact]
    public async Task Blob_Search_BlobPhoto_ReturnsOnlyPhotos()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var photos = db.Search<BlobPhoto>().ToList();
        Assert.Single(photos);
        Assert.Equal("photos/cat.jpg", photos[0].Path);
        Assert.Equal("image/jpeg", photos[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobMusic_ReturnsOnlyMusic()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var music = db.Search<BlobMusic>().ToList();
        Assert.Single(music);
        Assert.Equal("music/song.mp3", music[0].Path);
        Assert.Equal("audio/mpeg", music[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobDocument_ReturnsOnlyDocuments()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/report.pdf", new byte[] { 0x25, 0x50, 0x44, 0x46 }, cancellationToken: ct);
        await db.UploadBlobAsync("notes/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var docs = db.Search<BlobDocument>().ToList();
        Assert.Single(docs);
        Assert.Equal("docs/report.pdf", docs[0].Path);
        Assert.Equal("application/pdf", docs[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobVideo_ReturnsOnlyVideos()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("videos/clip.mp4", new byte[] { 0x00, 0x00, 0x00, 0x1C }, cancellationToken: ct);
        await db.UploadBlobAsync("notes/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var videos = db.Search<BlobVideo>().ToList();
        Assert.Single(videos);
        Assert.Equal("videos/clip.mp4", videos[0].Path);
        Assert.Equal("video/mp4", videos[0].MediaType);
    }

    [Fact]
    public async Task Blob_Search_BlobFile_ReturnsAllBlobTypes()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("photos/cat.jpg", new byte[] { 0xFF, 0xD8, 0xFF }, cancellationToken: ct);
        await db.UploadBlobAsync("music/song.mp3", new byte[] { 0x49, 0x44, 0x33 }, cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.txt", "some text content", cancellationToken: ct);
        db.ReloadSearcher();

        var all = db.Search<BlobFile>().ToList();
        Assert.Equal(3, all.Count);
        Assert.Contains(all, b => b is BlobPhoto);
        Assert.Contains(all, b => b is BlobMusic);
        Assert.Contains(all, b => b is BlobFile f && f.MediaType == "text/plain");
    }

    [Fact]
    public async Task Blob_Search_BlobPhoto_DownloadAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var content = new byte[] { 0xFF, 0xD8, 0xFF, 0xE0, 0x00 };
        await db.UploadBlobAsync("photos/test.jpg", content, cancellationToken: ct);
        db.ReloadSearcher();

        var photos = db.Search<BlobPhoto>().ToList();
        Assert.Single(photos);

        var stream = await photos[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        Assert.Equal(content, ms.ToArray());
    }

    [Fact]
    public async Task Blob_Search_BlobMusic_DownloadAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        var content = new byte[] { 0x49, 0x44, 0x33, 0x04, 0x00 };
        await db.UploadBlobAsync("music/track.mp3", content, cancellationToken: ct);
        db.ReloadSearcher();

        var music = db.Search<BlobMusic>().ToList();
        Assert.Single(music);

        var stream = await music[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms, ct);
        Assert.Equal(content, ms.ToArray());
    }

    [Fact]
    public async Task Blob_Search_BlobDocument_DownloadAsync_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await catalog.GetDatabaseAsync("db1", config =>
        {
            config.OnUpload();
        }, ct);

        await db.UploadBlobAsync("docs/notes.txt", "downloadable text", cancellationToken: ct);
        db.ReloadSearcher();

        // BlobFile with text/plain should still be searchable and downloadable
        var results = db.Search<BlobFile>("downloadable").ToList();
        Assert.Single(results);

        var stream = await results[0].DownloadAsync(ct);
        Assert.NotNull(stream);
        using var reader = new StreamReader(stream);
        Assert.Equal("downloadable text", await reader.ReadToEndAsync(ct));
    }
}

