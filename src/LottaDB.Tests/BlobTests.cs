using System.Runtime.CompilerServices;

namespace Lotta.Tests;

public class BlobTests : LottaTestBase
{
    [Fact]
    public async Task Blob_UploadAndDownload_Stream()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        var content = "Hello, Blob World!";
        using var uploadStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
        await db.UploadBlobAsync("test.txt", uploadStream, cancellationToken: ct);

        var downloadStream = await db.DownloadBlobAsync("test.txt", cancellationToken: ct);
        Assert.NotNull(downloadStream);
        using var reader = new StreamReader(downloadStream);
        var result = await reader.ReadToEndAsync(ct);
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_Bytes()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        var content = new byte[] { 1, 2, 3, 4, 5 };
        await db.UploadBlobAsync("data.bin", content, cancellationToken: ct);

        var result = await db.DownloadBlobBytesAsync("data.bin", cancellationToken: ct);
        Assert.NotNull(result);
        Assert.Equal(content, result);
    }

    [Fact]
    public async Task Blob_UploadAndDownload_String()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("note.txt", "Hello from LottaDB", cancellationToken: ct);

        var result = await db.DownloadBlobStringAsync("note.txt", cancellationToken: ct);
        Assert.Equal("Hello from LottaDB", result);
    }

    [Fact]
    public async Task Blob_Download_NotFound_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        var stream = await db.DownloadBlobAsync("nonexistent.txt", cancellationToken: ct);
        Assert.Null(stream);

        var bytes = await db.DownloadBlobBytesAsync("nonexistent.txt", cancellationToken: ct);
        Assert.Null(bytes);

        var str = await db.DownloadBlobStringAsync("nonexistent.txt", cancellationToken: ct);
        Assert.Null(str);
    }

    [Fact]
    public async Task Blob_Delete()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("todelete.txt", "temp", cancellationToken: ct);
        var deleted = await db.DeleteBlobAsync("todelete.txt", cancellationToken: ct);
        Assert.True(deleted);

        var result = await db.DownloadBlobStringAsync("todelete.txt", cancellationToken: ct);
        Assert.Null(result);

        // Delete again — should return false
        var deletedAgain = await db.DeleteBlobAsync("todelete.txt", cancellationToken: ct);
        Assert.False(deletedAgain);
    }

    [Fact]
    public async Task Blob_ListBlobs()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("photos/a.jpg", "image-a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/b.jpg", "image-b", cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: ct);

        // List all
        var all = await db.ListBlobsAsync(cancellationToken: ct);
        Assert.Equal(3, all.Count);

        // List with prefix
        var photos = await db.ListBlobsAsync("photos/", cancellationToken: ct);
        Assert.Equal(2, photos.Count);
        Assert.Contains("photos/a.jpg", photos);
        Assert.Contains("photos/b.jpg", photos);

        var docs = await db.ListBlobsAsync("docs/", cancellationToken: ct);
        Assert.Single(docs);
        Assert.Contains("docs/readme.md", docs);
    }

    [Fact]
    public async Task Blob_ListBlobs_NonRecursive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("root.txt", "root", cancellationToken: ct);
        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c", cancellationToken: ct);

        // Non-recursive at root — only root.txt
        var rootFiles = await db.ListBlobsAsync(recursive: false, cancellationToken: ct);
        Assert.Single(rootFiles);
        Assert.Contains("root.txt", rootFiles);

        // Non-recursive in photos/ — only a.jpg
        var photosFlat = await db.ListBlobsAsync("photos/", recursive: false, cancellationToken: ct);
        Assert.Single(photosFlat);
        Assert.Contains("photos/a.jpg", photosFlat);

        // Recursive in photos/ — all 3 photos
        var photosAll = await db.ListBlobsAsync("photos/", recursive: true, cancellationToken: ct);
        Assert.Equal(3, photosAll.Count);
        Assert.Contains("photos/a.jpg", photosAll);
        Assert.Contains("photos/2024/b.jpg", photosAll);
        Assert.Contains("photos/2024/trip/c.jpg", photosAll);
    }

    [Fact]
    public async Task Blob_ListFolders_NonRecursive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("root.txt", "root", cancellationToken: ct);
        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: ct);

        // Immediate subfolders of root
        var rootFolders = await db.ListBlobFoldersAsync(recursive: false, cancellationToken: ct);
        Assert.Equal(2, rootFolders.Count);
        Assert.Contains("photos/", rootFolders);
        Assert.Contains("docs/", rootFolders);

        // Immediate subfolders of photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: false, cancellationToken: ct);
        Assert.Single(photoFolders);
        Assert.Contains("photos/2024/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_Recursive()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/b.jpg", "b", cancellationToken: ct);
        await db.UploadBlobAsync("photos/2024/trip/c.jpg", "c", cancellationToken: ct);
        await db.UploadBlobAsync("docs/readme.md", "readme", cancellationToken: ct);

        // All folders recursively from root
        var allFolders = await db.ListBlobFoldersAsync(recursive: true, cancellationToken: ct);
        Assert.Contains("photos/", allFolders);
        Assert.Contains("photos/2024/", allFolders);
        Assert.Contains("photos/2024/trip/", allFolders);
        Assert.Contains("docs/", allFolders);

        // All folders recursively under photos/
        var photoFolders = await db.ListBlobFoldersAsync("photos/", recursive: true, cancellationToken: ct);
        Assert.Contains("photos/2024/", photoFolders);
        Assert.Contains("photos/2024/trip/", photoFolders);
        Assert.DoesNotContain("photos/", photoFolders); // don't include the queried folder itself
        Assert.DoesNotContain("docs/", photoFolders);
    }

    [Fact]
    public async Task Blob_ListFolders_EmptyFolder()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("only-file.txt", "content", cancellationToken: ct);

        var folders = await db.ListBlobFoldersAsync(recursive: false, cancellationToken: ct);
        Assert.Empty(folders);
    }

    [Fact]
    public async Task Blob_ListBlobs_FolderNormalization()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("photos/a.jpg", "a", cancellationToken: ct);

        // Both with and without trailing slash should work
        var withSlash = await db.ListBlobsAsync("photos/", cancellationToken: ct);
        var withoutSlash = await db.ListBlobsAsync("photos", cancellationToken: ct);
        Assert.Equal(withSlash, withoutSlash);
    }

    [Fact]
    public async Task Blob_IsolatedPerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db1 = await CreateDbAsync(catalog, "db1", ct);
        var db2 = await CreateDbAsync(catalog, "db2", ct);

        await db1.UploadBlobAsync("shared.txt", "from db1", cancellationToken: ct);
        await db2.UploadBlobAsync("shared.txt", "from db2", cancellationToken: ct);

        // Each database has its own blob
        var result1 = await db1.DownloadBlobStringAsync("shared.txt", ct);
        var result2 = await db2.DownloadBlobStringAsync("shared.txt", ct);
        Assert.Equal("from db1", result1);
        Assert.Equal("from db2", result2);

        // Listing is scoped per database
        var db1Blobs = await db1.ListBlobsAsync(cancellationToken: ct);
        var db2Blobs = await db2.ListBlobsAsync(cancellationToken: ct);
        Assert.Single(db1Blobs);
        Assert.Single(db2Blobs);

        // Deleting from db1 doesn't affect db2
        await db1.DeleteBlobAsync("shared.txt", ct);
        Assert.Null(await db1.DownloadBlobStringAsync("shared.txt", ct));
        Assert.Equal("from db2", await db2.DownloadBlobStringAsync("shared.txt", ct));
    }

    [Fact]
    public async Task Blob_Overwrite()
    {
        var ct = TestContext.Current.CancellationToken;
        using var catalog = CreateCatalog();
        var db = await CreateDbAsync(catalog, "db1", ct);

        await db.UploadBlobAsync("file.txt", "version 1", cancellationToken: ct);
        await db.UploadBlobAsync("file.txt", "version 2", cancellationToken: ct);

        var result = await db.DownloadBlobStringAsync("file.txt", cancellationToken: ct);
        Assert.Equal("version 2", result);
    }
}
