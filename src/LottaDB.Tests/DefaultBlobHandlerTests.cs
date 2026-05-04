namespace Lotta.Tests;

public class DefaultBlobHandlerTests
{
    // === Mime type detection from extension ===

    [Theory]
    [InlineData(".jpg", "image/jpeg")]
    [InlineData(".jpeg", "image/jpeg")]
    [InlineData(".png", "image/png")]
    [InlineData(".gif", "image/gif")]
    [InlineData(".webp", "image/webp")]
    [InlineData(".tiff", "image/tiff")]
    [InlineData(".bmp", "image/bmp")]
    [InlineData(".svg", "image/svg+xml")]
    [InlineData(".mp3", "audio/mpeg")]
    [InlineData(".wav", "audio/wav")]
    [InlineData(".flac", "audio/flac")]
    [InlineData(".mp4", "video/mp4")]
    [InlineData(".mkv", "video/x-matroska")]
    [InlineData(".avi", "video/x-msvideo")]
    [InlineData(".pdf", "application/pdf")]
    [InlineData(".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")]
    [InlineData(".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")]
    [InlineData(".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation")]
    [InlineData(".html", "text/html")]
    [InlineData(".json", "application/json")]
    [InlineData(".xml", "application/xml")]
    [InlineData(".md", "text/markdown")]
    [InlineData(".txt", "text/plain")]
    [InlineData(".cs", "text/x-csharp")]
    [InlineData(".py", "text/x-python")]
    [InlineData(".eml", "message/rfc822")]
    [InlineData(".unknown", "application/octet-stream")]
    [InlineData("", "application/octet-stream")]
    public void GetMimeType_ReturnsCorrectType(string ext, string expected)
    {
        Assert.Equal(expected, DefaultBlobHandler.GetMimeType(ext));
    }

    [Theory]
    [InlineData(".JPG", "image/jpeg")]
    [InlineData(".Pdf", "application/pdf")]
    [InlineData(".JSON", "application/json")]
    public void GetMimeType_IsCaseInsensitive(string ext, string expected)
    {
        Assert.Equal(expected, DefaultBlobHandler.GetMimeType(ext));
    }

    // === Object type creation from mime type ===

    [Theory]
    [InlineData("photos/test.jpg", typeof(BlobPhoto))]
    [InlineData("photos/test.png", typeof(BlobPhoto))]
    [InlineData("photos/test.gif", typeof(BlobPhoto))]
    [InlineData("music/song.mp3", typeof(BlobMusic))]
    [InlineData("music/song.flac", typeof(BlobMusic))]
    [InlineData("videos/clip.mp4", typeof(BlobVideo))]
    [InlineData("videos/clip.mkv", typeof(BlobVideo))]
    [InlineData("docs/report.pdf", typeof(BlobDocument))]
    [InlineData("docs/report.docx", typeof(BlobDocument))]
    [InlineData("docs/budget.xlsx", typeof(BlobSpreadsheet))]
    [InlineData("docs/slides.pptx", typeof(BlobPresentation))]
    [InlineData("mail/inbox.eml", typeof(BlobMessage))]
    [InlineData("web/page.html", typeof(BlobWebPage))]
    [InlineData("data/file.bin", typeof(BlobFile))]
    [InlineData("data/archive.zip", typeof(BlobFile))]
    public async Task HandleAsync_CreatesCorrectBlobFileType(string path, Type expectedType)
    {
        using var stream = new MemoryStream(new byte[] { 0x00 });
        var result = await DefaultBlobHandler.HandleAsync(path, null, stream, null!);

        Assert.NotNull(result);
        Assert.IsType(expectedType, result);
    }

    // === Basic properties filled in ===

    [Fact]
    public async Task HandleAsync_SetsBasicProperties()
    {
        using var stream = new MemoryStream(new byte[1024]);
        var result = await DefaultBlobHandler.HandleAsync("photos/vacation.jpg", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal("photos/vacation.jpg", result.Path);
        Assert.Equal("vacation.jpg", result.Name);
        Assert.Equal("photos", result.FolderPath);
        Assert.Equal("image/jpeg", result.MediaType);
        Assert.Equal(1024, result.ContentLength);
    }

    [Fact]
    public async Task HandleAsync_SetsNameWithoutFolder()
    {
        using var stream = new MemoryStream(new byte[10]);
        var result = await DefaultBlobHandler.HandleAsync("readme.txt", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal("readme.txt", result.Name);
        Assert.Null(result.FolderPath);
    }

    [Fact]
    public async Task HandleAsync_SetsNestedFolderPath()
    {
        using var stream = new MemoryStream(new byte[10]);
        var result = await DefaultBlobHandler.HandleAsync("a/b/c/file.txt", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal("file.txt", result.Name);
        Assert.Equal("a/b/c", result.FolderPath);
    }

    // === Text content extraction ===

    [Theory]
    [InlineData("doc.txt")]
    [InlineData("doc.md")]
    [InlineData("doc.json")]
    [InlineData("doc.xml")]
    [InlineData("doc.csv")]
    [InlineData("doc.html")]
    [InlineData("doc.css")]
    [InlineData("doc.yaml")]
    [InlineData("doc.yml")]
    [InlineData("doc.cs")]
    [InlineData("doc.js")]
    [InlineData("doc.ts")]
    [InlineData("doc.py")]
    [InlineData("doc.sql")]
    [InlineData("doc.sh")]
    public async Task HandleAsync_ExtractsTextContent_ForTextFormats(string fileName)
    {
        var text = "Hello, this is test content!";
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(text));
        var result = await DefaultBlobHandler.HandleAsync(fileName, null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal(text, result.Content);
    }

    [Fact]
    public async Task HandleAsync_DoesNotExtractContent_ForBinaryFormats()
    {
        using var stream = new MemoryStream(new byte[] { 0x89, 0x50, 0x4E, 0x47 }); // PNG header
        var result = await DefaultBlobHandler.HandleAsync("image.png", null, stream, null!);

        Assert.NotNull(result);
        Assert.Null(result.Content);
    }

    [Fact]
    public async Task HandleAsync_SetsContentLength_ForTextFiles()
    {
        var text = "Hello world";
        var bytes = System.Text.Encoding.UTF8.GetBytes(text);
        using var stream = new MemoryStream(bytes);
        var result = await DefaultBlobHandler.HandleAsync("readme.txt", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal(bytes.Length, result.ContentLength);
    }

    [Fact]
    public async Task HandleAsync_SetsContentLength_ForBinaryFiles()
    {
        var data = new byte[2048];
        using var stream = new MemoryStream(data);
        var result = await DefaultBlobHandler.HandleAsync("photo.jpg", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal(2048, result.ContentLength);
    }

    // === Explicit contentType overrides extension ===

    [Fact]
    public async Task HandleAsync_UsesExplicitContentType_OverExtension()
    {
        using var stream = new MemoryStream(new byte[10]);
        var result = await DefaultBlobHandler.HandleAsync("data.bin", "image/png", stream, null!);

        Assert.NotNull(result);
        Assert.Equal("image/png", result.MediaType);
        Assert.IsType<BlobPhoto>(result);
    }

    [Fact]
    public async Task HandleAsync_ExplicitContentType_CreatesCorrectType()
    {
        using var stream = new MemoryStream(new byte[10]);
        var result = await DefaultBlobHandler.HandleAsync("file.dat", "audio/mpeg", stream, null!);

        Assert.NotNull(result);
        Assert.Equal("audio/mpeg", result.MediaType);
        Assert.IsType<BlobMusic>(result);
    }

    [Fact]
    public async Task HandleAsync_FallsBackToExtension_WhenContentTypeIsNull()
    {
        using var stream = new MemoryStream(new byte[10]);
        var result = await DefaultBlobHandler.HandleAsync("photo.jpg", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal("image/jpeg", result.MediaType);
        Assert.IsType<BlobPhoto>(result);
    }

    // === Handler returns null ===

    [Fact]
    public async Task HandleAsync_ReturnsNonNull_ForAllPaths()
    {
        // Default handler always returns a BlobFile, never null
        using var stream = new MemoryStream(new byte[1]);
        var result = await DefaultBlobHandler.HandleAsync("unknown.xyz", null, stream, null!);

        Assert.NotNull(result);
        Assert.IsType<BlobFile>(result);
        Assert.Equal("application/octet-stream", result.MediaType);
    }

    // === Text detection ===

    [Theory]
    [InlineData(".txt", true)]
    [InlineData(".cs", true)]
    [InlineData(".json", true)]
    [InlineData(".html", true)]
    [InlineData(".jpg", false)]
    [InlineData(".pdf", false)]
    [InlineData(".mp3", false)]
    [InlineData(".unknown", false)]
    [InlineData("", false)]
    public void IsTextExtension_ReturnsCorrectResult(string ext, bool expected)
    {
        Assert.Equal(expected, DefaultBlobHandler.IsTextExtension(ext));
    }

    // === ContentLength for non-seekable stream ===

    [Fact]
    public async Task HandleAsync_CalculatesContentLength_FromText_WhenStreamNotSeekable()
    {
        var text = "Hello world";
        var bytes = System.Text.Encoding.UTF8.GetBytes(text);
        // Pipe streams are non-seekable
        var pipe = new System.IO.Pipelines.Pipe();
        await pipe.Writer.WriteAsync(bytes);
        pipe.Writer.Complete();
        var stream = pipe.Reader.AsStream();

        var result = await DefaultBlobHandler.HandleAsync("readme.txt", null, stream, null!);

        Assert.NotNull(result);
        Assert.Equal(text, result.Content);
        Assert.Equal(bytes.Length, result.ContentLength);
    }

    [Fact]
    public async Task HandleAsync_ContentLength_IsNull_ForBinaryNonSeekableStream()
    {
        var pipe = new System.IO.Pipelines.Pipe();
        await pipe.Writer.WriteAsync(new byte[] { 0x89, 0x50, 0x4E, 0x47 });
        pipe.Writer.Complete();
        var stream = pipe.Reader.AsStream();

        var result = await DefaultBlobHandler.HandleAsync("image.png", null, stream, null!);

        Assert.NotNull(result);
        Assert.Null(result.ContentLength); // non-seekable, can't determine length
    }
}
