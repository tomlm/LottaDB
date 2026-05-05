using Tiki.Documents;
using Tiki.Mime;

namespace Lotta.Tiki.Tests;

public class BlobFileMapperTests
{
    private static readonly MediaType TextPlain = new("text", "plain");
    private static readonly MediaType ImageJpeg = new("image", "jpeg");
    private static readonly MediaType ApplicationPdf = new("application", "pdf");

    [Fact]
    public void Name_DerivedFromBlobPath_WhenTikiNameIsNull()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "" };
        var result = BlobFileMapper.FromTikiFile(tiki, "photos/vacation.jpg");

        Assert.Equal("vacation.jpg", result.Name);
    }

    [Fact]
    public void Name_AlwaysDerivedFromBlobPath_EvenWhenTikiNameIsSet()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "", Name = "original.jpg" };
        var result = BlobFileMapper.FromTikiFile(tiki, "photos/vacation.jpg");

        Assert.Equal("vacation.jpg", result.Name);
    }

    [Fact]
    public void FolderPath_DerivedFromBlobPath_WhenTikiFolderPathIsNull()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "" };
        var result = BlobFileMapper.FromTikiFile(tiki, "photos/2024/vacation.jpg");

        Assert.Equal("photos/2024", result.FolderPath);
    }

    [Fact]
    public void FolderPath_AlwaysDerivedFromBlobPath_EvenWhenTikiFolderPathIsSet()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "", FolderPath = "/original/path" };
        var result = BlobFileMapper.FromTikiFile(tiki, "photos/vacation.jpg");

        Assert.Equal("photos", result.FolderPath);
    }

    [Fact]
    public void FolderPath_IsNull_WhenBlobPathHasNoFolder()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "" };
        var result = BlobFileMapper.FromTikiFile(tiki, "readme.txt");

        Assert.Null(result.FolderPath);
    }

    [Fact]
    public void Path_AlwaysSetToBlobPath()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "", Path = "/some/other/path" };
        var result = BlobFileMapper.FromTikiFile(tiki, "docs/report.pdf");

        Assert.Equal("docs/report.pdf", result.Path);
    }

    [Fact]
    public void Name_DerivedFromBlobPath_WhenPathIsDeeplyNested()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "" };
        var result = BlobFileMapper.FromTikiFile(tiki, "a/b/c/d/file.txt");

        Assert.Equal("file.txt", result.Name);
        Assert.Equal("a/b/c/d", result.FolderPath);
    }

    [Fact]
    public void BaseProperties_CopiedFromTikiFile()
    {
        var tiki = new TikiFile
        {
            MediaType = ApplicationPdf,
            Content = "extracted text",
            Title = "My Report",
            Authors = new[] { "Alice", "Bob" },
            Description = "A test report",
            DateCreated = new DateTime(2025, 1, 15),
            DateModified = new DateTime(2025, 3, 20),
            ContentLength = 54321,
            Keywords = new[] { "test", "report" },
        };

        var result = BlobFileMapper.FromTikiFile(tiki, "docs/report.pdf");

        Assert.Equal("application/pdf", result.MediaType);
        Assert.Equal("extracted text", result.Content);
        Assert.Equal("My Report", result.Title);
        Assert.Equal(new[] { "Alice", "Bob" }, result.Authors);
        Assert.Equal("A test report", result.Description);
        Assert.Equal(new DateTime(2025, 1, 15), result.DateCreated);
        Assert.Equal(new DateTime(2025, 3, 20), result.DateModified);
        Assert.Equal(54321, result.ContentLength);
        Assert.Equal(new[] { "test", "report" }, result.Keywords);
    }

    [Fact]
    public void Content_SetToNull_WhenTikiContentIsEmpty()
    {
        var tiki = new TikiFile { MediaType = TextPlain, Content = "" };
        var result = BlobFileMapper.FromTikiFile(tiki, "file.bin");

        Assert.Null(result.Content);
    }

    [Fact]
    public void TikiPhoto_MapsToBlobPhoto()
    {
        var tiki = new TikiPhoto
        {
            MediaType = ImageJpeg,
            Content = "",
            CameraManufacturer = "Canon",
            CameraModel = "EOS R5",
            IsoSpeed = 400,
            Width = 8192,
            Height = 5464,
            Latitude = 47.6062,
            Longitude = -122.3321,
        };

        var result = BlobFileMapper.FromTikiFile(tiki, "photos/test.jpg");

        var photo = Assert.IsType<BlobPhoto>(result);
        Assert.Equal("Canon", photo.CameraManufacturer);
        Assert.Equal("EOS R5", photo.CameraModel);
        Assert.Equal(400, photo.IsoSpeed);
        Assert.Equal(8192, photo.Width);
        Assert.Equal(5464, photo.Height);
        Assert.Equal(47.6062, photo.Latitude);
        Assert.Equal(-122.3321, photo.Longitude);
        Assert.Equal("photos/test.jpg", photo.Path);
        Assert.Equal("test.jpg", photo.Name);
        Assert.Equal("photos", photo.FolderPath);
    }

    [Fact]
    public void TikiDocument_MapsToBlobDocument()
    {
        var tiki = new TikiDocument
        {
            MediaType = ApplicationPdf,
            Content = "page text",
            PageCount = 42,
            WordCount = 15000,
            Company = "Acme Corp",
        };

        var result = BlobFileMapper.FromTikiFile(tiki, "docs/annual-report.pdf");

        var doc = Assert.IsType<BlobDocument>(result);
        Assert.Equal(42, doc.PageCount);
        Assert.Equal(15000, doc.WordCount);
        Assert.Equal("Acme Corp", doc.Company);
        Assert.Equal("annual-report.pdf", doc.Name);
        Assert.Equal("docs", doc.FolderPath);
    }

    [Fact]
    public void TikiMusic_MapsToBlobMusic()
    {
        var tiki = new TikiMusic
        {
            MediaType = new MediaType("audio", "mpeg"),
            Content = "",
            Artist = "Pink Floyd",
            Album = "Dark Side of the Moon",
            TrackNumber = 3,
            Year = 1973,
            Duration = TimeSpan.FromMinutes(3.5),
        };

        var result = BlobFileMapper.FromTikiFile(tiki, "music/time.mp3");

        var music = Assert.IsType<BlobMusic>(result);
        Assert.Equal("Pink Floyd", music.Artist);
        Assert.Equal("Dark Side of the Moon", music.Album);
        Assert.Equal(3, music.TrackNumber);
        Assert.Equal(1973, music.Year);
        Assert.Equal(210.0, music.DurationSeconds);
        Assert.Equal("time.mp3", music.Name);
    }

    [Fact]
    public void UnknownTikiType_MapsToBlobFile()
    {
        var tiki = new TikiFile { MediaType = new MediaType("application", "octet-stream"), Content = "" };
        var result = BlobFileMapper.FromTikiFile(tiki, "data/unknown.bin");

        Assert.IsType<BlobFile>(result);
        Assert.Equal("application/octet-stream", result.MediaType);
        Assert.Equal("unknown.bin", result.Name);
        Assert.Equal("data", result.FolderPath);
    }
}
