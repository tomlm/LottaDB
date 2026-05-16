using Lotta.Internal;

namespace Lotta.Tests;

public class GetManyAsyncTests
{
    [Fact]
    public async Task GetManyAsync_NoKeys_ReturnsAllEntities()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_EmptyTable_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Empty(all);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_DeserializesPolymorphically()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Alice", Email = "alice@test.com", Department = "Eng" }, ct);
        await db.SaveAsync(new Person { Id = "person1", Name = "Bob", Email = "bob@test.com" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(2, all.Count);
        Assert.Contains(all, o => o is Employee);
        Assert.Contains(all, o => o is Person && o is not Employee);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_WithMaxPerPage_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 0; i < 5; i++)
            await db.SaveAsync(new Actor { Domain = "bulk.test", Username = $"user-{i}" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, maxPerPage: 2,
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(5, all.Count);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_ReturnsOnlyMatchingEntities()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob" }, ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "carol" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "carol" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(2, results.Count);
        var actors = results.Cast<Actor>().OrderBy(a => a.Username).ToList();
        Assert.Equal("alice", actors[0].Username);
        Assert.Equal("carol", actors[1].Username);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_NonExistentKeys_ReturnsOnlyExisting()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "nonexistent" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Single(results);
        Assert.Equal("alice", ((Actor)results[0]).Username);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_EmptyKeyList_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, Enumerable.Empty<string>(),
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Empty(results);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_DeserializesPolymorphically()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Alice", Email = "alice@test.com", Department = "Eng" }, ct);
        await db.SaveAsync(new Person { Id = "person1", Name = "Bob", Email = "bob@test.com" }, ct);
        await db.SaveAsync(new BaseEntity { Id = "base1", Name = "Carol" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "emp1", "person1" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is Employee e && e.Department == "Eng");
        Assert.Contains(results, o => o is Person p && p.Email == "bob@test.com" && o is not Employee);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_SingleKey_ReturnsSingleResult()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob", DisplayName = "Bob" }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "bob" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Single(results);
        Assert.Equal("Bob", ((Actor)results[0]).DisplayName);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_MixedTypes_ReturnsAllRequested()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "n1" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is Actor);
        Assert.Contains(results, o => o is Note);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_ReturnsBlobFileEntities()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config => config.OnUpload(), cancellationToken: ct);
        await db.SaveAsync(new BlobPhoto { Path = "photos/cat.jpg", Name = "cat.jpg", MediaType = "image/jpeg", Width = 1920, Height = 1080 }, ct);
        await db.SaveAsync(new BlobMusic { Path = "music/song.mp3", Name = "song.mp3", MediaType = "audio/mpeg", Artist = "TestArtist", Album = "TestAlbum" }, ct);
        await db.SaveAsync(new BlobDocument { Path = "docs/report.pdf", Name = "report.pdf", MediaType = "application/pdf", PageCount = 42 }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(3, results.Count);
        Assert.Contains(results, o => o is BlobPhoto p && p.Width == 1920);
        Assert.Contains(results, o => o is BlobMusic m && m.Artist == "TestArtist");
        Assert.Contains(results, o => o is BlobDocument d && d.PageCount == 42);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_ReturnsBlobFileEntities()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config => config.OnUpload(), cancellationToken: ct);
        await db.SaveAsync(new BlobPhoto { Path = "photos/cat.jpg", Name = "cat.jpg", MediaType = "image/jpeg", Width = 1920, Height = 1080 }, ct);
        await db.SaveAsync(new BlobMusic { Path = "music/song.mp3", Name = "song.mp3", MediaType = "audio/mpeg", Artist = "TestArtist" }, ct);
        await db.SaveAsync(new BlobVideo { Path = "videos/clip.mp4", Name = "clip.mp4", MediaType = "video/mp4", Width = 3840, FrameRate = 60.0 }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "photos/cat.jpg", "videos/clip.mp4" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is BlobPhoto p && p.Path == "photos/cat.jpg" && p.Width == 1920);
        Assert.Contains(results, o => o is BlobVideo v && v.Path == "videos/clip.mp4" && v.FrameRate == 60.0);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_BlobPropertiesPreserved()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(config => config.OnUpload(), cancellationToken: ct);
        await db.SaveAsync(new BlobPhoto
        {
            Path = "photos/vacation.jpg",
            Name = "vacation.jpg",
            FolderPath = "photos",
            MediaType = "image/jpeg",
            ContentLength = 5_000_000,
            Width = 4032,
            Height = 3024,
            CameraManufacturer = "Canon",
            CameraModel = "EOS R5",
            IsoSpeed = 400,
            FNumber = 2.8,
            FocalLength = 50.0,
            DateTaken = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc)
        }, ct);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "photos/vacation.jpg" },
                cancellationToken: ct)
            .ToListAsync(ct);

        Assert.Single(results);
        var photo = Assert.IsType<BlobPhoto>(results[0]);
        Assert.Equal("photos/vacation.jpg", photo.Path);
        Assert.Equal("vacation.jpg", photo.Name);
        Assert.Equal("photos", photo.FolderPath);
        Assert.Equal("image/jpeg", photo.MediaType);
        Assert.Equal(5_000_000, photo.ContentLength);
        Assert.Equal(4032, photo.Width);
        Assert.Equal(3024, photo.Height);
        Assert.Equal("Canon", photo.CameraManufacturer);
        Assert.Equal("EOS R5", photo.CameraModel);
        Assert.Equal(400, photo.IsoSpeed);
        Assert.Equal(2.8, photo.FNumber);
        Assert.Equal(50.0, photo.FocalLength);
        Assert.Equal(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc), photo.DateTaken);
    }
}
