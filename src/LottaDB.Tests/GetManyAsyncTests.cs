using Lotta.Internal;

namespace Lotta.Tests;

public class GetManyAsyncTests
{
    [Fact]
    public async Task GetManyAsync_NoKeys_ReturnsAllEntities()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_EmptyTable_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(all);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_DeserializesPolymorphically()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Alice", Email = "alice@test.com", Department = "Eng" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person1", Name = "Bob", Email = "bob@test.com" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, all.Count);
        Assert.Contains(all, o => o is Employee);
        Assert.Contains(all, o => o is Person && o is not Employee);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_WithMaxPerPage_ReturnsAll()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 5; i++)
            await db.SaveAsync(new Actor { Domain = "bulk.test", Username = $"user-{i}" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var all = await adapter.GetManyAsync(tableName, maxPerPage: 2,
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(5, all.Count);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_ReturnsOnlyMatchingEntities()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "carol" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "carol" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        var actors = results.Cast<Actor>().OrderBy(a => a.Username).ToList();
        Assert.Equal("alice", actors[0].Username);
        Assert.Equal("carol", actors[1].Username);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_NonExistentKeys_ReturnsOnlyExisting()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "nonexistent" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(results);
        Assert.Equal("alice", ((Actor)results[0]).Username);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_EmptyKeyList_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, Enumerable.Empty<string>(),
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Empty(results);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_DeserializesPolymorphically()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Alice", Email = "alice@test.com", Department = "Eng" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person1", Name = "Bob", Email = "bob@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BaseEntity { Id = "base1", Name = "Carol" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "emp1", "person1" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is Employee e && e.Department == "Eng");
        Assert.Contains(results, o => o is Person p && p.Email == "bob@test.com" && o is not Employee);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_SingleKey_ReturnsSingleResult()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice", DisplayName = "Alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "bob", DisplayName = "Bob" }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "bob" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Single(results);
        Assert.Equal("Bob", ((Actor)results[0]).DisplayName);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_MixedTypes_ReturnsAllRequested()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Domain = "bulk.test", Username = "alice" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Hello", Published = DateTimeOffset.UtcNow }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "alice", "n1" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is Actor);
        Assert.Contains(results, o => o is Note);
    }

    [Fact]
    public async Task GetManyAsync_NoKeys_ReturnsBlobFileEntities()
    {
        using var db = await LottaDBFixture.CreateDbAsync(config => config.OnUpload());
        await db.SaveAsync(new BlobPhoto { Path = "photos/cat.jpg", Name = "cat.jpg", MediaType = "image/jpeg", Width = 1920, Height = 1080 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BlobMusic { Path = "music/song.mp3", Name = "song.mp3", MediaType = "audio/mpeg", Artist = "TestArtist", Album = "TestAlbum" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BlobDocument { Path = "docs/report.pdf", Name = "report.pdf", MediaType = "application/pdf", PageCount = 42 }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, results.Count);
        Assert.Contains(results, o => o is BlobPhoto p && p.Width == 1920);
        Assert.Contains(results, o => o is BlobMusic m && m.Artist == "TestArtist");
        Assert.Contains(results, o => o is BlobDocument d && d.PageCount == 42);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_ReturnsBlobFileEntities()
    {
        using var db = await LottaDBFixture.CreateDbAsync(config => config.OnUpload());
        await db.SaveAsync(new BlobPhoto { Path = "photos/cat.jpg", Name = "cat.jpg", MediaType = "image/jpeg", Width = 1920, Height = 1080 }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BlobMusic { Path = "music/song.mp3", Name = "song.mp3", MediaType = "audio/mpeg", Artist = "TestArtist" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new BlobVideo { Path = "videos/clip.mp4", Name = "clip.mp4", MediaType = "video/mp4", Width = 3840, FrameRate = 60.0 }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "photos/cat.jpg", "videos/clip.mp4" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.Contains(results, o => o is BlobPhoto p && p.Path == "photos/cat.jpg" && p.Width == 1920);
        Assert.Contains(results, o => o is BlobVideo v && v.Path == "videos/clip.mp4" && v.FrameRate == 60.0);
    }

    [Fact]
    public async Task GetManyAsync_WithKeys_BlobPropertiesPreserved()
    {
        using var db = await LottaDBFixture.CreateDbAsync(config => config.OnUpload());
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
        }, TestContext.Current.CancellationToken);

        var (adapter, tableName) = db.GetTableForTesting();
        var results = await adapter.GetManyAsync(tableName, new[] { "photos/vacation.jpg" },
                cancellationToken: TestContext.Current.CancellationToken)
            .ToListAsync(TestContext.Current.CancellationToken);

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
