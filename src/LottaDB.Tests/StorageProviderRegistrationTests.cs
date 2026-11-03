namespace Lotta.Tests;

/// <summary>
/// A catalog has exactly one storage provider: registering a second throws instead of
/// silently replacing part or all of the first.
/// </summary>
public class StorageProviderRegistrationTests
{
    [Fact]
    public void SingleProvider_IsRecorded()
    {
        using var catalog = new LottaCatalog("single", c => c.UseMemory());
        Assert.Equal("Memory", catalog.StorageProvider);
    }

    [Fact]
    public void NoProvider_IsNull()
    {
        using var catalog = new LottaCatalog("none");
        Assert.Null(catalog.StorageProvider);
    }

    [Fact]
    public void SecondProvider_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new LottaCatalog("stacked", c => c.UseMemory().UseLucene()));
        Assert.Contains("Memory", ex.Message);
        Assert.Contains("Lucene", ex.Message);
    }

    [Fact]
    public void SameProviderTwice_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => new LottaCatalog("twice", c => c.UseMemory().UseMemory()));
    }

    [Fact]
    public void ConnectionStringConstructor_RegistersAzure()
    {
        Assert.Throws<InvalidOperationException>(() => new LottaCatalog("azure", "UseDevelopmentStorage=true", c => c.UseMemory()));
    }

    [Fact]
    public void FactoryProperties_CanStillBeOverridden()
    {
        Lucene.Net.Store.Directory? created = null;
        using var catalog = new LottaCatalog("override", c =>
        {
            c.UseMemory();
            c.LuceneDirectoryFactory = path => created = new Lucene.Net.Store.RAMDirectory();
        });

        catalog.LuceneDirectoryFactory("x");
        Assert.NotNull(created);
        Assert.Equal("Memory", catalog.StorageProvider);
    }
}
