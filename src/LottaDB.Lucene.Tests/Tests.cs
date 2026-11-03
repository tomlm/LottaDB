using Lotta.Tests;

namespace Lotta.Lucene.Tests;

/// <summary>
/// The shared suite with Lucene as the only storage: UseLucene() over in-memory directories,
/// and over the file system for the tests that reopen storage.
/// </summary>
internal static class Tests
{
    internal static void Configure(LottaCatalog catalog) => catalog.UseLucene();
    internal static void ConfigureFileSystem(LottaCatalog catalog) => catalog.UseLucene(LuceneDirectories.FileSystem(TestRun.GetTempFolder()));
}

public class Lucene_AdHocJoinTests : AdHocJoinTestBase { public Lucene_AdHocJoinTests() : base(Tests.Configure) { } }
public class Lucene_AutoQueryableTests : AutoQueryableTestBase { public Lucene_AutoQueryableTests() : base(Tests.Configure) { } }
public class Lucene_BatchTests : BatchTestBase { public Lucene_BatchTests() : base(Tests.Configure) { } }
public class Lucene_BlobOnUploadTests : BlobOnUploadTestBase { public Lucene_BlobOnUploadTests() : base(Tests.Configure) { } }
public class Lucene_BlobTests : BlobTestBase { public Lucene_BlobTests() : base(Tests.Configure) { } }
public class Lucene_BuilderTests : BuilderTestBase { public Lucene_BuilderTests() : base(Tests.Configure) { } }
public class Lucene_CascadingViewTests : CascadingViewTestBase { public Lucene_CascadingViewTests() : base(Tests.Configure) { } }
public class Lucene_ChangeAsyncTests : ChangeAsyncTestBase { public Lucene_ChangeAsyncTests() : base(Tests.Configure) { } }
public class Lucene_CoverageTests : CoverageTestBase { public Lucene_CoverageTests() : base(Tests.Configure) { } }
public class Lucene_CreateViewTests : CreateViewTestBase { public Lucene_CreateViewTests() : base(Tests.Configure) { } }
public class Lucene_CrudTests : CrudTestBase { public Lucene_CrudTests() : base(Tests.Configure) { } }
public class Lucene_CycleDetectionTests : CycleDetectionTestBase { public Lucene_CycleDetectionTests() : base(Tests.Configure) { } }
public class Lucene_DatabaseIsolationTests : DatabaseIsolationTestBase { public Lucene_DatabaseIsolationTests() : base(Tests.Configure) { } }
public class Lucene_DatabaseLifecycleTests : DatabaseLifecycleTestBase { public Lucene_DatabaseLifecycleTests() : base(Tests.Configure) { } }
public class Lucene_ETagTests : ETagTestBase { public Lucene_ETagTests() : base(Tests.Configure) { } }
public class Lucene_GetManyAsyncTests : GetManyAsyncTestBase { public Lucene_GetManyAsyncTests() : base(Tests.Configure) { } }
public class Lucene_JsonExpressionTests : JsonExpressionTestBase { public Lucene_JsonExpressionTests() : base(Tests.Configure) { } }
public class Lucene_JsonMetadataTests : JsonMetadataTestBase { public Lucene_JsonMetadataTests() : base(Tests.Configure) { } }
public class Lucene_JsonRoundtripTests : JsonRoundtripTestBase { public Lucene_JsonRoundtripTests() : base(Tests.Configure) { } }
public class Lucene_LargeObjectTests : LargeObjectTestBase { public Lucene_LargeObjectTests() : base(Tests.Configure) { } }
public class Lucene_MatchTests : MatchTestBase { public Lucene_MatchTests() : base(Tests.Configure) { } }
public class Lucene_MultiValuedFieldTests : MultiValuedFieldTestBase { public Lucene_MultiValuedFieldTests() : base(Tests.Configure) { } }
public class Lucene_ObserveTests : ObserveTestBase { public Lucene_ObserveTests() : base(Tests.Configure) { } }
public class Lucene_PolymorphismTests : PolymorphismTestBase { public Lucene_PolymorphismTests() : base(Tests.Configure) { } }
public class Lucene_QueryMethodTests : QueryMethodTestBase { public Lucene_QueryMethodTests() : base(Tests.Configure) { } }
public class Lucene_QueryScoreTests : QueryScoreTestBase { public Lucene_QueryScoreTests() : base(Tests.Configure) { } }
public class Lucene_QueryTests : QueryTestBase { public Lucene_QueryTests() : base(Tests.Configure) { } }
public class Lucene_RebuildIndexTests : RebuildIndexTestBase { public Lucene_RebuildIndexTests() : base(Tests.Configure) { } }
public class Lucene_NearRealTimeSearchTests : NearRealTimeSearchTestBase { public Lucene_NearRealTimeSearchTests() : base(Tests.Configure) { } }
public class Lucene_SearchTests : SearchTestBase { public Lucene_SearchTests() : base(Tests.Configure) { } }
public class Lucene_StoreRegistrationTests : StoreRegistrationTestBase { public Lucene_StoreRegistrationTests() : base(Tests.Configure) { } }
public class Lucene_ToAsyncEnumerableTests : ToAsyncEnumerableTestBase { public Lucene_ToAsyncEnumerableTests() : base(Tests.Configure) { } }
public class Lucene_VectorSearchTests : VectorSearchTestBase { public Lucene_VectorSearchTests() : base(Tests.Configure) { } }
public class Lucene_WriteLockTests : WriteLockTestBase { public Lucene_WriteLockTests() : base(Tests.Configure) { } }

public class LuceneFileSystem_CrudTests : CrudTestBase { public LuceneFileSystem_CrudTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_ChangeAsyncTests : ChangeAsyncTestBase { public LuceneFileSystem_ChangeAsyncTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_BatchTests : BatchTestBase { public LuceneFileSystem_BatchTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_DatabaseLifecycleTests : DatabaseLifecycleTestBase { public LuceneFileSystem_DatabaseLifecycleTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_DatabaseIsolationTests : DatabaseIsolationTestBase { public LuceneFileSystem_DatabaseIsolationTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_ETagTests : ETagTestBase { public LuceneFileSystem_ETagTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_JsonMetadataTests : JsonMetadataTestBase { public LuceneFileSystem_JsonMetadataTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_BlobTests : BlobTestBase { public LuceneFileSystem_BlobTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_BlobOnUploadTests : BlobOnUploadTestBase { public LuceneFileSystem_BlobOnUploadTests() : base(Tests.ConfigureFileSystem) { } }
public class LuceneFileSystem_RebuildIndexTests : RebuildIndexTestBase { public LuceneFileSystem_RebuildIndexTests() : base(Tests.ConfigureFileSystem) { } }

/// <summary>Cross-process write-lock contention with the index as the only shared storage.</summary>
public class Lucene_MultiProcessLockTests : MultiProcessLockTests
{
    protected override bool IndexIsStorage => true;

    [Fact]
    public void EachProcess_HasItsOwnLuceneStorage()
    {
        var (c1, c2) = CreateSharedCatalogs();
        using var _1 = c1;
        using var _2 = c2;
        var t1 = Assert.IsType<LuceneTableServiceClient>(c1.GetTableServiceClient());
        var t2 = Assert.IsType<LuceneTableServiceClient>(c2.GetTableServiceClient());
        Assert.NotSame(t1, t2);
    }
}
