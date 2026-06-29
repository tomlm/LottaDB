using Lotta.Tests;

namespace Lotta.FileSystem.Tests;

internal static class Tests
{
    internal static void Configure(LottaCatalog catalog) => catalog.UseFileSystem(TestRun.GetTempFolder());
}

public class FileSystem_AdHocJoinTests : AdHocJoinTestBase { public FileSystem_AdHocJoinTests() : base(Tests.Configure) { } }
public class FileSystem_AutoQueryableTests : AutoQueryableTestBase { public FileSystem_AutoQueryableTests() : base(Tests.Configure) { } }
public class FileSystem_BatchTests : BatchTestBase { public FileSystem_BatchTests() : base(Tests.Configure) { } }
public class FileSystem_BlobOnUploadTests : BlobOnUploadTestBase { public FileSystem_BlobOnUploadTests() : base(Tests.Configure) { } }
public class FileSystem_BlobTests : BlobTestBase { public FileSystem_BlobTests() : base(Tests.Configure) { } }
public class FileSystem_BuilderTests : BuilderTestBase { public FileSystem_BuilderTests() : base(Tests.Configure) { } }
public class FileSystem_CascadingViewTests : CascadingViewTestBase { public FileSystem_CascadingViewTests() : base(Tests.Configure) { } }
public class FileSystem_ChangeAsyncTests : ChangeAsyncTestBase { public FileSystem_ChangeAsyncTests() : base(Tests.Configure) { } }
public class FileSystem_CoverageTests : CoverageTestBase { public FileSystem_CoverageTests() : base(Tests.Configure) { } }
public class FileSystem_CreateViewTests : CreateViewTestBase { public FileSystem_CreateViewTests() : base(Tests.Configure) { } }
public class FileSystem_CrudTests : CrudTestBase { public FileSystem_CrudTests() : base(Tests.Configure) { } }
public class FileSystem_CycleDetectionTests : CycleDetectionTestBase { public FileSystem_CycleDetectionTests() : base(Tests.Configure) { } }
public class FileSystem_DatabaseIsolationTests : DatabaseIsolationTestBase { public FileSystem_DatabaseIsolationTests() : base(Tests.Configure) { } }
public class FileSystem_DatabaseLifecycleTests : DatabaseLifecycleTestBase { public FileSystem_DatabaseLifecycleTests() : base(Tests.Configure) { } }
public class FileSystem_ETagTests : ETagTestBase { public FileSystem_ETagTests() : base(Tests.Configure) { } }
public class FileSystem_GetManyAsyncTests : GetManyAsyncTestBase { public FileSystem_GetManyAsyncTests() : base(Tests.Configure) { } }
public class FileSystem_JsonExpressionTests : JsonExpressionTestBase { public FileSystem_JsonExpressionTests() : base(Tests.Configure) { } }
public class FileSystem_JsonMetadataTests : JsonMetadataTestBase { public FileSystem_JsonMetadataTests() : base(Tests.Configure) { } }
public class FileSystem_JsonRoundtripTests : JsonRoundtripTestBase { public FileSystem_JsonRoundtripTests() : base(Tests.Configure) { } }
public class FileSystem_LargeObjectTests : LargeObjectTestBase { public FileSystem_LargeObjectTests() : base(Tests.Configure) { } }
public class FileSystem_MatchTests : MatchTestBase { public FileSystem_MatchTests() : base(Tests.Configure) { } }
public class FileSystem_ObserveTests : ObserveTestBase { public FileSystem_ObserveTests() : base(Tests.Configure) { } }
public class FileSystem_PolymorphismTests : PolymorphismTestBase { public FileSystem_PolymorphismTests() : base(Tests.Configure) { } }
public class FileSystem_QueryMethodTests : QueryMethodTestBase { public FileSystem_QueryMethodTests() : base(Tests.Configure) { } }
public class FileSystem_QueryTests : QueryTestBase { public FileSystem_QueryTests() : base(Tests.Configure) { } }
public class FileSystem_RebuildIndexTests : RebuildIndexTestBase { public FileSystem_RebuildIndexTests() : base(Tests.Configure) { } }
public class FileSystem_SearchTests : SearchTestBase { public FileSystem_SearchTests() : base(Tests.Configure) { } }
public class FileSystem_StoreRegistrationTests : StoreRegistrationTestBase { public FileSystem_StoreRegistrationTests() : base(Tests.Configure) { } }
public class FileSystem_ToAsyncEnumerableTests : ToAsyncEnumerableTestBase { public FileSystem_ToAsyncEnumerableTests() : base(Tests.Configure) { } }
public class FileSystem_VectorSearchTests : VectorSearchTestBase { public FileSystem_VectorSearchTests() : base(Tests.Configure) { } }
