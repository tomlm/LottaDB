using Lotta.Tests;

namespace Lotta.Memory.Tests;

internal static class Tests
{
    internal static void Configure(LottaCatalog catalog) => catalog.UseMemory();
}

public class Memory_AdHocJoinTests : AdHocJoinTestBase { public Memory_AdHocJoinTests() : base(Tests.Configure) { } }
public class Memory_AutoQueryableTests : AutoQueryableTestBase { public Memory_AutoQueryableTests() : base(Tests.Configure) { } }
public class Memory_BatchTests : BatchTestBase { public Memory_BatchTests() : base(Tests.Configure) { } }
public class Memory_BlobOnUploadTests : BlobOnUploadTestBase { public Memory_BlobOnUploadTests() : base(Tests.Configure) { } }
public class Memory_BlobTests : BlobTestBase { public Memory_BlobTests() : base(Tests.Configure) { } }
public class Memory_BuilderTests : BuilderTestBase { public Memory_BuilderTests() : base(Tests.Configure) { } }
public class Memory_CascadingViewTests : CascadingViewTestBase { public Memory_CascadingViewTests() : base(Tests.Configure) { } }
public class Memory_ChangeAsyncTests : ChangeAsyncTestBase { public Memory_ChangeAsyncTests() : base(Tests.Configure) { } }
public class Memory_CoverageTests : CoverageTestBase { public Memory_CoverageTests() : base(Tests.Configure) { } }
public class Memory_CreateViewTests : CreateViewTestBase { public Memory_CreateViewTests() : base(Tests.Configure) { } }
public class Memory_CrudTests : CrudTestBase { public Memory_CrudTests() : base(Tests.Configure) { } }
public class Memory_CycleDetectionTests : CycleDetectionTestBase { public Memory_CycleDetectionTests() : base(Tests.Configure) { } }
public class Memory_DatabaseIsolationTests : DatabaseIsolationTestBase { public Memory_DatabaseIsolationTests() : base(Tests.Configure) { } }
public class Memory_DatabaseLifecycleTests : DatabaseLifecycleTestBase { public Memory_DatabaseLifecycleTests() : base(Tests.Configure) { } }
public class Memory_ETagTests : ETagTestBase { public Memory_ETagTests() : base(Tests.Configure) { } }
public class Memory_GetManyAsyncTests : GetManyAsyncTestBase { public Memory_GetManyAsyncTests() : base(Tests.Configure) { } }
public class Memory_JsonExpressionTests : JsonExpressionTestBase { public Memory_JsonExpressionTests() : base(Tests.Configure) { } }
public class Memory_JsonMetadataTests : JsonMetadataTestBase { public Memory_JsonMetadataTests() : base(Tests.Configure) { } }
public class Memory_JsonRoundtripTests : JsonRoundtripTestBase { public Memory_JsonRoundtripTests() : base(Tests.Configure) { } }
public class Memory_LargeObjectTests : LargeObjectTestBase { public Memory_LargeObjectTests() : base(Tests.Configure) { } }
public class Memory_MatchTests : MatchTestBase { public Memory_MatchTests() : base(Tests.Configure) { } }
public class Memory_ObserveTests : ObserveTestBase { public Memory_ObserveTests() : base(Tests.Configure) { } }
public class Memory_PolymorphismTests : PolymorphismTestBase { public Memory_PolymorphismTests() : base(Tests.Configure) { } }
public class Memory_QueryMethodTests : QueryMethodTestBase { public Memory_QueryMethodTests() : base(Tests.Configure) { } }
public class Memory_QueryTests : QueryTestBase { public Memory_QueryTests() : base(Tests.Configure) { } }
public class Memory_RebuildIndexTests : RebuildIndexTestBase { public Memory_RebuildIndexTests() : base(Tests.Configure) { } }
public class Memory_NearRealTimeSearchTests : NearRealTimeSearchTestBase { public Memory_NearRealTimeSearchTests() : base(Tests.Configure) { } }
public class Memory_SearchTests : SearchTestBase { public Memory_SearchTests() : base(Tests.Configure) { } }
public class Memory_StoreRegistrationTests : StoreRegistrationTestBase { public Memory_StoreRegistrationTests() : base(Tests.Configure) { } }
public class Memory_ToAsyncEnumerableTests : ToAsyncEnumerableTestBase { public Memory_ToAsyncEnumerableTests() : base(Tests.Configure) { } }
public class Memory_VectorSearchTests : VectorSearchTestBase { public Memory_VectorSearchTests() : base(Tests.Configure) { } }
