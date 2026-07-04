using Lotta.Tests;

namespace Lotta.Azure.Tests;

internal static class Tests
{
    private const string ConnectionString = "UseDevelopmentStorage=true";

    internal static void Configure(LottaCatalog catalog) => catalog.UseAzure(ConnectionString);
}

public class Azure_AdHocJoinTests : AdHocJoinTestBase { public Azure_AdHocJoinTests() : base(Tests.Configure) { } }
public class Azure_AutoQueryableTests : AutoQueryableTestBase { public Azure_AutoQueryableTests() : base(Tests.Configure) { } }
public class Azure_BatchTests : BatchTestBase { public Azure_BatchTests() : base(Tests.Configure) { } }
public class Azure_BlobOnUploadTests : BlobOnUploadTestBase { public Azure_BlobOnUploadTests() : base(Tests.Configure) { } }
public class Azure_BlobTests : BlobTestBase { public Azure_BlobTests() : base(Tests.Configure) { } }
public class Azure_BuilderTests : BuilderTestBase { public Azure_BuilderTests() : base(Tests.Configure) { } }
public class Azure_CascadingViewTests : CascadingViewTestBase { public Azure_CascadingViewTests() : base(Tests.Configure) { } }
public class Azure_ChangeAsyncTests : ChangeAsyncTestBase { public Azure_ChangeAsyncTests() : base(Tests.Configure) { } }
public class Azure_CoverageTests : CoverageTestBase { public Azure_CoverageTests() : base(Tests.Configure) { } }
public class Azure_CreateViewTests : CreateViewTestBase { public Azure_CreateViewTests() : base(Tests.Configure) { } }
public class Azure_CrudTests : CrudTestBase { public Azure_CrudTests() : base(Tests.Configure) { } }
public class Azure_CycleDetectionTests : CycleDetectionTestBase { public Azure_CycleDetectionTests() : base(Tests.Configure) { } }
public class Azure_DatabaseIsolationTests : DatabaseIsolationTestBase { public Azure_DatabaseIsolationTests() : base(Tests.Configure) { } }
public class Azure_DatabaseLifecycleTests : DatabaseLifecycleTestBase { public Azure_DatabaseLifecycleTests() : base(Tests.Configure) { } }
public class Azure_ETagTests : ETagTestBase { public Azure_ETagTests() : base(Tests.Configure) { } }
public class Azure_GetManyAsyncTests : GetManyAsyncTestBase { public Azure_GetManyAsyncTests() : base(Tests.Configure) { } }
public class Azure_JsonExpressionTests : JsonExpressionTestBase { public Azure_JsonExpressionTests() : base(Tests.Configure) { } }
public class Azure_JsonMetadataTests : JsonMetadataTestBase { public Azure_JsonMetadataTests() : base(Tests.Configure) { } }
public class Azure_JsonRoundtripTests : JsonRoundtripTestBase { public Azure_JsonRoundtripTests() : base(Tests.Configure) { } }
public class Azure_LargeObjectTests : LargeObjectTestBase { public Azure_LargeObjectTests() : base(Tests.Configure) { } }
public class Azure_MatchTests : MatchTestBase { public Azure_MatchTests() : base(Tests.Configure) { } }
public class Azure_ObserveTests : ObserveTestBase { public Azure_ObserveTests() : base(Tests.Configure) { } }
public class Azure_PolymorphismTests : PolymorphismTestBase { public Azure_PolymorphismTests() : base(Tests.Configure) { } }
public class Azure_QueryMethodTests : QueryMethodTestBase { public Azure_QueryMethodTests() : base(Tests.Configure) { } }
public class Azure_QueryTests : QueryTestBase { public Azure_QueryTests() : base(Tests.Configure) { } }
public class Azure_RebuildIndexTests : RebuildIndexTestBase { public Azure_RebuildIndexTests() : base(Tests.Configure) { } }
public class Azure_NearRealTimeSearchTests : NearRealTimeSearchTestBase { public Azure_NearRealTimeSearchTests() : base(Tests.Configure) { } }
public class Azure_SearchTests : SearchTestBase { public Azure_SearchTests() : base(Tests.Configure) { } }
public class Azure_StoreRegistrationTests : StoreRegistrationTestBase { public Azure_StoreRegistrationTests() : base(Tests.Configure) { } }
public class Azure_ToAsyncEnumerableTests : ToAsyncEnumerableTestBase { public Azure_ToAsyncEnumerableTests() : base(Tests.Configure) { } }
public class Azure_VectorSearchTests : VectorSearchTestBase { public Azure_VectorSearchTests() : base(Tests.Configure) { } }
