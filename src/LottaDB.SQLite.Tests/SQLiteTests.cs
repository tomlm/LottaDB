using Lotta.Tests;

namespace Lotta.SQLite.Tests;

internal static class Tests
{
    // This class is intentionally left empty. It serves as a container for the test classes that follow.
    internal static void Configure(LottaCatalog catalog) => catalog.UseSQLite(TestRun.GetTempFolder());
}

public class SQLite_AdHocJoinTests : AdHocJoinTestBase { public SQLite_AdHocJoinTests() : base(Tests.Configure) { } }
public class SQLite_AutoQueryableTests : AutoQueryableTestBase { public SQLite_AutoQueryableTests() : base(Tests.Configure) { } }
public class SQLite_BatchTests : BatchTestBase { public SQLite_BatchTests() : base(Tests.Configure) { } }
public class SQLite_BlobOnUploadTests : BlobOnUploadTestBase { public SQLite_BlobOnUploadTests() : base(Tests.Configure) { } }
public class SQLite_BlobTests : BlobTestBase { public SQLite_BlobTests() : base(Tests.Configure) { } }
public class SQLite_BuilderTests : BuilderTestBase { public SQLite_BuilderTests() : base(Tests.Configure) { } }
public class SQLite_CascadingViewTests : CascadingViewTestBase { public SQLite_CascadingViewTests() : base(Tests.Configure) { } }
public class SQLite_ChangeAsyncTests : ChangeAsyncTestBase { public SQLite_ChangeAsyncTests() : base(Tests.Configure) { } }
public class SQLite_CoverageTests : CoverageTestBase { public SQLite_CoverageTests() : base(Tests.Configure) { } }
public class SQLite_CreateViewTests : CreateViewTestBase { public SQLite_CreateViewTests() : base(Tests.Configure) { } }
public class SQLite_CrudTests : CrudTestBase { public SQLite_CrudTests() : base(Tests.Configure) { } }
public class SQLite_CycleDetectionTests : CycleDetectionTestBase { public SQLite_CycleDetectionTests() : base(Tests.Configure) { } }
public class SQLite_DatabaseIsolationTests : DatabaseIsolationTestBase { public SQLite_DatabaseIsolationTests() : base(Tests.Configure) { } }
public class SQLite_DatabaseLifecycleTests : DatabaseLifecycleTestBase { public SQLite_DatabaseLifecycleTests() : base(Tests.Configure) { } }
public class SQLite_ETagTests : ETagTestBase { public SQLite_ETagTests() : base(Tests.Configure) { } }
public class SQLite_GetManyAsyncTests : GetManyAsyncTestBase { public SQLite_GetManyAsyncTests() : base(Tests.Configure) { } }
public class SQLite_JsonExpressionTests : JsonExpressionTestBase { public SQLite_JsonExpressionTests() : base(Tests.Configure) { } }
public class SQLite_JsonMetadataTests : JsonMetadataTestBase { public SQLite_JsonMetadataTests() : base(Tests.Configure) { } }
public class SQLite_JsonRoundtripTests : JsonRoundtripTestBase { public SQLite_JsonRoundtripTests() : base(Tests.Configure) { } }
public class SQLite_LargeObjectTests : LargeObjectTestBase { public SQLite_LargeObjectTests() : base(Tests.Configure) { } }
public class SQLite_MatchTests : MatchTestBase { public SQLite_MatchTests() : base(Tests.Configure) { } }
public class SQLite_ObserveTests : ObserveTestBase { public SQLite_ObserveTests() : base(Tests.Configure) { } }
public class SQLite_PolymorphismTests : PolymorphismTestBase { public SQLite_PolymorphismTests() : base(Tests.Configure) { } }
public class SQLite_QueryMethodTests : QueryMethodTestBase { public SQLite_QueryMethodTests() : base(Tests.Configure) { } }
public class SQLite_QueryTests : QueryTestBase { public SQLite_QueryTests() : base(Tests.Configure) { } }
public class SQLite_RebuildIndexTests : RebuildIndexTestBase { public SQLite_RebuildIndexTests() : base(Tests.Configure) { } }
public class SQLite_NearRealTimeSearchTests : NearRealTimeSearchTestBase { public SQLite_NearRealTimeSearchTests() : base(Tests.Configure) { } }
public class SQLite_SearchTests : SearchTestBase { public SQLite_SearchTests() : base(Tests.Configure) { } }
public class SQLite_StoreRegistrationTests : StoreRegistrationTestBase { public SQLite_StoreRegistrationTests() : base(Tests.Configure) { } }
public class SQLite_ToAsyncEnumerableTests : ToAsyncEnumerableTestBase { public SQLite_ToAsyncEnumerableTests() : base(Tests.Configure) { } }
public class SQLite_VectorSearchTests : VectorSearchTestBase { public SQLite_VectorSearchTests() : base(Tests.Configure) { } }
