using Lotta.Internal;

namespace Lotta.Tests;

public class ODataFilterValidationTests
{
    [Theory]
    [InlineData("Age gt 20")]
    [InlineData("Name eq 'Alice'")]
    [InlineData("Age gt 20 and Name eq 'Alice'")]
    [InlineData("(Age gt 20 and Age lt 30) or Name eq 'Bob'")]
    [InlineData("Score ge 4.5")]
    [InlineData("Active eq true")]
    public void ValidFilter_DoesNotThrow(string filter)
    {
        TableStorageAdapter.ValidateODataFilter(filter);
    }

    [Theory]
    [InlineData("PartitionKey eq 'other'")]
    [InlineData("partitionkey eq 'other'")]
    [InlineData("Age gt 20 and PartitionKey eq 'other'")]
    public void Filter_WithPartitionKey_Throws(string filter)
    {
        Assert.Throws<ArgumentException>(() => TableStorageAdapter.ValidateODataFilter(filter));
    }

    [Theory]
    [InlineData("RowKey eq 'some-key'")]
    [InlineData("rowkey eq 'some-key'")]
    [InlineData("Age gt 20 and RowKey ne 'x'")]
    public void Filter_WithRowKey_Throws(string filter)
    {
        Assert.Throws<ArgumentException>(() => TableStorageAdapter.ValidateODataFilter(filter));
    }

    [Theory]
    [InlineData("Type eq 'SomeType'")]
    [InlineData("type eq 'SomeType'")]
    [InlineData("Age gt 20 and Type eq 'Other'")]
    [InlineData("Type ne '_dynamic_Person'")]
    [InlineData("(Type eq 'SomeType')")]
    [InlineData("Age gt 20 and (Type eq 'Other' or Age lt 10)")]
    public void Filter_WithType_Throws(string filter)
    {
        Assert.Throws<ArgumentException>(() => TableStorageAdapter.ValidateODataFilter(filter));
    }

    [Fact]
    public void Filter_WithTypeInPropertyName_DoesNotThrow()
    {
        // "ContentType" contains "Type" but not "Type eq" — should be allowed
        TableStorageAdapter.ValidateODataFilter("ContentType eq 'text/plain'");
    }
}
