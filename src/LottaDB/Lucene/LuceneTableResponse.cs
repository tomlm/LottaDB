using Azure;
using Azure.Core;
using System.Diagnostics.CodeAnalysis;

namespace Lotta;

/// <summary>
/// In-process <see cref="Response"/> returned by <see cref="LuceneTableClient"/> and the Lucene
/// blob clients. Carries the status code and, for table writes, the new ETag header that LottaDB
/// reads back.
/// </summary>
internal sealed class LuceneTableResponse : Response
{
    private readonly List<HttpHeader> _headers = new();

    private LuceneTableResponse(int status, string reasonPhrase)
    {
        Status = status;
        ReasonPhrase = reasonPhrase;
    }

    public override int Status { get; }
    public override string ReasonPhrase { get; }
    public override Stream? ContentStream { get; set; }
    public override string ClientRequestId { get; set; } = Guid.NewGuid().ToString();
    public override void Dispose() => ContentStream?.Dispose();

    protected override bool ContainsHeader(string name)
        => _headers.Any(h => string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase));

    protected override IEnumerable<HttpHeader> EnumerateHeaders() => _headers;

    protected override bool TryGetHeader(string name, [NotNullWhen(true)] out string? value)
    {
        value = _headers.FirstOrDefault(h => string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase)).Value;
        return value != null;
    }

    protected override bool TryGetHeaderValues(string name, [NotNullWhen(true)] out IEnumerable<string>? values)
    {
        var list = _headers.Where(h => string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase)).Select(h => h.Value).ToArray();
        values = list.Length > 0 ? list : null;
        return values != null;
    }

    public static Response Ok() => new LuceneTableResponse(200, "OK");
    public static Response Created() => new LuceneTableResponse(201, "Created");
    public static Response Accepted() => new LuceneTableResponse(202, "Accepted");
    public static Response NotFound() => new LuceneTableResponse(404, "Not Found");

    public static Response NoContent(ETag? etag = null)
    {
        var response = new LuceneTableResponse(204, "No Content");
        // HTTP ETag headers are quoted (RFC 7232); ResponseHeaders.ETag strips the quotes again.
        if (etag != null)
            response._headers.Add(new HttpHeader("ETag", $"\"{etag.Value}\""));
        return response;
    }
}

/// <summary>A <see cref="NullableResponse{T}"/> for a row that does not exist.</summary>
internal sealed class LuceneTableNoValue<T> : NullableResponse<T>
{
    public override bool HasValue => false;
    public override T? Value => default;
    public override Response GetRawResponse() => LuceneTableResponse.NotFound();
}
