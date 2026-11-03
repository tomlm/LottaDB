using Azure;
using Azure.Storage.Blobs.Models;
using Lucene.Net.Codecs;
using Lucene.Net.Store;
using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// The blobs of one container, stored as files in a Lucene <see cref="LuceneDirectory"/>.
/// <para>
/// Lucene directories are flat and their file names may be case-folded by the file system, so
/// each blob is named by a hash of its blob name; the blob name itself lives in the file header.
/// A blob has two slots (<c>{hash}.a.blob</c> and <c>{hash}.b.blob</c>): an upload writes the
/// slot that is not current and only then deletes the old one, so a failed upload leaves the
/// previous content intact. A slot is valid once its checksum footer is written.
/// </para>
/// <para>
/// File layout: codec header, name, content type, ETag, created, last modified, generation,
/// metadata, content bytes, checksum footer.
/// </para>
/// </summary>
internal sealed class LuceneBlobStore
{
    private const string Codec = "LottaBlob";
    private const int Version = 0;
    private const string Extension = ".blob";
    private static readonly string[] Slots = ["a", "b"];

    private readonly Lazy<LuceneDirectory> _directory;

    // Serializes the read-check-write of each blob within this process.
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new();

    public LuceneBlobStore(Func<LuceneDirectory> directoryFactory)
    {
        _directory = new Lazy<LuceneDirectory>(directoryFactory);
    }

    private LuceneDirectory Directory => _directory.Value;

    /// <summary>A blob's properties, as read from its file header.</summary>
    public sealed record Header(
        string Name,
        string ContentType,
        ETag ETag,
        DateTimeOffset CreatedOn,
        DateTimeOffset LastModified,
        long Generation,
        IDictionary<string, string> Metadata,
        long ContentLength);

    public async Task<Header> UploadAsync(string name, Stream content, BlobHttpHeaders? httpHeaders,
        IDictionary<string, string>? metadata, BlobRequestConditions? conditions, CancellationToken cancellationToken)
    {
        var hash = Hash(name);
        using var gate = await EnterAsync(hash, cancellationToken).ConfigureAwait(false);

        var current = FindCurrent(hash, name);
        if (current != null && conditions?.IfNoneMatch == ETag.All)
            throw new RequestFailedException(409, "The specified blob already exists.", "BlobAlreadyExists", null);
        CheckIfMatch(current?.header, conditions);

        var target = FileName(hash, current?.slot == Slots[0] ? Slots[1] : Slots[0]);
        var now = DateTimeOffset.UtcNow;
        var header = new Header(
            name,
            httpHeaders?.ContentType ?? "application/octet-stream",
            new ETag($"\"{Guid.NewGuid():N}\""),
            current?.header.CreatedOn ?? now,
            now,
            (current?.header.Generation ?? 0) + 1,
            metadata != null ? new Dictionary<string, string>(metadata) : new Dictionary<string, string>(),
            0);

        long length = 0;
        try
        {
            using (var output = Directory.CreateOutput(target, IOContext.DEFAULT))
            {
                WriteHeader(output, header);
                var buffer = ArrayPool<byte>.Shared.Rent(81920);
                try
                {
                    int read;
                    while ((read = await content.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
                    {
                        output.WriteBytes(buffer, 0, read);
                        length += read;
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
                CodecUtil.WriteFooter(output);
            }
            Directory.Sync([target]);
        }
        catch
        {
            TryDelete(target);
            throw;
        }

        if (current != null)
            TryDelete(FileName(hash, current.Value.slot));
        return header with { ContentLength = length };
    }

    /// <summary>Read a blob's properties and content, or null if it does not exist.</summary>
    public async Task<(Header header, byte[] content)?> ReadAsync(string name, BlobRequestConditions? conditions, CancellationToken cancellationToken)
    {
        var hash = Hash(name);
        using var gate = await EnterAsync(hash, cancellationToken).ConfigureAwait(false);

        var current = FindCurrent(hash, name);
        if (current == null)
            return null;
        CheckIfMatch(current.Value.header, conditions);

        using var input = Directory.OpenChecksumInput(FileName(hash, current.Value.slot), IOContext.READ_ONCE);
        var header = ReadHeader(input, input.Length);
        var content = new byte[header.ContentLength];
        input.ReadBytes(content, 0, content.Length);
        CodecUtil.CheckFooter(input);
        return (header, content);
    }

    /// <summary>Read a blob's properties, or null if it does not exist.</summary>
    public async Task<Header?> GetPropertiesAsync(string name, CancellationToken cancellationToken)
    {
        var hash = Hash(name);
        using var gate = await EnterAsync(hash, cancellationToken).ConfigureAwait(false);
        return FindCurrent(hash, name)?.header;
    }

    /// <summary>Delete a blob. Returns false if it did not exist.</summary>
    public async Task<bool> DeleteAsync(string name, BlobRequestConditions? conditions, CancellationToken cancellationToken)
    {
        var hash = Hash(name);
        using var gate = await EnterAsync(hash, cancellationToken).ConfigureAwait(false);

        var current = FindCurrent(hash, name);
        if (current == null)
            return false;
        CheckIfMatch(current.Value.header, conditions);

        foreach (var slot in Slots)
            TryDelete(FileName(hash, slot));
        return true;
    }

    /// <summary>All blobs whose name starts with <paramref name="prefix"/>, ordered by name.</summary>
    public IReadOnlyList<Header> List(string? prefix)
    {
        var headers = new List<Header>();
        foreach (var hash in ListHashes())
        {
            if (FindCurrent(hash, name: null) is { } current
                && (prefix == null || current.header.Name.StartsWith(prefix, StringComparison.Ordinal)))
                headers.Add(current.header);
        }
        headers.Sort((x, y) => string.CompareOrdinal(x.Name, y.Name));
        return headers;
    }

    /// <summary>Delete every blob. Returns false if there were none.</summary>
    public bool DeleteAll()
    {
        var any = false;
        foreach (var file in Directory.ListAll().Where(f => f.EndsWith(Extension, StringComparison.Ordinal)))
        {
            TryDelete(file);
            any = true;
        }
        return any;
    }

    private IEnumerable<string> ListHashes()
        => Directory.ListAll()
            .Where(f => f.EndsWith(Extension, StringComparison.Ordinal))
            .Select(f => f[..f.IndexOf('.')])
            .Distinct(StringComparer.Ordinal);

    /// <summary>
    /// The live slot of a blob: the valid slot, or — if a crash left both valid — the newer one.
    /// When <paramref name="name"/> is given, a slot holding a different name (a hash collision)
    /// does not count.
    /// </summary>
    private (string slot, Header header)? FindCurrent(string hash, string? name)
    {
        (string slot, Header header)? current = null;
        foreach (var slot in Slots)
        {
            var header = TryReadHeader(FileName(hash, slot));
            if (header == null || (name != null && header.Name != name))
                continue;
            if (current == null || header.Generation > current.Value.header.Generation)
                current = (slot, header);
        }
        return current;
    }

    private Header? TryReadHeader(string fileName)
    {
        try
        {
            if (!Directory.FileExists(fileName))
                return null;
            using var input = Directory.OpenInput(fileName, IOContext.READ_ONCE);
            CodecUtil.RetrieveChecksum(input); // throws unless the footer is complete
            input.Seek(0);
            return ReadHeader(input, input.Length);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            // Missing, partially written, or deleted while we looked.
            return null;
        }
    }

    private static void WriteHeader(IndexOutput output, Header header)
    {
        CodecUtil.WriteHeader(output, Codec, Version);
        output.WriteString(header.Name);
        output.WriteString(header.ContentType);
        output.WriteString(header.ETag.ToString());
        output.WriteInt64(header.CreatedOn.UtcTicks);
        output.WriteInt64(header.LastModified.UtcTicks);
        output.WriteInt64(header.Generation);
        output.WriteStringStringMap(header.Metadata);
    }

    private static Header ReadHeader(IndexInput input, long fileLength)
    {
        CodecUtil.CheckHeader(input, Codec, Version, Version);
        var name = input.ReadString();
        var contentType = input.ReadString();
        var etag = new ETag(input.ReadString());
        var createdOn = new DateTimeOffset(input.ReadInt64(), TimeSpan.Zero);
        var lastModified = new DateTimeOffset(input.ReadInt64(), TimeSpan.Zero);
        var generation = input.ReadInt64();
        var metadata = input.ReadStringStringMap();
        var contentLength = fileLength - input.Position - CodecUtil.FooterLength();
        return new Header(name, contentType, etag, createdOn, lastModified, generation, metadata, contentLength);
    }

    private static void CheckIfMatch(Header? current, BlobRequestConditions? conditions)
    {
        var ifMatch = conditions?.IfMatch;
        if (ifMatch == null)
            return;
        if (current == null || (ifMatch != ETag.All && ifMatch != current.ETag))
            throw new RequestFailedException(412, "The condition specified using HTTP conditional header(s) is not met.", "ConditionNotMet", null);
    }

    private void TryDelete(string fileName)
    {
        try
        {
            if (Directory.FileExists(fileName))
                Directory.DeleteFile(fileName);
        }
        catch (IOException)
        {
            // Already gone.
        }
    }

    private async Task<IDisposable> EnterAsync(string hash, CancellationToken cancellationToken)
    {
        var gate = _gates.GetOrAdd(hash, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new Releaser(gate);
    }

    private sealed class Releaser(SemaphoreSlim gate) : IDisposable
    {
        private int _released;
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                gate.Release();
        }
    }

    private static string FileName(string hash, string slot) => $"{hash}.{slot}{Extension}";

    /// <summary>A file-system-safe, case-insensitive-safe name for a blob.</summary>
    private static string Hash(string name)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(name)), 0, 16).ToLowerInvariant();
}
