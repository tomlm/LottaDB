using System.IO.Pipelines;

namespace Lotta;

/// <summary>
/// A read-only stream wrapper that tees all bytes read from the source to a <see cref="PipeWriter"/>.
/// As the consumer (e.g. blob upload) reads from this stream, the same bytes are pushed
/// concurrently to the pipe for a second consumer (e.g. content parser).
/// No full-file buffering is required — memory usage is bounded by the pipe's buffer size.
/// On any failure, the pipe is completed with the exception so the reader doesn't hang.
/// </summary>
internal sealed class TeeStream : Stream
{
    private readonly Stream _source;
    private readonly PipeWriter _pipeWriter;
    private bool _completed;

    public TeeStream(Stream source, PipeWriter pipeWriter)
    {
        _source = source;
        _pipeWriter = pipeWriter;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _source.Length;
    public override long Position
    {
        get => _source.Position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException("Use ReadAsync. Synchronous reads are not supported to avoid deadlocks with the pipe.");

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        try
        {
            int bytesRead = await _source.ReadAsync(buffer, offset, count, cancellationToken);
            if (bytesRead > 0)
            {
                await _pipeWriter.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, bytesRead), cancellationToken);
                await _pipeWriter.FlushAsync(cancellationToken);
            }
            else
            {
                Complete();
            }
            return bytesRead;
        }
        catch (Exception ex)
        {
            Complete(ex);
            throw;
        }
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        try
        {
            int bytesRead = await _source.ReadAsync(buffer, cancellationToken);
            if (bytesRead > 0)
            {
                await _pipeWriter.WriteAsync(buffer.Slice(0, bytesRead), cancellationToken);
                await _pipeWriter.FlushAsync(cancellationToken);
            }
            else
            {
                Complete();
            }
            return bytesRead;
        }
        catch (Exception ex)
        {
            Complete(ex);
            throw;
        }
    }

    private void Complete(Exception? exception = null)
    {
        if (!_completed)
        {
            _completed = true;
            _pipeWriter.Complete(exception);
        }
    }

    public override void Flush() { }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Complete();
        }
        base.Dispose(disposing);
    }
}
