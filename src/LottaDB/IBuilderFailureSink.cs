namespace LottaDB;

/// <summary>
/// Receives reports of builder failures for retry, alerting, or logging.
/// Builder errors do not block the source object's save — they are captured
/// in <see cref="ObjectResult.Errors"/> and reported to this sink.
/// </summary>
public interface IBuilderFailureSink
{
    /// <summary>Called when a builder fails during a write operation.</summary>
    Task ReportAsync(BuilderError error, CancellationToken ct = default);
}
