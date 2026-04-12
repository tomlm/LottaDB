namespace LottaDB;

public interface IBuilderFailureSink
{
    Task ReportAsync(BuilderError error, CancellationToken ct = default);
}
