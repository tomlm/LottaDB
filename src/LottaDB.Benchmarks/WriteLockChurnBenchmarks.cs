using BenchmarkDotNet.Attributes;

namespace Lotta.Benchmarks;

/// <summary>
/// Cost of re-acquiring the cross-process Lucene write lock.
///
/// <para>
/// The write lock is taken on the first write and released after
/// <see cref="ILottaConfiguration.WriteLockReleaseDelay"/> of inactivity. A server that writes
/// steadily takes it once and keeps it, so it pays nothing — that is what
/// <see cref="SaveBenchmarks"/> measures. But a server whose writes are spaced further apart
/// than the release delay rebuilds the <c>IndexWriter</c> on every write.
/// </para>
///
/// <para>
/// This measures exactly that difference, so the default release delay can be chosen against a
/// real number rather than a guess. Runs on SQLite (an <c>FSDirectory</c>, so a real
/// <c>write.lock</c> file). On Azure the gap is larger still: the lock is a blob lease, so each
/// acquire/release is a network round trip rather than a local file operation.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class WriteLockChurnBenchmarks
{
    private LottaCatalog _catalog = null!;
    private LottaDB _db = null!;
    private string _tempPath = "";
    private int _counter;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _tempPath = Path.Combine(Path.GetTempPath(), $"LottaChurnBench_{Guid.NewGuid():N}");
        _catalog = new LottaCatalog("WriteLockChurn", c => c.UseSQLite(_tempPath));
        _db = await _catalog.GetDatabaseAsync("bench", config =>
        {
            config.Store<BenchmarkDocument>();
            // Never release on the timer — the benchmarks drive acquisition explicitly so the
            // measurement is not racing a background release.
            config.WriteLockReleaseDelay = -1;
        });
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await _db.DeleteDatabaseAsync();
        _catalog.Dispose();
        try { Directory.Delete(_tempPath, true); } catch { }
    }

    private BenchmarkDocument NextDocument() => new()
    {
        Id = $"doc-{Interlocked.Increment(ref _counter):D6}",
        Title = "Churn",
        Content = "The quick dog jumps over the lazy fence.",
        Counter = _counter,
    };

    /// <summary>One save with the writer already held — the steady-state cost.</summary>
    [IterationSetup(Target = nameof(Save_WriterAlreadyHeld))]
    public void EnsureWriterHeld()
    {
        // Take the writer outside the measured region.
        _db.SaveAsync(NextDocument()).GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task Save_WriterAlreadyHeld()
    {
        await _db.SaveAsync(NextDocument());
    }

    /// <summary>
    /// One save that must re-acquire the write lock and rebuild the IndexWriter, as happens to
    /// any writer whose writes are spaced further apart than WriteLockReleaseDelay.
    /// </summary>
    [IterationSetup(Target = nameof(Save_WriterReacquired))]
    public void ReleaseWriter()
    {
        _db.SaveAsync(NextDocument()).GetAwaiter().GetResult();
        _db.ReleaseWriteLockAsync().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Save_WriterReacquired()
    {
        await _db.SaveAsync(NextDocument());
    }
}
