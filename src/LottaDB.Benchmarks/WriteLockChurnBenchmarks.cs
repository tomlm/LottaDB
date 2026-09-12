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
public abstract class WriteLockChurnBenchmarksBase
{
    private LottaCatalog _catalog = null!;
    private LottaDB _db = null!;
    private int _counter;

    /// <summary>Provider-specific catalog configuration.</summary>
    protected abstract void Configure(LottaCatalog catalog);

    /// <summary>Called after the catalog is disposed, for any provider-specific cleanup.</summary>
    protected virtual void Cleanup() { }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        _catalog = new LottaCatalog(GetType().Name, Configure);
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
        Cleanup();
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

/// <summary>
/// Churn cost on SQLite — the Lucene index is a local <c>FSDirectory</c>, so the write lock is
/// an OS file lock and re-acquiring it is local file I/O.
/// </summary>
public class SQLite_WriteLockChurnBenchmarks : WriteLockChurnBenchmarksBase
{
    private readonly string _tempPath =
        Path.Combine(Path.GetTempPath(), $"LottaChurnBench_{Guid.NewGuid():N}");

    protected override void Configure(LottaCatalog catalog) => catalog.UseSQLite(_tempPath);

    protected override void Cleanup()
    {
        try { Directory.Delete(_tempPath, true); } catch { }
    }
}

/// <summary>
/// Churn cost on Azure — the index lives in blob storage via <c>AzureDirectory</c>, so the write
/// lock is a blob lease and re-acquiring it is a network round trip, on top of syncing segment
/// files through the local cache.
///
/// <para>
/// Requires Azurite (or real Azure Storage). Note that Azurite is a local emulator, so these
/// numbers <b>understate</b> real Azure: the lease acquire/release round trips are sub-millisecond
/// here and tens of milliseconds against the real service.
/// </para>
/// </summary>
public class Azure_WriteLockChurnBenchmarks : WriteLockChurnBenchmarksBase
{
    protected override void Configure(LottaCatalog catalog) =>
        catalog.UseAzure("UseDevelopmentStorage=true");
}
