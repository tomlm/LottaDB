namespace Lotta;

/// <summary>When queued handlers (<see cref="ILottaConfiguration.OnAsync{T}"/>) run.</summary>
public enum AsyncHandlerMode
{
    /// <summary>By themselves, in the background, for as long as the database is open.</summary>
    Auto,

    /// <summary>Only when <see cref="LottaDB.DrainAsync"/> is called.</summary>
    Manual,
}

/// <summary>How one queued handler is retried and how long a run may take.</summary>
public sealed class AsyncHandlerOptions
{
    /// <summary>How many times a failing run is tried before its work is set aside in the poison queue. Default: 5.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>How long one run may take before it is cancelled and counted as a failure. Default: 5 minutes.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long to wait after the first failure before trying again. The wait doubles with each
    /// further failure, up to five minutes. A run that finds the write lock held by another
    /// process waits the same way, counted separately and not as a failure. Default: 2 seconds.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>Where one queued handler stands.</summary>
/// <param name="Name">The handler's name.</param>
/// <param name="Priority">Its priority; higher runs first.</param>
/// <param name="Latest">Whether it runs once for an object's latest state instead of once per save.</param>
/// <param name="Waiting">Messages in its queue, including any being worked on or waiting to be retried. Approximate.</param>
/// <param name="Running">Runs in progress in this process.</param>
/// <param name="Poisoned">Messages set aside after failing too often. Approximate.</param>
/// <param name="LastError">The message of the most recent failure in this process; null after a success.</param>
/// <param name="Paused">Whether it is paused in this process.</param>
public sealed record AsyncHandlerStatus(
    string Name, int Priority, bool Latest, int Waiting, int Running, int Poisoned, string? LastError, bool Paused);
