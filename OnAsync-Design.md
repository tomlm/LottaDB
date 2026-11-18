# `OnAsync<T>`: queued handlers

Status: implemented for every built-in storage provider: Memory, SQLite, FileSystem, Lucene and Azure (see *Storage*).

## Summary

`On<T>` runs a handler inside the save, under the write lease. That suits quick, derived work. It does not suit slow work: a model call of a few seconds would block every other write for that long, a failure is recorded but never retried, and nothing reruns a handler after a crash.

`OnAsync<T>` registers a handler that LottaDB runs later, from a queue, outside the write lease, and retries until it succeeds.

```csharp
config.OnAsync<Mail>("describe", async (mail, kind, db, ct) =>
{
    if (mail.Summary != null) return;                       // already done: handlers must be safe to repeat
    var summary = await describer.SummarizeAsync(mail, ct);
    await db.ChangeAsync<Mail>(mail.Id, m => { m.Summary = summary; return m; }, ct);
}, priority: 10, latest: true);
```

| | `On<T>` | `OnAsync<T>` |
| --- | --- | --- |
| Runs | Inside the save | Later, from a queue |
| Write lease | Held while it runs | Not held; taken only for the handler's own writes |
| Failure | Returned in `ObjectResult` | Retried with back-off, then set aside in a poison queue |
| After a crash | Lost | Runs when the database is next opened |
| Order | The order of saves | By priority, then first in, first out |
| Deletes | Delivered | Not delivered |

## How it works

Every save of an object sends one small message to the queue of each matching handler. The message holds only what is needed to trigger the handler: the object's type and key. It never holds the object, so an object's size does not matter.

A pump takes messages by priority, fetches the object as it is at that moment, and runs the handler on it. Success deletes the message. Failure keeps it, hidden for a wait that doubles each time, until it has failed `MaxAttempts` times and is moved to the handler's poison queue.

The queue is the only stored state. There are no extra table rows.

## Registration

```csharp
ILottaConfiguration OnAsync<T>(string name, EntityHandler<T> handler, int priority = 0,
    bool latest = true, AsyncHandlerOptions? options = null) where T : class, new();
```

- **`name`** identifies the handler across runs of the program, and its queue is named after it: 3 to 40 lower-case letters and digits, with single hyphens between them. A duplicate name throws.
- **`handler`** is the delegate type `On<T>` uses. It is always called with `TriggerKind.Saved`.
- **`priority`**: higher runs first.
- **`latest`**: whether only an object's latest state matters.
- **`options`**: `MaxAttempts` (5), `Timeout` for one run (5 minutes), `RetryDelay` before the first retry (2 seconds, doubling, at most 5 minutes).

A handler registered for a base type also receives derived types, from every save path. `JsonDocument` objects are not delivered.

## Priority

- Each handler has its own queue.
- The pump takes its next message from the highest priority that has one ready. Lower priorities run only while every higher one is empty.
- Within a handler, messages are taken first in, first out.
- Among handlers of equal priority, the pump takes the oldest message. Azure records a message's time to the second, so there the order across handlers is only that fine.

## Latest

**`latest: true`** (the default) means the handler is done with an object once it has run on the object's latest state. Several saves before the handler gets to the object give one run.

Each open database keeps, in memory, the newest message it has sent per handler and object. A message is dropped without running the handler only when this database instance knows it has since sent a newer one for the same handler and object. That newer message was sent after the older one's save, so whenever it runs it sees that save, and the queue carries it to success, retry or poison on its own account.

| Where the newer message is when the older one is received | The older message |
| --- | --- |
| Waiting or running | Dropped |
| Finished here (the older one was a retry) | Runs: an extra run on the current object |
| Finished by another process | Dropped: the newer one ran after the older save |
| Poisoned | Runs: a second attempt, and the failure stays visible |

The note can only err towards an extra run. After a restart, or for messages another process sent, there is no note and every message runs.

**`latest: false`** runs the handler once for every save. Each run still gets the object as it is when the run starts, not as it was at that save.

Either way, a handler that is expensive should check whether its work is already done and return.

## What the handler is given

The object is fetched when the handler runs, as its concrete type, with its current ETag. A write-back with `SaveAsync` throws `ConcurrencyException` only if the object changes during the run. `SaveManyAsync` ignores ETags and gives no such check.

A message whose object no longer exists is dropped. Deletes are not queued: `On<T>` remains the way to react to a delete.

## Loop prevention

- **A save made inside a queued handler queues only handlers of lower priority** than the one running. A handler therefore never queues itself, and handlers cannot queue one another in a circle.
- **A write that changes nothing queues nothing.** `ChangeAsync` whose mutation leaves the object identical to what is stored now writes nothing at all: no table write, no `On<T>` handlers, no reindex. `SaveAsync` of an object that is unchanged since it was loaded still writes, but queues nothing.
- **A hop limit** (`AsyncHandlerMaxHops`, default 16) is the backstop for chains across databases. A message past the limit goes to the poison queue without running.

## The pump

- One per open, writable database that has queued handlers. It starts when the catalog has fully opened the database and kept it, never for a read-only one.
- The catalog hands out a database only once it is fully open, handlers started. If any step of opening fails (a schema rebuild, the manifest, creating the queues), the half-open database is disposed and nothing is kept, so the next `GetDatabaseAsync` opens it again from scratch. Callers asking for the database while it is being opened share that one open. The open runs under the catalog's token, not any one caller's, and each caller waits with its own: one that cancels stops waiting without failing the open for the others. An open that every caller has given up on still finishes and is kept. An open still in progress when the catalog is disposed or deleted keeps nothing: its database is disposed before its pump starts, and the open throws `ObjectDisposedException`, or after a deletion `InvalidOperationException` (the next `GetDatabaseAsync` opens it again).
- `config.AsyncHandlers = AsyncHandlerMode.Manual` leaves it stopped; handlers then run only in `DrainAsync`.
- One handler run at a time per database.
- A run holds its message with a short lease, renewed while it runs. A run that loses its lease, or outlasts `Timeout`, is cancelled and counts as a failure.
- Disposing the database cancels the token of the run in progress, in `Auto` and `Manual` mode alike: the pump's run, or a `DrainAsync`'s (whose handlers get a token linked to both the caller's token and the database's). It waits up to 10 seconds in all for the run to end before tearing the database down. A run stopped this way, or by cancelling the drain, is given back at once with no failure counted, whatever it threw; `DrainAsync` then throws `OperationCanceledException`, even when the run stopped was the last one its `max` allowed.
- A run that could not get the write lock (`WriteLockUnavailableException`: another process is writing) counts no failure either, so waiting on another process never sets work aside. But it is given back hidden for `RetryDelay`, doubling with each such run of the message up to five minutes, as failures are; given back at once, it would be run again and again while the lock is held.
- Delivery is at least once.

## Operating it

```csharp
Task<int> DrainAsync(int? max = null, CancellationToken ct = default);   // run until nothing is ready, or max runs
void PauseHandlers(string? name = null);
void ResumeHandlers(string? name = null);
Task<IReadOnlyList<AsyncHandlerStatus>> GetHandlerStatusAsync(CancellationToken ct = default);
Task<int> RetryPoisonedAsync(string name, CancellationToken ct = default);
Task<int> RequeueAsync(string name, CancellationToken ct = default);     // queue the handler for every stored object of its type
```

`DrainAsync` cannot be called from inside a handler or a save. Work waiting out a retry delay, after a failure or after finding the write lock held, is not ready and is left for later.

`RequeueAsync` is how work is redone after a handler changes (a new prompt or model). It reads every object in the database.

## Storage

- `LottaCatalog.UseStorage(provider, tables, blobs, queues, directories)` takes a queue factory. Every built-in `Use…()` provider passes one.
- Registering an `OnAsync<T>` handler on a catalog with no queue storage throws `NotSupportedException` when the database is opened, before anything is created.
- **FileSystem:** `UseFileSystem` passes the queue client of `Iciclecreek.Azure.Storage.FileSystem`. It needs 2.1.0 or later: the 2.0.4 client is not first in, first out and can hand one message to two receivers.
- **Lucene:** `UseLucene` passes the queue client of `Iciclecreek.Azure.Storage.Lucene`, over the same directory factory as everything else. Each queue is a Lucene `Directory` of its own (`queues/{queue}`) with a file per message; no index is involved. A message's file name is about 100 characters. Lucene.Net syncs each file through its own call to Windows, which fails past 260 characters unless the program is declared long-path aware, so `LuceneDirectories.FileSystem(rootPath)` opens each `FSDirectory` by its extended path (`\\?\C:\…`). A factory of your own should do the same, or a send to a deep folder fails and is reported in the save's `Errors`.
- Queue names are `l{catalog hash}{database hash}-{handler}` and `…-poison`, since queue names allow only lower-case letters, digits and hyphens. The readable names are in each queue's metadata.
- `ResetDatabaseAsync` empties the database's queues; `DeleteDatabaseAsync` and deleting the catalog delete them.
- `RebuildSearchIndex` queues nothing.

## Known limits

- A save in the instant before a crash may not be queued, the same window in which `On<T>` and indexing are lost today.
- `latest: true` collapses saves only within one running database instance.
- Sends for a batch save go one at a time.
- Not tested: a `latest` message arriving from another process.

## Left out on purpose

- Collapsing saves across restarts and processes.
- Handing the handler the object as it was at each save.
- Queuing deletes.
- Detecting a changed handler automatically.
- Batch handlers, and more than one run at a time.
