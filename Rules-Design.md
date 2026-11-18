# Rules: inline and queued

Status: implemented for every built-in storage provider: Memory, SQLite, FileSystem, Lucene and Azure (see *Storage*). Replaced `On<T>` and `OnAsync<T>` in 6.0.

## Summary

A rule says what to do when an object is saved or deleted. Rules are like database triggers, except that they never roll back the save (an inline rule's exception goes into the save's `ObjectResult.Errors` and the object stays saved) and they can also queue durable work. Rules live under `Rules`, on the configuration (`config.Rules`) and on an open database (`db.Rules`). A rule names a type and, optionally, a filter, then takes one or more actions:

- **`.Do(handler)`** runs inside the save, under the write lease. That suits quick, derived work.
- **`.Queue(name, handler)`** runs later, from a queue, outside the write lease, and is retried until it succeeds. That suits slow work: a model call of a few seconds inside the save would block every other write for that long, a failure would be recorded but never retried, and nothing would rerun it after a crash.

```csharp
config.Rules.When<Mail>(mail => mail.SummarizedBody != mail.Body)   // only mail whose summary is out of date
      .Queue(Describe).Priority(10);                                  // queue "describe"

static async Task Describe(Mail mail, LottaDB db, CancellationToken ct)
{
    var summary = await describer.SummarizeAsync(mail, ct);   // slow: outside the write lease
    await db.ChangeAsync<Mail>(mail.Id, m =>
    {
        if (m.Body == mail.Body)                               // still the mail that was summarized
        {
            m.Summary = summary;
            m.SummarizedBody = mail.Body;
        }
        return m;
    }, ct);
}
```

| | `.Do` | `.Queue` |
| --- | --- | --- |
| Runs | Inside the save | Later, from a queue |
| Write lease | Held while it runs | Not held; taken only for the handler's own writes |
| Failure | Returned in `ObjectResult` | Retried with back-off, then set aside in a poison queue |
| After a crash | Lost | Runs when the database is next opened |
| Order | The order of saves | By priority, then first in, first out |
| Deletes | `WhenDeleted<T>().Do(...)` | Not available (see *Left out on purpose*) |
| On an open database | `db.Rules.When<T>().Do(...)`, removable | Configuration only |

## Registration

```csharp
public delegate Task RuleHandler<in T>(T item, LottaDB db, CancellationToken cancellationToken);

// config.Rules (RuleSet)
RuleBuilder<T>        When<T>(Expression<Func<T, bool>>? filter = null);
DeletedRuleBuilder<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null);

// RuleBuilder<T>
RuleBuilder<T>       Do(RuleHandler<T> handler);
QueuedRuleBuilder<T> Queue(string name, RuleHandler<T> handler);
QueuedRuleBuilder<T> Queue(RuleHandler<T> handler, [CallerArgumentExpression] string? handlerExpression = null);

// QueuedRuleBuilder<T>: modifiers for the queue just added, and Do/Queue to add more actions
QueuedRuleBuilder<T> Priority(int priority);                  // default 0
QueuedRuleBuilder<T> EverySave();                             // default: latest state only
QueuedRuleBuilder<T> Retry(int maxAttempts, TimeSpan delay);  // defaults 5 and 2 s
QueuedRuleBuilder<T> Timeout(TimeSpan timeout);               // default 5 minutes

// DeletedRuleBuilder<T>
DeletedRuleBuilder<T> Do(RuleHandler<T> handler);

// db.Rules (LiveRuleSet): inline only; Do returns an IDisposable that removes the rule
LiveRule<T>        When<T>(Expression<Func<T, bool>>? filter = null);
LiveDeletedRule<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null);
```

```csharp
config.Rules.When<Note>(n => n.IsPublished)
      .Do(UpdateFeedView)
      .Queue("notify-followers", Notify).Priority(5);
config.Rules.When<Note>().Queue("audit", Audit).EverySave();
config.Rules.WhenDeleted<Note>().Do(RemoveFeedView);
```

- **A rule registers when `When` is called.** One rule may have several actions (`.Do(a).Queue("b", h)`), and they run in the order added. A rule with no action makes opening the database throw `InvalidOperationException` naming the type. Rules are read when the database opens; adding to one, or calling a modifier, after that throws `InvalidOperationException` instead of silently doing nothing.
- **Modifiers** apply to the queued action most recently added by `.Queue`, and check their values at the call: `Retry` needs `maxAttempts >= 1` and `delay >= 0`, `Timeout` needs more than zero.
- **The filter** says which objects the rule is for. Without it, every object of the type. It is an expression, compiled once when the rule is made; every check uses the compiled delegate. The expression itself is kept on the queued registration for `RequeueAsync`.
  - **For `.Do`:** checked before the handler.
  - **For `.Queue`, checked on save:** a save it is false for queues nothing, which saves the message, the run and the read of the object.
  - **For `.Queue`, checked again when the run comes up,** on the object as it then is. A run it has become false for is skipped, not counted as a run, and its message is deleted. `RequeueAsync` queues only the objects it is true for.
  - **Quick and pure:** it runs inside the save, under the write lease, so it should look only at the object. To react to a change, compare against a marker the handler writes, such as `SummarizedBody` above. A save does not read the stored version first, so "what changed" is not available.
  - **When it throws:** on save, the error is in the save's `Errors` and the save still happens (for `.Queue`, nothing is queued). When a queued run comes up, it counts as a failed run.
- **Queue names** identify the work across runs of the program, and its queue is named after it: 3 to 40 lower-case letters and digits, with single hyphens between them, not ending in `-poison`. A duplicate name in one configuration throws.
- **Default queue names.** `.Queue(handler)` takes the compile-time source text of the handler argument (`[CallerArgumentExpression]`), keeps its last identifier, drops an `Async` suffix and kebab-cases it: `Summarize` gives `summarize`, `handlers.NotifyFollowersAsync` gives `notify-followers`, `Handle<Note>` gives `handle`. Anything that is not an identifier or member access (a lambda, a call, a cast) throws `ArgumentException` asking for `.Queue(name, handler)`. Renaming the method renames the queue; see *Orphaned queues*.
- **The handler's own saves** never queue the handler itself: inside a queued run, only lower priorities are queued (see *Loop prevention*).

**Inside an inline rule, search already reflects the change that triggered it.** The built-in handlers (the per-type Lucene index handler, and those for `JsonDocument` and `JsonSchema`) are kept apart from rules and run first: for a save of a `BlobPhoto`, the index handlers of `BlobPhoto` and of `BlobFile` all run before any rule for either type, on saves and deletes and through every save and delete path. Built-in handlers are not visible through `Rules`, take no filter, and cannot be removed by a live rule's handle.

A rule for a base type also fires for derived types, from every save path (`When<BlobFile>` fires for an uploaded `BlobPhoto`). `JsonDocument` objects are not delivered to queued rules.

## How queuing works

Every save of an object sends one small message to the queue of each matching queued action. The message holds only what is needed to trigger the handler: the object's type and key. It never holds the object, so an object's size does not matter.

A pump takes messages by priority, fetches the object as it is at that moment, and runs the handler on it. Success deletes the message. Failure keeps it, hidden for a wait that doubles each time, until it has failed `maxAttempts` times and is moved to the queue's poison queue.

The queue is the only stored state. There are no extra table rows.

## Priority

- Each queued action has its own queue.
- The pump takes its next message from the highest priority that has one ready. Lower priorities run only while every higher one is empty.
- Within a queue, messages are taken first in, first out.
- Among queues of equal priority, the pump takes the oldest message. Azure records a message's time to the second, so there the order across queues is only that fine.

## Latest

**By default** a queued action is done with an object once it has run on the object's latest state. Several saves before the run comes up give one run.

Each open database keeps, in memory, the newest message it has sent per queue and object. A message is dropped without running the handler only when this database instance knows it has since sent a newer one for the same queue and object. That newer message was sent after the older one's save, so whenever it runs it sees that save, and the queue carries it to success, retry or poison on its own account.

| Where the newer message is when the older one is received | The older message |
| --- | --- |
| Waiting or running | Dropped |
| Finished here (the older one was a retry) | Runs: an extra run on the current object |
| Finished by another process | Dropped: the newer one ran after the older save |
| Poisoned | Runs: a second attempt, and the failure stays visible |

The note can only err towards an extra run. After a restart, or for messages another process sent, there is no note and every message runs.

**`.EverySave()`** runs the handler once for every save. Each run still gets the object as it is when the run starts, not as it was at that save.

Either way, a handler that is expensive should check whether its work is already done and return.

## What the handler is given

The object is fetched when the handler runs, as its concrete type, with its current ETag. A write-back with `SaveAsync` throws `ConcurrencyException` only if the object changes during the run. `SaveManyAsync` ignores ETags and gives no such check.

A message whose object no longer exists is dropped. Deletes are not queued: `WhenDeleted<T>().Do(...)` is the way to react to a delete.

## Loop prevention

- **A save made inside a queued run queues only actions of lower priority** than the one running. A queued action therefore never queues itself, and queued actions cannot queue one another in a circle.
- **A write that changes nothing queues nothing.** `ChangeAsync` whose mutation leaves the object identical to what is stored now writes nothing at all: no table write, no inline rules, no reindex. `SaveAsync` of an object that is unchanged since it was loaded still writes, but queues nothing.
- **A hop limit** (`MaxQueueHops`, default 16) is the backstop for chains across databases. A message past the limit goes to the poison queue without running.
- Inline rules keep their own guard: a chain of `.Do` rules stops when it reaches a type already being handled in the chain.

## The pump

- One per open, writable database that has queued rules. It starts when the catalog has fully opened the database and kept it, never for a read-only one.
- The catalog hands out a database only once it is fully open, queues started. If any step of opening fails (a schema rebuild, the manifest, creating the queues), the half-open database is disposed and nothing is kept, so the next `GetDatabaseAsync` opens it again from scratch. Callers asking for the database while it is being opened share that one open. The open runs under the catalog's token, not any one caller's, and each caller waits with its own: one that cancels stops waiting without failing the open for the others. An open that every caller has given up on still finishes and is kept. An open still in progress when the catalog is disposed or deleted keeps nothing: its database is disposed before its pump starts, and the open throws `ObjectDisposedException`, or after a deletion `InvalidOperationException` (the next `GetDatabaseAsync` opens it again).
- `config.QueueMode = QueueMode.Manual` leaves it stopped; queued work then runs only in `DrainAsync`.
- One run at a time per database.
- A run holds its message with a short lease, renewed while it runs. A run that loses its lease, or outlasts its `Timeout`, is cancelled and counts as a failure.
- Disposing the database cancels the token of the run in progress, in `Auto` and `Manual` mode alike: the pump's run, or a `DrainAsync`'s (whose handlers get a token linked to both the caller's token and the database's). It waits up to 10 seconds in all for the run to end before tearing the database down. A run stopped this way, or by cancelling the drain, is given back at once with no failure counted, whatever it threw; `DrainAsync` then throws `OperationCanceledException`, even when the run stopped was the last one its `max` allowed.
- A run that could not get the write lock (`WriteLockUnavailableException`: another process is writing) counts no failure either, so waiting on another process never sets work aside. But it is given back hidden for the retry delay, doubling with each such run of the message up to five minutes, as failures are; given back at once, it would be run again and again while the lock is held.
- Delivery is at least once.

## Operating it

```csharp
Task<int> DrainAsync(int? max = null, CancellationToken ct = default);   // run until nothing is ready, or max runs
void PauseQueue(string? name = null);
void ResumeQueue(string? name = null);
Task<IReadOnlyList<QueueStatus>> GetQueueStatusAsync(CancellationToken ct = default);   // includes orphaned queues
Task<int> RetryPoisonedAsync(string name, CancellationToken ct = default);
Task<int> RequeueAsync(string name, CancellationToken ct = default);     // queue the work for every stored object of its type
Task<int> MoveQueuedAsync(string from, string to, CancellationToken ct = default);
Task DeleteQueueAsync(string name, CancellationToken ct = default);
```

`DrainAsync` cannot be called from inside a handler or a save. Work waiting out a retry delay, after a failure or after finding the write lock held, is not ready and is left for later.

`RequeueAsync` is how work is redone after a handler changes (a new prompt or model), or how a new rule is backfilled.

- **With a filter the index can express** (`[Queryable]` properties compared with constants), it asks the search index for the matching objects (`Search<T>().Where(filter)`), which is cheap.
- **Otherwise** (no filter, a type that is not searchable, or a filter the index cannot translate, such as comparing two fields or calling a method) it reads every object in the database.
- Either way every candidate is checked again with the compiled filter before it is queued, since the index may return more than the filter is true for (an analyzed text field matches a word, not the whole value).
- The index can lag the newest writes by up to `MaxSearchStaleness`. An object saved within that window queued the rule itself when it was saved, so a requeue that misses it loses nothing.
- For a condition the index cannot express, store a flag (`NeedsSummary`) that the save sets and the handler clears, and filter on that.

## Orphaned queues

A queue is named after its rule, so renaming a rule, or the method its default name comes from, leaves the old queue behind with any work still in it; removing a rule does the same.

- **Reported:** `GetQueueStatusAsync` lists every queue of the database (prefix `l{catalog hash}{database hash}-`, poison queues folded into their queue) that no rule in the configuration uses, with `Orphaned = true` and its `Waiting` and `Poisoned` counts. The list is made when the status is asked for, so it is never out of date.
- **Never touched by itself:** during a rolling deploy an older server may still be running the old rule and queuing to the old name, so nothing is run, moved or deleted automatically.
- **`MoveQueuedAsync(from, to)`** moves the work of an orphaned queue into the queue of a rule (`to` must be a rule's queue; `from` must not be). Each message is sent to the new queue before it is deleted from the old one, so a crash part way through can run a message twice but never loses one. Set-aside messages go to the rule's poison queue, still set aside. The old queues are left in place, empty.
- **`DeleteQueueAsync(name)`** deletes an orphaned queue and its poison queue, with any work in them. It refuses a name a rule uses.

## Storage

- `LottaCatalog.UseStorage(provider, tables, blobs, queues, directories)` takes a queue factory. Every built-in `Use…()` provider passes one.
- A queued rule on a catalog with no queue storage throws `NotSupportedException` when the database is opened, before anything is created.
- **FileSystem:** `UseFileSystem` passes the queue client of `Iciclecreek.Azure.Storage.FileSystem`. It needs 2.1.0 or later: the 2.0.4 client is not first in, first out and can hand one message to two receivers.
- **Lucene:** `UseLucene` passes the queue client of `Iciclecreek.Azure.Storage.Lucene`, over the same directory factory as everything else. Each queue is a Lucene `Directory` of its own (`queues/{queue}`) with a file per message; no index is involved. A message's file name is about 100 characters. Lucene.Net syncs each file through its own call to Windows, which fails past 260 characters unless the program is declared long-path aware, so `LuceneDirectories.FileSystem(rootPath)` opens each `FSDirectory` by its extended path (`\\?\C:\…`). A factory of your own should do the same, or a send to a deep folder fails and is reported in the save's `Errors`.
- Queue names are `l{catalog hash}{database hash}-{name}` and `…-poison`, since queue names allow only lower-case letters, digits and hyphens. The part after the prefix is the readable name; the catalog, database and name are also in each queue's metadata.
- `ResetDatabaseAsync` empties the database's queues; `DeleteDatabaseAsync` and deleting the catalog delete them, orphaned ones included.
- `RebuildSearchIndex` queues nothing.

## Known limits

- A save in the instant before a crash may not be queued, the same window in which inline rules and indexing are lost.
- Latest-only collapses saves only within one running database instance.
- Sends for a batch save go one at a time.
- Not tested: a latest-only message arriving from another process.

## Left out on purpose

- Queued deletes (`WhenDeleted<T>().Queue(...)`): the object is gone by the time the run comes up, so it needs a snapshot or tombstone design. Planned for 6.x.
- More than one run at a time (`MaxConcurrency`), and batch handlers.
- A change feed (`db.Changes`).
- Collapsing saves across restarts and processes.
- Handing the handler the object as it was at each save.
- Detecting a changed handler automatically.
