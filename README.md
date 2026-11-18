[![Build and Test](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml)
[![NuGet](https://img.shields.io/nuget/v/LottaDB.svg)](https://www.nuget.org/packages/LottaDB)

![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB

**LottaDB** is a .NET library that makes it easy to store any **POCO** in **Azure Table Storage** with full **Lucene** search, all with the goodness of **LINQ**. No schema required -- just define a class and go.

- **A lotta power, a little work.** Store any C# class with zero attributes. All properties are automatically queryable and searchable.
- **A lotta bang for a little buck.** Table Storage is the cheapest durable storage in Azure. LottaDB adds Lucene so you get rich queries without the rich pricing.
- **A lotta LINQ.** `GetManyAsync<T>()` and `Search<T>()`, .Where(), .OrderBy() etc.
- **A lotta fidelity.** Full JSON roundtrip. Lists, dictionaries, nested objects -- everything survives.
- **A lotta views.** Rules (`Rules.When<T>().Do(...)`) build materialized views with plain C#, and `.Queue(...)` runs slow work later.
- **A lotta tenants.** One catalog per tenant with multiple databases. Natural isolation, simple cleanup.
- **A lotta nothing to operate.** Table Storage is serverless. Lucene runs in-process.
- **A lotta schema safety.** Schema changes are detected automatically -- Lucene index is rebuilt on startup.
- **A lotta concurrency.** `ChangeAsync<T>()` provides atomic read-modify-write with optimistic concurrency and automatic retry.

### Sweet spot

LottaDB is ideal for **per-user or per-tenant workloads** -- think user profiles, settings, activity feeds, personal knowledge bases, mailboxes, or per-project data. Thousands of objects per tenant, thousands of tenants per deployment.

## Installation

```
dotnet add package LottaDB
```

## Quick Start -- Just Store a lotta objects

Just define a class. No attributes, no base classes, no interfaces:

```csharp
public class Actor
{
    public string Username { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string AvatarUrl { get; set; } = "";
}
```

Create a catalog, register the type, and start storing:

```csharp
var catalog = new LottaCatalog("myapp", "<your Azure Storage connection string>");
var db = await catalog.GetDatabaseAsync("default");

// Save -- a unique key (ULID) is auto-generated
var actor = new Actor { Username = "alice", DisplayName = "Alice" };
await db.SaveAsync(actor);
var key = actor.GetKey();  // e.g. "01JKX3Q7..."

// Point read by key
var loaded = await db.GetAsync<Actor>(key);

// Search -- all properties are automatically queryable
var found = db.Search<Actor>()
    .Where(a => a.DisplayName == "Alice")
    .ToList();
```

That's it. No key attribute needed -- a ULID is assigned automatically. Every property on your class is automatically:
- **Stored** as full-fidelity JSON in table storage
- **Indexed** in Lucene for fast search
- **Queryable** via LINQ expressions and full-text search

## Fine-Tuning with Attributes

When you need more control, attributes let you specify keys, indexing behavior, and exclusions:

```csharp
public class Note
{
    [Key]                                    // explicit key property
    public string NoteId { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]   // exact match only
    public string AuthorId { get; set; } = "";

    [Queryable]                              // full-text search
    public string Content { get; set; } = "";

    [NotQueryable]                           // exclude from indexing (e.g., large payloads)
    public string RawHtml { get; set; } = "";

    public DateTimeOffset Published { get; set; }
    public List<string> Tags { get; set; } = new();
}
```

| Attribute | Effect |
|-----------|--------|
| `[Key]` | Designates the unique key property. Without it, a ULID is auto-generated. |
| `[Queryable]` | Controls how a property is indexed. Strings get full-text search by default. |
| `[Queryable(Facet = true)]` | Also makes the property's values countable with `.Facets(...)`. |
| `[NotQueryable]` | Excludes a property from automatic indexing (useful for large strings). |
| `[DefaultSearch]` | (class-level) Sets the default property for free-text queries. |

## Facets and Joins

Count how many matching objects carry each value of a property, in the same search that returns the results:

```csharp
public class Product
{
    [Queryable(QueryableMode.NotAnalyzed, Facet = true)]
    public string Category { get; set; } = "";

    [Queryable(Facet = true)]
    public List<string> Tags { get; set; } = new();

    [Queryable(Facet = true)]
    public double Price { get; set; }
}

var page = db.Search<Product>(p => p.Category == "phone")
    .Facets(out var facets, f => f
        .Count(p => p.Tags)
        .Ranges(p => p.Price, new[] { new FacetRange("under 100", null, 100.0), new FacetRange("100 and up", 100.0, null) }))
    .Take(20)
    .ToList();   // .Facets(...) and FacetRange need: using Lucene.Net.Linq;

foreach (var tag in facets.For(p => p.Tags))
    Console.WriteLine($"{tag.Label}: {tag.Count}");
```

LINQ `join` works between `Search<T>()` queries. A join between a string `[Key]` and a `NotAnalyzed` string property runs inside Lucene, so a query that returns one side never loads the other:

```csharp
public class Author
{
    [Key]
    public string AuthorId { get; set; } = "";

    [Queryable]
    public string DisplayName { get; set; } = "";
}

var notes = (
    from note in db.Search<Note>()                      // Note.AuthorId is NotAnalyzed
    join author in db.Search<Author>(a => a.DisplayName == "Alice") on note.AuthorId equals author.AuthorId
    orderby note.Published descending
    select note
).Take(20).ToList();
```

Other joins are done in memory, which loads the rows of both sides. A join to a `[Key]` or a `NotAnalyzed` property is case-sensitive. See the wiki pages [Facets](https://github.com/tomlm/LottaDB/wiki/Facets) and [Search and LINQ](https://github.com/tomlm/LottaDB/wiki/Search-and-LINQ#joins).

## Concurrency

`ChangeAsync<T>()` provides safe read-modify-write with automatic retry on conflict:

```csharp
await db.ChangeAsync<Actor>("alice", actor =>
{
    actor.DisplayName = "Alice Updated";
});
```

Multiple concurrent writers on the same key are handled correctly -- ETag-based optimistic concurrency ensures no updates are lost.

## Rules

Rules say what to do when objects are saved or deleted. They live under `Rules`, on the configuration and on an open database. A rule names the type and, optionally, a filter, then one or more actions: `.Do(...)` runs inside the save, `.Queue(...)` runs later from a queue. Rules are like database triggers, except that they never roll back the save (an inline rule's exception goes into the save's `ObjectResult.Errors` and the object stays saved) and they can also queue durable work.

```csharp
var db = await catalog.GetDatabaseAsync("default", config =>
{
    config.Rules.When<Note>(n => n.IsPublished)
          .Do(UpdateFeedView)                        // inline, in the save
          .Queue("notify-followers", Notify).Priority(5);   // later, from the queue "notify-followers"

    config.Rules.When<Note>(n => n.SummarizedContent != n.Content)   // compares two fields: RequeueAsync reads every note
          .Queue(Summarize)                          // queue "summarize", named after the method
              .Priority(10)
              .Retry(maxAttempts: 3, delay: TimeSpan.FromSeconds(10))
              .Timeout(TimeSpan.FromMinutes(2));

    config.Rules.When<Note>(n => n.NeedsReview).Queue("review", Review);   // a flag: RequeueAsync asks the index
    config.Rules.When<Note>().Queue("audit", Audit).EverySave();
    config.Rules.When<Mail>().Queue("classify", async (mail, db, ct) => { /* ... */ });   // a lambda must be named

    config.Rules.WhenDeleted<Note>().Do(RemoveFeedView);
});

// On an open database: inline only, removed when the handle is disposed.
using var rule = db.Rules.When<Note>().Do(Refresh);
```

A handler has the shape `Task Handler(T item, LottaDB db, CancellationToken ct)` and can query, save or delete anything.

- **Filter** (optional) is an expression, compiled once, and runs inside the save, so it must be quick and look only at the object. For `.Do` it is checked before the handler. For `.Queue` it is checked when the object is saved (false: nothing is queued) and again when the run comes up (false: the run is skipped, not counted). A filter that throws on a save puts its error in the save's `ObjectResult.Errors`; the save still happens. A filter on indexed (`[Queryable]`) properties compared with constants lets `RequeueAsync` (backfill) find the matching objects through the search index; one the index cannot express, such as comparing two fields, makes it read every object. For those, store a flag such as `NeedsSummary` and filter on that.
- **`.Do`** runs in the save, after the object is written and indexed: inside an inline rule, search already reflects the change that triggered it (the built-in index handlers run before any rule, for the object's type and its base types). What it saves is part of the save's `ObjectResult.Changes`; what it throws is in `ObjectResult.Errors`. Rules for a base type also fire for derived types (a `When<BlobFile>` rule fires for an uploaded `BlobPhoto`).
- **`.Queue`** runs later, outside the write lock, on the object as it is when the run comes up, so it suits slow work such as a model call. It may run more than once for one save, so it must be safe to repeat. Its modifiers apply to the queue they follow:
  - `.Priority(n)`: higher runs first (default 0). Work queued from inside a queued run only goes to lower priorities, so queued rules cannot feed each other in a circle.
  - `.EverySave()`: one run per save. By default several saves of one object before the run comes up give one run.
  - `.Retry(maxAttempts, delay)`: a failing run is retried with a delay that doubles each time (defaults 5 and 2 s), then set aside in a poison queue (`RetryPoisonedAsync`). A run that finds the write lock held by another server waits the same way without counting as a failure.
  - `.Timeout(t)`: how long one run may take (default 5 minutes).
- **Queue names** are 3 to 40 lower-case letters, digits and single hyphens, unique in the configuration. `.Queue(handler)` takes the name from the method: `Summarize` gives `summarize`, `handlers.NotifyFollowersAsync` gives `notify-followers`. A lambda has no name, so it needs `.Queue("name", handler)`.
- **Deletes** have inline rules only: `WhenDeleted<T>(filter).Do(...)`, given the object as it was.
- A rule with no action (`config.Rules.When<Note>()` alone) makes opening the database throw.
- Queued work runs by itself while the database is open. Set `config.QueueMode = QueueMode.Manual` to run it only when you call `db.DrainAsync()`, which is what tests usually want. `PauseQueue`, `ResumeQueue`, `GetQueueStatusAsync` and `RequeueAsync` manage queues by name.

### Slow work and `ChangeAsync`

Do the slow work outside the `ChangeAsync` lambda; inside it, apply the result only if the object has not changed meanwhile. The lambda runs under the write lock, and again on every conflict:

```csharp
static async Task Summarize(Note note, LottaDB db, CancellationToken ct)
{
    // Slow work outside the lock, on the note as this run was given it.
    var summary = await model.SummarizeAsync(note.Content, ct);

    // Quick inside ChangeAsync: apply the summary only if the note still says what was
    // summarized. If it changed, its save queued another run.
    await db.ChangeAsync<Note>(note.NoteId, n =>
    {
        if (n.Content == note.Content)
        {
            n.Summary = summary;
            n.SummarizedContent = note.Content;
        }
        return n;   // unchanged => nothing is written
    }, ct);
}
```

### Renamed and removed queues

A queue is named after its rule, so renaming a rule (or the method a default name comes from) leaves the old queue behind. `GetQueueStatusAsync` lists such queues with `Orphaned = true` and their message counts. Nothing is deleted by itself, because during a rolling deploy an older server may still be using the old name. Move the work with `db.MoveQueuedAsync("old-name", "new-name")`, or drop it with `db.DeleteQueueAsync("old-name")`.

Every built-in storage provider supplies the queues. See [Rules-Design.md](Rules-Design.md) for the behaviour in full.

## 6.0 changes

6.0 replaces `On<T>` and `OnAsync<T>` with rules. One method had grown to carry the name, the trigger, the filter, the scheduling and the work; each is now its own step.

Before:

```csharp
config.On<Note>(async (note, kind, db, ct) =>
{
    if (kind == TriggerKind.Deleted) await RemoveFeedView(note, db, ct);
    else await UpdateFeedView(note, db, ct);
});
config.OnAsync<Note>("summarize", n => n.SummarizedContent != n.Content,
    (note, kind, db, ct) => Summarize(note, db, ct),
    priority: 10, options: new AsyncHandlerOptions { MaxAttempts = 3 });
```

After:

```csharp
config.Rules.When<Note>().Do(UpdateFeedView);
config.Rules.WhenDeleted<Note>().Do(RemoveFeedView);
config.Rules.When<Note>(n => n.SummarizedContent != n.Content)
      .Queue(Summarize).Priority(10).Retry(3, TimeSpan.FromSeconds(2));
```

| Removed | Use instead |
|---|---|
| `ILottaConfiguration.On<T>(EntityHandler<T>)` | `config.Rules.When<T>(filter).Do(handler)`, `config.Rules.WhenDeleted<T>(filter).Do(handler)` |
| `ILottaConfiguration.OnAsync<T>(name, [when,] handler, priority, latest, options)` | `config.Rules.When<T>(filter).Queue(name, handler).Priority(p).EverySave().Retry(n, delay).Timeout(t)` |
| `LottaDB.On<T>(EntityHandler<T>)` | `db.Rules.When<T>(filter).Do(handler)` (returns `IDisposable`) |
| `EntityHandler<T>`, `TriggerKind` | `RuleHandler<T>(item, db, ct)`; the trigger is `When` or `WhenDeleted` |
| `AsyncHandlerOptions` | `.Priority`, `.EverySave`, `.Retry`, `.Timeout` after `.Queue` |

| Renamed (old) | New |
|---|---|
| `AsyncHandlers` / `AsyncHandlerMode` | `QueueMode` / `QueueMode` |
| `AsyncHandlerMaxHops` | `MaxQueueHops` |
| `PauseHandlers` / `ResumeHandlers` | `PauseQueue` / `ResumeQueue` |
| `GetHandlerStatusAsync` / `AsyncHandlerStatus` | `GetQueueStatusAsync` / `QueueStatus` (now with `Orphaned`) |

New: default queue names from the handler method, `MoveQueuedAsync` and `DeleteQueueAsync` for orphaned queues, and filters on inline and delete rules.

## Running Multiple Servers Against One Database

Several server processes can open the same database at once. Reads never take a lock, so any
number of servers can search and query concurrently.

Writing is different: Lucene allows only one writer per index, enforced by a cross-process lock
(a `write.lock` file on disk, a blob lease on Azure). LottaDB takes that lock **on the first
write** rather than at open, and releases it after `WriteLockReleaseDelay` of write inactivity:

```csharp
var db = await catalog.GetDatabaseAsync("mydb", config =>
{
    config.Store<Note>();
    config.WriteLockReleaseDelay = 30000;  // release the writer after 30s idle (default)
    config.WriteLockTimeout      = 10000;  // wait this long for another server to let go
    config.MaxSearchStaleness    = 1000;   // how stale a search may be, in ms
});

await db.SaveAsync(note);        // acquires the write lock here
Console.WriteLine(db.HoldsWriteLock);   // true

await db.ReleaseWriteLockAsync();       // or hand it off explicitly, e.g. on shutdown
```

If another server holds the writer, a write waits up to `WriteLockTimeout` and then throws
`WriteLockUnavailableException`. The failure happens **before** anything is written, so no
partial state is left behind.

### Tuning `WriteLockReleaseDelay`

This is a trade between write latency and handover latency, and it is worth setting
deliberately. Writes spaced **closer together** than the delay cost nothing extra — the writer
is simply kept. Writes spaced **further apart** rebuild the `IndexWriter` every single time:

| Provider | Writer already held | Writer re-acquired | Extra per write |
|---|---|---|---|
| SQLite (local `FSDirectory`) | ~0.4 ms / 54 KB | ~9.4 ms / 484 KB | **~9 ms** |
| Azure (`AzureDirectory`, Azurite) | ~2.2 ms / 185 KB | ~32 ms / 1250 KB | **~30 ms** |

Measured with `WriteLockChurnBenchmarks`. Two things to read out of it:

- **Azure pays about 3x more in absolute terms** (~30 ms vs ~9 ms of added latency), because the
  lock is a blob lease and re-acquiring also re-syncs segment files through the local cache.
- The *multiplier* is actually smaller on Azure (~15x vs ~22x) only because an ordinary Azure
  write is already slower. Absolute added latency is the number that matters.

Those Azure figures come from **Azurite running locally, so they understate real Azure**: a blob
lease acquire and release are sub-millisecond against the emulator and tens of milliseconds
against the real service. Budget accordingly.

So set it comfortably above your normal gap between writes. Lower it only when you need another
server to be able to take over sooner — the default of 30s means a failover waits up to 30s.

> **What this is and isn't.** This gives you lock-free readers and a writer role that migrates
> between servers -- good for failover, for bursty writes, and for topologies where one server
> writes at a time. It does **not** let two servers write continuously at once: a server under
> sustained write load never goes idle, so it keeps the lock and the others time out. If every
> server must write continuously, use a single designated writer with a queue instead.

For that sustained-contention case there is `WriteLockMaxHoldTime`, which forces a writer to
yield after holding the lock for a given time:

```csharp
config.WriteLockMaxHoldTime = 30000;   // yield after 30s of continuously holding the writer
```

Note that yielding costs a pause: after giving up the lock, the instance will not re-acquire it
for about 1.5 seconds. Lucene's lock acquisition polls once per second, so without that back-off
this process would simply re-take the lock on its next write and no other server would ever
observe it free. Treat this as a fairness valve, not a throughput feature.

### Read-only replicas

Set `ReadOnly` to make the intent explicit. Writes then throw immediately instead of competing
for the lock, and the instance creates nothing at open -- no schema manifest, no index rebuild:

```csharp
var replica = await catalog.GetDatabaseAsync("mydb", config =>
{
    config.Store<Note>();
    config.ReadOnly = true;
});

replica.Search<Note>("lucene");      // fine
await replica.SaveAsync(note);        // throws InvalidOperationException
```

A read-only replica may safely start before the writer has ever created the index; it serves
empty results and picks the index up as soon as it appears.

### Things to know

- **`GetAsync` and `GetManyAsync` are always fresh** -- they read Table Storage directly.
  Only `Search` is eventually consistent, bounded by `MaxSearchStaleness`. Your own writes
  are always visible to your own searches immediately.
- **Inline rules (`.Do`) run only on the server that performed the write.** Views written through
  `db.SaveAsync` land in shared storage, so derived data stays correct -- but handlers with
  side effects outside the database (emails, cache invalidation, broadcasts) fire on the
  writing server only. Use a message bus, or a queued rule (`.Queue`), for those.
- **Azure multi-writer is best-effort.** `AzureDirectory` guards the index with a 60-second
  blob lease renewed every 30 seconds. If renewals fail for long enough (a network blip, a
  long GC pause, a suspended VM) the lease can expire while this process still believes it
  holds the writer. Keeping `WriteLockReleaseDelay` short limits the exposure. The same
  classic caveat applies to `FSDirectory` over NFS/SMB, where file locking is unreliable.

## Storage Providers

LottaDB works with Azure Table Storage out of the box. For local development and testing, install a provider package:

| Package | Install | Usage | Description |
|---------|---------|-------| ---------|
| **LottaDB** | `dotnet add package LottaDB` | `catalog.UseAzure(connectionString)` | The default provider. Uses Azure Table Storage API for durability. |
| **LottaDB** (Lucene) | `dotnet add package LottaDB` | `catalog.UseLucene(LuceneDirectories.FileSystem(path))` | Storage that needs nothing but Lucene: tables, blobs, queues and the search index are each kept in a Lucene `Directory` (in memory, on disk, or one you supply). |
| **LottaDB.Memory** | `dotnet add package LottaDB.Memory` | `catalog.UseMemory()` | In-memory provider for unit testing. No durability, but lightning fast and supports all features. |
| **LottaDB.SQLite** | `dotnet add package LottaDB.SQLite` | `catalog.UseSQLite(path)` | Local storage with SQLite. Durable and supports all features, but no serverless scaling. |

```csharp
// Local development with SQLite
var catalog = new LottaCatalog("myapp", catalog => catalog.UseSQLite(@"C:\data"));

// Unit tests with in-memory storage
var catalog = new LottaCatalog("myapp", catalog => catalog.UseMemory());
```

`LuceneDirectories` has ready-made directory factories:

| Factory | Description |
|---------|-------------|
| `LuceneDirectories.Memory()` | In-memory `RAMDirectory`. Contents are lost when the process exits. |
| `LuceneDirectories.FileSystem(rootPath)` | `FSDirectory` on disk under `rootPath`. |
| `LuceneDirectories.Azure(connectionString)` | `AzureDirectory` backed by Azure Blob Storage, with a local cache. |

You can also pass your own `Func<string, Directory>` returning a Lucene `Directory`; it receives paths like `{catalog}/{database}/Search`, and with `UseLucene` also `tables/{table}`, `blobs/{container}` and `queues/{queue}`.

## Documentation

Full documentation is available in the [wiki](https://github.com/tomlm/LottaDB/wiki):
