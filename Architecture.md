# LottaDB Architecture

## Overview

LottaDB is a .NET library that stores **POCOs in Azure Table Storage** and maintains **Lucene-backed materialized views**.

Applications hand LottaDB ordinary POCOs. `SaveObjectAsync` writes a row to Azure Table Storage, then runs user-defined view builders that read related objects and produce denormalized view documents indexed into Lucene. Queries against views are expressed in LINQ via `Iciclecreek.Lucene.Net.Linq` and return strongly-typed objects.

LottaDB is **unopinionated about data semantics**. Whether you use it for mutable objects (upsert by natural key), time-ordered immutable records (append with time-based keys), or a mix — that's your choice, expressed through the per-type `EntityMapping<T>`. LottaDB just stores what you give it and runs the projections.

A per-type **mapping** (modeled after [`Azure.EntityServices.Tables`](https://github.com/Aguafrommars/Azure.EntityServices)) tells LottaDB how to compute partition keys, row keys, and which properties to promote to native table columns ("tags"). The full POCO is always stored as JSON.

Storage backend: **Azure Table Storage**, accessed via `Azure.Data.Tables` + `Azure.EntityServices.Tables`. Local development and tests run against **[Azurite](https://github.com/Azure/Azurite)** — same wire protocol, same SDK, no separate in-memory provider.

### Design goals

1. **Store POCOs in Azure Table Storage** with a clean mapping — no `ITableEntity`, no infrastructure on the domain model.
2. **Promote hot properties to tags** for server-side filtering without polluting the domain model.
3. **Materialize views into Lucene** automatically when objects are written.
4. **Query views with LINQ** and get back POCOs.
5. **Rebuildable**: any view can be rebuilt from table storage.

## High-Level Components

```mermaid
flowchart LR
    App[Application code] --> Facade[ILottaDB facade]

    subgraph "Azure Table Storage (Objects)"
        Facade -->|"SaveObjectAsync / GetObjectAsync"| ETC[IEntityTableClient&lt;T&gt;<br/>Azure.EntityServices]
        ETC --> Tables[(Tables<br/>Azurite or Azure)]
    end

    ETC -->|"IEntityObserver&lt;T&gt;"| Engine[ViewProjectionEngine]
    Engine -->|"reads related objects"| Facade
    Engine -->|"upsert / delete"| Index

    subgraph "Lucene (Views)"
        Facade -->|"QueryView&lt;TView&gt;"| Index[ILuceneViewIndex&lt;TView&gt;]
        Index --> Dir[(Directory<br/>via IDirectoryProvider)]
    end

    Index -->|"IViewObserver&lt;TView&gt;"| Observers[View observers<br/>UI / SignalR / etc.]
    Engine -->|"SaveResult"| App
```

## Core Concepts

### Objects are plain POCOs

Objects in LottaDB are ordinary classes. They do **not** implement `ITableEntity`, do **not** inherit a base class, and do **not** carry `PartitionKey` / `RowKey` / `ETag` / `Timestamp` properties.

```csharp
public class Order
{
    public string TenantId   { get; set; }
    public string OrderId    { get; set; }
    public string CustomerId { get; set; }
    public decimal Total     { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public List<OrderLine> Lines { get; set; }
}
```

How that POCO becomes a table row is described entirely by an **`EntityMapping<T>`** registered at composition time.

### Entity Mapping

`EntityMapping<T>` is LottaDB's per-type configuration, a thin wrapper over `Azure.EntityServices.Tables`'s `EntityTableClientConfig<T>`. A mapping defines:

- **Table name** — defaults to the **CLR type name**, lowercased (e.g., `Order` → `orders`). Override with `SetTableName()` if needed.
- **Partition key resolver** — a `Func<T, string>` (or constant). This is the only required configuration.
- **Row key resolver** — a `Func<T, string>`. Can be a natural key, descending-time, ascending-time, or any custom function.
- **Tags** — properties promoted to native table columns so they can be filtered/sorted server-side.
- **Computed tags** — derived values written as columns but not stored on the POCO.

Each registered object type gets **its own Azure table** (one table per CLR type). The partition key provides the within-table grouping dimension.

Minimal mapping — only partition key and row key are needed:

```csharp
opts.Entity<Actor>(e =>
{
    e.SetPartitionKey(a => a.Domain);       // required
    e.SetRowKey(a => a.Username);           // natural key
    // table name defaults to "actors"
});
```

Full mapping with tags:

```csharp
opts.Entity<Order>(e =>
{
    e.SetPartitionKey(o => $"tenant:{o.TenantId}");
    e.SetRowKey(o => o.OrderId);

    e.AddTag(o => o.CustomerId);
    e.AddTag(o => o.Total);
    e.AddTag(o => o.CreatedAt);
    e.AddComputedTag("Year", o => o.CreatedAt.Year);
});
```

The row key strategy determines the storage semantics:

| Strategy | RowKey | Behavior | Use case |
|----------|--------|----------|----------|
| `o => o.OrderId` (natural key) | `order-42` | **Upsert** — one row per object, latest state | Mutable objects (users, profiles) |
| `RowKeyStrategy.DescendingTime(o => o.Published)` | `0250479199999_01HW...` | **Insert** — new row every write, newest first | Time-ordered records (activities, posts) |
| `RowKeyStrategy.AscendingTime(o => o.Published)` | `0638792800000_01HW...` | **Insert** — new row every write, oldest first | Logs, audit trails |
| Custom `Func<T,string>` | anything | Whatever you need | Composite keys, domain-specific ordering |

`SaveObjectAsync` is always an **upsert** (insert-or-replace) at the Azure Table Storage level. For natural-key objects this overwrites the existing row. For time-keyed objects every write has a unique RowKey, so the upsert is effectively an insert.

When stored, a row looks like:

| Column         | Value                                                               |
|----------------|---------------------------------------------------------------------|
| PartitionKey   | `tenant:acme`                                                       |
| RowKey         | `order-42` *(or time-based, depending on strategy)*                 |
| Timestamp      | server-assigned                                                     |
| ETag           | server-assigned                                                     |
| `_json`        | `{"tenantId":"acme","orderId":"...","lines":[...], ...}`            |
| CustomerId     | `cust-123`     *(tag)*                                              |
| Total          | `429.50`       *(tag)*                                              |
| CreatedAt      | `2026-04-09T...` *(tag)*                                            |
| Year           | `2026`         *(computed tag)*                                     |

The full POCO graph (including `Lines`) is preserved losslessly in `_json`. Tags exist purely as a write-side index for cheap server-side filtering; on read, the POCO is always rehydrated from `_json`.

### ILottaDB facade

LottaDB does **not** define its own `IEntityStore` abstraction. Storage is handled by [`Azure.EntityServices.Tables`](https://github.com/Aguafrommars/Azure.EntityServices) via `IEntityTableClient<T>`. For each registered object type, an `IEntityTableClient<T>` is created from the `EntityMapping<T>` and cached internally.

What LottaDB *does* own is a thin **`ILottaDB`** facade whose job is to:

1. Own the per-type `IEntityTableClient<T>` instances.
2. **Fire the view projection engine after every write.**

The API is split into two clear groups — **objects** (Azure Table Storage) and **views** (Lucene):

```csharp
public interface ILottaDB
{
    // === Objects (Azure Table Storage) ===

    // Save: upsert row, run view builders, notify observers
    Task<SaveResult> SaveObjectAsync<T>(T entity, CancellationToken ct = default);

    // Read by key (point-read)
    Task<T?> GetObjectAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default);

    // Query table storage (async LINQ — tag predicates push down to OData)
    IAsyncQueryable<T> QueryObjects<T>();

    // Delete from table storage, then run view builders (which remove affected views)
    Task<SaveResult> DeleteObjectAsync<T>(string partitionKey, string rowKey, CancellationToken ct = default);

    // === Views (Lucene) ===

    // Query materialized views (async LINQ to Lucene)
    IAsyncQueryable<TView> QueryView<TView>();

    // Subscribe to view changes
    IDisposable ObserveView<TView>(Func<ViewChange<TView>, Task> handler);

    // Rebuild a view by replaying all relevant objects through builders
    Task RebuildViews<TView>(CancellationToken ct = default);

    // === Escape hatch ===

    // Raw Azure.EntityServices client (bypasses projection engine)
    IEntityTableClient<T> Table<T>();
}
```

`SaveObjectAsync` returns a `SaveResult` containing the view changes that occurred, so synchronous callers can inspect what happened without subscribing to observers. `DeleteObjectAsync` also returns a `SaveResult` — deleting an object triggers view builders which remove affected views from Lucene. `ObserveView<TView>` registers a callback for decoupled consumers (UI, SignalR, etc.).

`QueryObjects<T>()` and `QueryView<TView>()` both return `IAsyncQueryable<T>` (from `System.Linq.Async`). Async-only — everything in LottaDB is I/O-bound (network to Azure Table Storage, disk to Lucene), so there's no reason to offer sync materialization.

```csharp
// Async materialization
var notes = await lottaDb.QueryObjects<Note>()
    .Where(n => n.Domain == "example.com" && n.AuthorId == "alice")
    .OrderByDescending(n => n.Published)
    .Take(20)
    .ToListAsync();

// Async streaming
await foreach (var note in lottaDb.QueryObjects<Note>()
    .Where(n => n.Domain == "example.com"))
{
    Process(note);
}

// Views — same pattern
var results = await lottaDb.QueryView<NoteView>()
    .Where(v => v.Tags.Contains("csharp"))
    .OrderByDescending(v => v.Published)
    .Take(20)
    .ToListAsync();
```

For objects, predicates against **tagged** properties are translated to server-side OData filters and pushed down to Azure Table Storage; predicates against **non-tagged** properties are evaluated client-side after JSON deserialization. For views, the LINQ provider is `Iciclecreek.Lucene.Net.Linq`.

`IViewBuilder<TTrigger,TView>.BuildAsync` receives the `ILottaDB` facade so view builders can load related objects via `GetObjectAsync` — the same interface the application uses.

**Local development and tests use [Azurite](https://github.com/Azure/Azurite)**. The test connection string is `UseDevelopmentStorage=true`; tests exercise the same code path as production.

```csharp
services.AddLottaDB(opts =>
{
    opts.UseAzureTables("UseDevelopmentStorage=true");   // Azurite for dev/test
    // or: opts.UseAzureTables(productionConnectionString);
});
```

### Materialized Views

A **materialized view** is a denormalized POCO purpose-built for a query pattern. Views live only in Lucene and can be rebuilt at any time from table storage.

```csharp
public class OrderSummaryView
{
    public string OrderId { get; set; }
    public string CustomerName { get; set; }
    public string CustomerEmail { get; set; }
    public decimal Total { get; set; }
    public string[] ProductNames { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}
```

Views are produced by an `IViewBuilder<TTrigger, TView>`:

```csharp
public interface IViewBuilder<TTrigger, TView>
{
    /// <summary>
    /// Called whenever a TTrigger object is saved or deleted. The builder may
    /// read related objects via ILottaDB and return zero or more view documents
    /// to upsert into (or delete from) the Lucene index.
    /// </summary>
    IAsyncEnumerable<ViewResult<TView>> BuildAsync(TTrigger entity, ILottaDB db, CancellationToken ct);
}

/// <summary>
/// Result from a view builder — either an upsert or a delete.
/// </summary>
public record ViewResult<TView>
{
    public string Key { get; init; }
    public TView? View { get; init; }        // null = delete this key from the index
}
```

Example — an ActivityPub-style scenario with actors and notes:

```csharp
// Objects: Actor is upserted by natural key, Note is appended by time
public class Actor
{
    public string Domain       { get; set; }
    public string Username     { get; set; }
    public string DisplayName  { get; set; }
    public string AvatarUrl    { get; set; }
}

public class Note
{
    public string NoteId     { get; set; }
    public string AuthorId   { get; set; }
    public string Domain     { get; set; }
    public string Content    { get; set; }
    public DateTimeOffset Published { get; set; }
    public List<string> Tags { get; set; }
}

// Materialized view: denormalized note with author info baked in
public class NoteView
{
    public string NoteId          { get; set; }
    public string AuthorUsername  { get; set; }
    public string AuthorDisplay   { get; set; }
    public string AvatarUrl       { get; set; }
    public string Content         { get; set; }
    public DateTimeOffset Published { get; set; }
    public string[] Tags          { get; set; }
}

// View builder: when a Note is saved, load the author and produce a NoteView
public class NoteViewBuilder : IViewBuilder<Note, NoteView>
{
    public async IAsyncEnumerable<ViewResult<NoteView>> BuildAsync(
        Note note, ILottaDB db, [EnumeratorCancellation] CancellationToken ct)
    {
        var author = await db.GetObjectAsync<Actor>(note.Domain, note.AuthorId, ct);

        yield return new ViewResult<NoteView>
        {
            Key = note.NoteId,
            View = new NoteView
            {
                NoteId         = note.NoteId,
                AuthorUsername = author?.Username ?? "",
                AuthorDisplay  = author?.DisplayName ?? "",
                AvatarUrl      = author?.AvatarUrl ?? "",
                Content        = note.Content,
                Published      = note.Published,
                Tags           = note.Tags?.ToArray() ?? Array.Empty<string>(),
            }
        };
    }
}
```

A trigger may fan out to **multiple** view builders, and one builder may emit **multiple** view results (e.g., an `Actor` change rewrites every `NoteView` for that actor's posts).

### View Projection Engine

The `ViewProjectionEngine` is registered as an `IEntityObserver<T>` on each object type's `IEntityTableClient<T>`. When `Azure.EntityServices` writes or deletes a row, the observer fires and the engine:

1. Looks up all `IViewBuilder<TEntity, *>` registrations for the written object's CLR type.
2. Invokes each builder, which yields `ViewResult<TView>` items.
3. For each result: if `View` is non-null → **upsert** into Lucene; if `View` is null → **delete** from Lucene by key.
4. Notifies all registered view observers with the full typed view.
5. Collects all `ViewChange` items into the `SaveResult` returned to the caller.

Projection runs **inline by default** so reads after writes are consistent. An optional `IProjectionDispatcher` allows queueing to a background channel for high-throughput scenarios.

### Observers & SaveResult

LottaDB provides two ways to access the view changes produced by a write:

#### 1. SaveResult (synchronous return)

`SaveObjectAsync` and `DeleteObjectAsync` return a `SaveResult` containing the view changes that were produced:

```csharp
public record SaveResult
{
    public IReadOnlyList<ViewChange> ViewChanges { get; init; }
}

public record ViewChange
{
    public string ViewTypeName { get; init; }    // e.g., "NoteView"
    public string Key { get; init; }             // view key
    public ViewChangeKind Kind { get; init; }    // Upserted or Deleted
    public object? View { get; init; }           // the full typed view (or null if deleted)
}

public enum ViewChangeKind { Upserted, Deleted }
```

Usage:

```csharp
var result = await lottaDb.SaveObjectAsync(note);

foreach (var change in result.ViewChanges)
{
    if (change.View is NoteView noteView)
        Console.WriteLine($"View {change.Kind}: {noteView.NoteId} by {noteView.AuthorDisplay}");
}
```

#### 2. ObserveView&lt;TView&gt; (decoupled, async callback)

For consumers that want to react to view changes without being the caller of `SaveObjectAsync` — UI updates, SignalR push, cache invalidation, etc.:

```csharp
public record ViewChange<TView>
{
    public string Key { get; init; }
    public TView? View { get; init; }            // full typed view; null = deleted
    public ViewChangeKind Kind { get; init; }
}
```

Usage:

```csharp
// Subscribe — returns IDisposable for unsubscription
var subscription = lottaDb.ObserveView<NoteView>(async change =>
{
    await hub.Clients.Group(change.Key).SendAsync("noteChanged", change);
});

// Later: unsubscribe
subscription.Dispose();
```

Multiple observers can be registered per view type. Observers are invoked after the Lucene index has been updated, so `QueryView<TView>()` is consistent by the time the observer fires.

#### How the two layers connect

```mermaid
sequenceDiagram
    participant App
    participant Lotta as ILottaDB
    participant ATS as Azure Table Storage
    participant Eng as ViewProjectionEngine
    participant Idx as ILuceneViewIndex&lt;TView&gt;
    participant Obs as IViewObserver&lt;TView&gt;

    App->>Lotta: SaveObjectAsync(note)
    Lotta->>ATS: write row (JSON + tags)
    ATS-->>Lotta: ok

    note over Lotta,Eng: Azure.EntityServices IEntityObserver fires

    Lotta->>Eng: project(note)
    Eng->>Lotta: GetObjectAsync(actor)
    Lotta->>ATS: point-read
    ATS-->>Lotta: actor
    Lotta-->>Eng: actor
    Eng->>Idx: Upsert or Delete (per ViewResult)
    Idx-->>Eng: ok
    Eng->>Obs: ViewChange&lt;NoteView&gt; (full typed view)
    Obs-->>Eng: ok
    Eng-->>Lotta: list of ViewChanges
    Lotta-->>App: SaveResult (with ViewChanges)
```

The projection engine is itself registered as an `IEntityObserver<T>` on the `Azure.EntityServices` `IEntityTableClient<T>`. When a row is written or deleted, the SDK calls the observer, which runs view builders, updates Lucene, notifies view observers, and collects the changes into the `SaveResult` returned to the caller.

### Lucene View Index

`ILuceneViewIndex<TView>` wraps a Lucene.Net `IndexWriter` / `IndexSearcher` pair for a single view type:

```csharp
public interface ILuceneViewIndex<TView>
{
    void Upsert(string key, TView view);
    void Delete(string key);
    IQueryable<TView> AsQueryable();   // Iciclecreek.Lucene.Net.Linq
    void Commit();
}
```

- One Lucene index (one `Directory`) per view type.
- Schema is inferred from the view POCO via `Iciclecreek.Lucene.Net.Linq` attributes (`[Field]`, `[NumericField]`, etc.) or convention.
- The `Directory` is **pluggable** via an `IDirectoryProvider`:

```csharp
public interface IDirectoryProvider
{
    Lucene.Net.Store.Directory GetDirectory(string viewName);
}
```

Built-in providers:

- `FSDirectoryProvider` — local filesystem (default for production).
- `RAMDirectoryProvider` — in-memory (default for tests).
- Room for `AzureBlobDirectoryProvider` or any community Directory implementation.

### Query API

**Point-read** an object from table storage:

```csharp
var actor = await lottaDb.GetObjectAsync<Actor>("example.com", "alice");
```

**Query objects** (LINQ — tag predicates push down to server-side OData):

```csharp
// Async
var notes = await lottaDb.QueryObjects<Note>()
    .Where(n => n.Domain == "example.com" && n.AuthorId == "alice")
    .OrderByDescending(n => n.Published)
    .Take(20)
    .ToListAsync();

// Streaming
await foreach (var note in lottaDb.QueryObjects<Note>()
    .Where(n => n.Domain == "example.com")
    .OrderByDescending(n => n.Published))
{
    Console.WriteLine($"{note.Published}: {note.Content}");
}
```

**Delete** an object:

```csharp
var result = await lottaDb.DeleteObjectAsync<Note>("example.com", noteRowKey);
// result.ViewChanges shows which views were removed
```

**Query a materialized view** (LINQ to Lucene):

```csharp
var results = await lottaDb.QueryView<NoteView>()
    .Where(v => v.Tags.Contains("csharp") && v.Published > cutoff)
    .OrderByDescending(v => v.Published)
    .Take(20)
    .ToListAsync();
```

## Registration & Composition

```csharp
services.AddLottaDB(opts =>
{
    opts.UseAzureTables(connectionString);
    opts.UseLuceneDirectory(new FSDirectoryProvider("./lucene-views"));

    // Mutable object — natural key, one row per actor
    // Table name defaults to "actors"
    opts.Entity<Actor>(e =>
    {
        e.SetPartitionKey(a => a.Domain);
        e.SetRowKey(a => a.Username);
        e.AddTag(a => a.DisplayName);
    });

    // Time-ordered object — descending time, one row per write
    // Table name defaults to "notes"
    opts.Entity<Note>(e =>
    {
        e.SetPartitionKey(n => n.Domain);
        e.SetRowKey(RowKeyStrategy.DescendingTime(n => n.Published));
        e.AddTag(n => n.AuthorId);
        e.AddTag(n => n.Published);
    });

    opts.AddView<Note,  NoteView, NoteViewBuilder>();
    opts.AddView<Actor, NoteView, ActorChangedToNoteViewBuilder>();

    // Optional: register view observers at composition time
    opts.AddViewObserver<NoteView>(async change =>
    {
        // Push to SignalR, update cache, etc.
        await hub.Clients.All.SendAsync("noteChanged", change);
    });
});
```

Note how `Actor` uses a natural key (upsert — one row per actor) while `Note` uses descending time (append — one row per write). Both trigger view builders the same way. Neither needs `SetTableName()` — the CLR type name is used by convention. Observers can also be registered at runtime via `lottaDb.ObserveView<TView>(...)`.

## Rebuild & Backfill

Because views are projections from table storage, LottaDB exposes a `RebuildViews<TView>()` operation that:

1. Drops or creates the Lucene index for `TView`.
2. Streams every relevant object from table storage via `QueryObjects<T>()`.
3. Re-runs each registered builder for each object.
4. Commits in batches.

This is the recovery mechanism. If Lucene data is lost, corrupted, or a view's shape changes, rebuild it. Table storage is the system of record; the view is disposable.

Tag columns can be regenerated by replaying every row's `_json` through the current `EntityMapping<T>` — useful when adding a new tag to an existing object type.

## Project Layout (proposed)

```
/src
  LottaDB                          // EntityMapping<T>, RowKeyStrategy, ViewProjectionEngine, ILottaDB
  LottaDB.Lucene                   // ILuceneViewIndex, IDirectoryProvider
/test
  LottaDB.Tests                    // run against Azurite
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Clear Object/View split in the API** | `*Object*` methods = table storage (raw POCOs). `*View*` methods = Lucene (materialized projections). No ambiguity about which side you're talking to. |
| **Unopinionated about data semantics** | Natural keys → upsert; time keys → append. The library doesn't care; the row key strategy determines behavior. |
| **One table per CLR type, name inferred** | Table name defaults to lowercased type name; no boilerplate. Type segregation at the table level, partition key for within-type grouping. |
| **`SaveObjectAsync` is always upsert** | No insert/upsert distinction. Natural keys → overwrites; time keys → unique RowKey makes upsert equivalent to insert. One operation, no ambiguity. |
| **`DeleteObjectAsync` is a real delete** | Removes the row from table storage and triggers view builders to clean up affected views. |
| Objects are plain POCOs | Domain models stay clean; PK/RK/tags are infrastructure, configured via mapping. |
| Mapping modeled on `Azure.EntityServices.Tables` | Reuse a battle-tested mapping/tags model. |
| Azure Table Storage (Azurite for dev/test) | One backend, one code path. |
| No bespoke `IEntityStore` — reuse `IEntityTableClient<T>` | Thin `ILottaDB` facade hosts the post-write projection hook. |
| **`IAsyncQueryable<T>` — async-only LINQ** | Everything is I/O-bound; no reason to offer sync. Clean, modern, no legacy `IQueryable` baggage. |
| Tags = property promotion | Server-side filterable hot fields without sacrificing the JSON document. |
| Full POCO as JSON in `_json` column | The POCO is the source of truth; reads always rehydrate from JSON. |
| `ViewResult<TView>` with nullable `View` | Builders signal upsert *or* delete from the same method. |
| **`SaveResult` + `ObserveView<TView>` — both access patterns** | Synchronous callers get view changes in the return value; decoupled consumers subscribe to typed observer callbacks. Complementary, not redundant. |
| **Projection engine is an `IEntityObserver<T>`** | Hooks into `Azure.EntityServices`' built-in observer mechanism — no custom post-write plumbing. |
| Views are write-through, inline by default | Read-after-write consistency for the writer. |
| Lucene `Directory` is pluggable | Same indexing code works on disk, in RAM, or in blob storage. |
| Builders are explicit and typed | Projections are discoverable, testable, and rebuildable. |
| LINQ via Iciclecreek.Lucene.Net.Linq | Strongly-typed queries returning POCOs. |
| One Lucene index per view type | Clean schema, isolated rebuilds, simpler concurrency. |

## Out of Scope (initially)

- Change logs / event sourcing (layer this on top if you need it).
- Cross-view transactions.
- Distributed projection coordination across multiple writer processes.
- Full-text analyzer customization beyond what `Iciclecreek.Lucene.Net.Linq` exposes by default.
