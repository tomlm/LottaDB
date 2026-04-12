# LottaDB Architecture

## Overview

LottaDB is a .NET library that stores **POCOs in Azure Table Storage** and automatically indexes them into **Lucene** for rich queries. A LottaDB instance is a **single database** — one Azure table, one Lucene index, identified by a name. Types are discriminated by a `_Type` column/field, not by separate tables or indexes.

Materialized views are declared as **LINQ join expressions** via `CreateView<T>()` — LottaDB parses the expression tree, extracts dependencies and join keys, and incrementally maintains the derived objects as source data changes. Everything is an object; derived objects are stored and indexed like any other, enabling cascading views.

LottaDB is **unopinionated about data semantics**. Whether you use it for mutable objects (upsert by natural key), time-ordered immutable records (append with time-based keys), or a mix — that's your choice, expressed through `Store<T>()`. LottaDB just stores what you give it and runs the builders.

### Design goals

1. **One database = one table + one index.** No per-type tables, no per-type indexes. Simple.
2. **`[Key]` is the only required attribute.** No partition keys, no row keys — LottaDB handles storage internals.
3. **`_Type` hierarchy enables polymorphic queries.** `Query<BaseClass>()` returns all derived types.
4. **Auto-index everything into Lucene.** Every object is searchable out of the box.
5. **Materialized views as LINQ joins** — `CreateView<T>()` declares the join; LottaDB incrementally maintains the result.
6. **Rebuildable**: the Lucene index can be rebuilt from table storage.

## The Database

A `LottaDB` instance represents a single database:

- **Name** — used as the Azure table name and the Lucene directory/index name.
- **One Azure table** — all object types stored in the same table, discriminated by `PartitionKey = typeof(T).Name`.
- **One Lucene index** — all object types indexed together, discriminated by a `_Type` field.
- **`TableServiceClient`** — injected via DI. Real Azure for production, Spotflow in-memory for tests.
- **`Directory`** — a single Lucene `Directory` for the database (FSDirectory for production, RAMDirectory for tests).

```csharp
// Production
services.AddSingleton(new TableServiceClient(connectionString));
services.AddLottaDB("myapp", new FSDirectory("./myapp-index"), opts =>
{
    opts.Store<Actor>();
    opts.Store<Note>();
    opts.Store<NoteView>();
});

// Tests
services.AddSingleton<TableServiceClient>(InMemoryTableServiceClient.FromAccount(account));
services.AddLottaDB("testdb", new RAMDirectory(), opts =>
{
    opts.Store<Actor>();
    opts.Store<Note>();
});
```

### How objects are stored

| Column | Value | Description |
|--------|-------|-------------|
| PartitionKey | `Actor` | CLR type name (set by LottaDB) |
| RowKey | `alice` | The `[Key]` value |
| `_json` | `{"username":"alice",...}` | Full POCO serialized as JSON |
| `_Type` | `Actor,BaseEntity,object` | Type hierarchy (comma-separated) |
| *(tags)* | promoted property values | `[Tag]` properties as native columns |

### How objects are indexed in Lucene

Every object is indexed into a single Lucene index with these system fields:

| Field | Value | Description |
|-------|-------|-------------|
| `_Type` | `["Actor", "BaseEntity", "object"]` | Multi-valued type hierarchy field |
| `_Key` | `alice` | The object's key |
| *(user fields)* | per `[Field]`/`[NumericField]` attributes | User-defined indexed fields |

## Core Concepts

### Everything is an object

Objects in LottaDB are ordinary classes. The only required annotation is `[Key]`.

```csharp
public class Actor
{
    [Key]
    public string Username { get; set; } = "";

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string DisplayName { get; set; } = "";

    public string AvatarUrl { get; set; } = "";
}

public class Note
{
    [Key(Strategy = KeyStrategy.DescendingTime)]
    public DateTimeOffset Published { get; set; }

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string AuthorId { get; set; } = "";

    [Field(Key = true)]
    public string NoteId { get; set; } = "";

    [Field]
    public string Content { get; set; } = "";
}
```

### The `[Key]` attribute

`[Key]` marks the property used to uniquely identify the object. It becomes the `RowKey` in Azure Table Storage.

```csharp
// Simple key — property value used directly
[Key] public string Username { get; set; }

// Time-ordered key — LottaDB generates a descending-time key from this property
[Key(Strategy = KeyStrategy.DescendingTime)]
public DateTimeOffset Published { get; set; }
```

For composite keys, use `SetKey` fluently:

```csharp
opts.Store<Note>(s => s.SetKey(n => $"{n.Domain}/{n.NoteId}"));
```

Built-in key strategies:

| Strategy | Key value | Behavior |
|----------|-----------|----------|
| `KeyStrategy.Natural` (default) | Property value | **Upsert** — one row per object |
| `KeyStrategy.DescendingTime` | Inverted ticks + ULID | **Append** — newest first |
| `KeyStrategy.AscendingTime` | Ticks + ULID | **Append** — oldest first |
| Fluent `SetKey(Func<T, string>)` | Any computed string | Custom composite keys |

### The `_Type` hierarchy

Every object is stored with a `_Type` field containing the full type hierarchy — the CLR type name and all its base classes (excluding `object` by convention, or including it — TBD).

```csharp
public class BaseEntity { }
public class Actor : BaseEntity { [Key] public string Username { get; set; } }
public class Bot : Actor { public string BotToken { get; set; } }
```

When a `Bot` is saved, `_Type` = `["Bot", "Actor", "BaseEntity"]`.

This enables **polymorphic queries**:

```csharp
// Only Bots
db.Search<Bot>().Where(b => b.BotToken != "");

// All Actors (including Bots)
db.Search<Actor>();

// Everything that extends BaseEntity
db.Query<BaseEntity>();
```

`Query<T>()` and `Search<T>()` automatically filter by `_Type contains typeof(T).Name`. No explicit filtering needed.

### Store&lt;T&gt; — type registration

`Store<T>()` registers a type with LottaDB. It defines the key strategy, tags, and Lucene field configuration.

**Minimal — just `[Key]` on the POCO:**

```csharp
opts.Store<Actor>();  // everything from attributes
```

**Fluent — for composite keys or types you can't attribute:**

```csharp
opts.Store<Note>(s =>
{
    s.SetKey(KeyStrategy.DescendingTime(n => n.Published));
    s.AddTag(n => n.AuthorId);
    s.Index(n => n.Content).AnalyzedWith<EnglishAnalyzer>();
});
```

**Attributes for everything:**

Table storage: `[Key]`, `[Tag]`
Lucene: `[Field]`, `[NumericField]`, `[IgnoreField]` (from `Iciclecreek.Lucene.Net.Linq`)

### ILottaDB facade

```csharp
public interface ILottaDB
{
    // === Write ===
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default);
    Task<ObjectResult> SaveAsync<T>(string key, T entity, CancellationToken ct = default);
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default);
    Task<ObjectResult> ChangeAsync<T>(T entity, Func<T, T> mutate, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default);

    // === Read ===
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default);
    Task<T?> GetAsync<T>(T entity, bool force = false, CancellationToken ct = default);
    IQueryable<T> Query<T>();     // table storage, auto-filtered by _Type
    IQueryable<T> Search<T>();    // Lucene, auto-filtered by _Type
    IQueryable<T> Search<T>(string query);  // Lucene with query string

    // === Observe ===
    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler);

    // === Maintain ===
    Task RebuildIndex(CancellationToken ct = default);  // rebuild entire Lucene index
}
```

Key simplifications from the previous design:
- **`GetAsync<T>(string key)`** — just the key, no partition key.
- **`DeleteAsync<T>(string key)`** — just the key.
- **`ChangeAsync<T>(string key, ...)`** — just the key.
- **`RebuildIndex()`** — rebuilds the entire index (one index per DB), not per type.
- **No `Table<T>()` escape hatch** — if needed, access the `TableServiceClient` from DI directly.

### Materialized Views via CreateView

```csharp
opts.CreateView<NoteView>(db =>
    from note in db.Query<Note>()
    join actor in db.Query<Actor>()
        on note.AuthorId equals actor.Username
    select new NoteView
    {
        NoteId         = note.NoteId,
        AuthorUsername = actor.Username,
        AuthorDisplay  = actor.DisplayName,
        Content        = note.Content,
        Published      = note.Published,
    }
);
```

LottaDB parses the expression tree to extract dependencies and join keys, and incrementally maintains the view as source objects change. `CreateView` uses `db.Query<T>()` (table storage) for joins.

### Explicit Builders (escape hatch)

```csharp
public interface IBuilder<TTrigger, TDerived>
{
    IAsyncEnumerable<BuildResult<TDerived>> BuildAsync(
        TTrigger entity, TriggerKind trigger, ILottaDB db, CancellationToken ct);
}
```

For custom logic that can't be expressed as a LINQ join. Registered via `opts.AddBuilder<TTrigger, TDerived, TBuilder>()`.

### Observers

```csharp
var subscription = db.Observe<NoteView>(async change =>
{
    await hub.Clients.All.SendAsync("noteChanged", change);
});
```

### Concurrency

- **`SaveAsync`** — unconditional upsert (clobbers ETag).
- **`ChangeAsync`** — optimistic read-modify-write with ETag retry.
- **ETags tracked internally** — POCOs stay clean.

### Error handling

- Source save always succeeds; builder failures captured in `ObjectResult.Errors` and reported to `IBuilderFailureSink`.

### Lucene index lifecycle

- **Writes** (Save/Change/Delete) go to the `IndexWriter`, set a dirty flag. **No flush.**
- **`Search<T>()`** checks the dirty flag. If dirty, flushes the writer and creates a fresh `IndexSearcher` snapshot. Returns Lucene's `IQueryable<T>`.
- This gives efficient bulk writes and consistent reads.

## Scaling Model

LottaDB targets **small-to-medium workloads** — per-user, per-tenant, per-instance. Scaling is horizontal by creating **separate LottaDB instances per tenant**.

Each instance = one table + one Lucene index. Write amplification, single-writer Lucene, and transaction scope are all bounded by the tenant's data.

## Registration & Composition

```csharp
services.AddSingleton(new TableServiceClient(connectionString));

services.AddLottaDB("myapp", new FSDirectory("./myapp-index"), opts =>
{
    opts.Store<Actor>();
    opts.Store<Note>();
    opts.Store<NoteView>();

    opts.CreateView<NoteView>(db =>
        from note in db.Query<Note>()
        join actor in db.Query<Actor>()
            on note.AuthorId equals actor.Username
        select new NoteView
        {
            NoteId         = note.NoteId,
            AuthorUsername = actor.Username,
            AuthorDisplay  = actor.DisplayName,
            Content        = note.Content,
            Published      = note.Published,
        }
    );

    opts.Observe<NoteView>(async change =>
    {
        await hub.Clients.All.SendAsync("noteChanged", change);
    });
});
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **One table + one index per database** | Simplest possible model. Types discriminated by PartitionKey / `_Type` field. |
| **`_Type` hierarchy for polymorphic queries** | `Query<BaseClass>()` returns all derived types. No manual type filtering. |
| **`[Key]` is the only required attribute** | No partition keys, no row keys. LottaDB handles storage internals. |
| **Single Lucene Directory per DB** | One writer, one searcher, dirty-flag flush. No per-type index management. |
| **`CreateView<T>()` as LINQ joins** | Declarative materialized views. Dependencies and join keys inferred from expression tree. |
| **Everything is an object** | No entity/view distinction. Derived objects stored and indexed like any other. |
| **Dirty flag Lucene writes** | Writes don't flush; Search flushes and creates fresh IndexSearcher. Efficient bulk writes. |
| **`Query<T>()` for joins, `Search<T>()` for search** | Query = table storage (supports LINQ join). Search = Lucene (indexed queries). |
| **`SaveAsync` clobbers, `ChangeAsync` retries** | Two write strategies. Honest about concurrency. |
| **Internal ETag tracking** | POCOs stay clean. |
| **Builder errors don't block writes** | Eventually consistent for derived objects. |
| **Per-tenant database instances** | Scaling model bounds all concerns. |

## Out of Scope (initially)

- Change logs / event sourcing.
- Cross-database transactions.
- Distributed builder coordination.
- Complex LINQ in `CreateView` beyond joins (group by, aggregations).
