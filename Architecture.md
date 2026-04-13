# LottaDB Architecture

## Overview

LottaDB is a .NET library that stores **POCOs in Azure Table Storage** and automatically indexes them into **Lucene** for rich queries. A LottaDB instance is a **single database** — one Azure table, one Lucene index, identified by a name.

Each write opens a Lucene session, adds/deletes the document, and the session disposes — triggering commit and IndexSearcher refresh. `Search<T>()` always reflects the last committed state.

Materialized views are declared as **LINQ join expressions** via `CreateView<T>()`. LottaDB parses the expression tree, extracts dependencies and join keys, and incrementally maintains the derived objects.

## The Database

A `LottaDB` instance represents a single database:

- **Name** — used as the Azure table name and Lucene index name.
- **One Azure table** — all types in the same table, `PartitionKey = typeof(T).Name`.
- **One Lucene index** — all types in one index.
- **`_type`** — stored in both table storage and Lucene, contains the type hierarchy (e.g. `"Bot,Actor,BaseEntity"`). Enables polymorphic queries via `Query<BaseClass>()`.
- **`_json`** — full POCO serialized as JSON in table storage.

```csharp
var db = new LottaDB("myapp", tableServiceClient, directory, opts =>
{
    opts.Store<Actor>();
    opts.Store<Note>();
});
```

## Core Concepts

### Objects are plain POCOs

Objects require two attributes: `[Key]` (LottaDB's key) and `[Field(Key = true)]` (Lucene's document key). Both go on the same property.

```csharp
public class Actor
{
    [Key]
    [Field(Key = true)]
    public string Username { get; set; } = "";

    [Tag]
    [Field(IndexMode.NotAnalyzed)]
    public string DisplayName { get; set; } = "";

    public string AvatarUrl { get; set; } = "";
}
```

- `[Key]` — tells LottaDB which property is the unique identifier (used as RowKey in table storage).
- `[Field(Key = true)]` — tells Lucene which field is the document key (for upsert/dedup behavior).
- `[Tag]` — promotes a property to a native table storage column for server-side filtering.
- `[Field]` / `[NumericField]` — configures Lucene indexing (from `Iciclecreek.Lucene.Net.Linq`).

### How objects are stored

| Column | Value | Description |
|--------|-------|-------------|
| PartitionKey | `Actor` | CLR type name (set by LottaDB) |
| RowKey | `alice` | The `[Key]` value |
| `_json` | `{"username":"alice",...}` | Full POCO as JSON |
| `_type` | `Actor,BaseEntity` | Type hierarchy (comma-separated) |
| *(tags)* | promoted values | `[Tag]` properties as native columns |

### Key strategies

| Strategy | Key value | Behavior |
|----------|-----------|----------|
| `KeyStrategy.Natural` (default) | Property value | Upsert — one row per object |
| `KeyStrategy.DescendingTime` | Inverted ticks + ULID | Append — newest first |
| `KeyStrategy.AscendingTime` | Ticks + ULID | Append — oldest first |
| Fluent `SetKey(Func<T, string>)` | Computed string | Custom composite keys |

### Polymorphic queries via `_type`

Every object's `_type` field contains its full type hierarchy. `Query<T>()` filters by `_type contains typeof(T).Name`, enabling polymorphic queries:

```csharp
db.Query<Actor>();          // only Actors
db.Query<BaseEntity>();     // Actors, Notes, everything extending BaseEntity
```

Note: Lucene polymorphic search (`Search<BaseClass>()`) requires `[DocumentKey]` support in `Iciclecreek.Lucene.Net.Linq` — not yet implemented. `Search<T>()` currently returns only exact-type matches.

### The API

```csharp
public class LottaDB
{
    // Write — each opens a Lucene session, commits on completion
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default);
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default);

    // Read
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default);
    IQueryable<T> Query<T>();              // table storage, polymorphic
    IQueryable<T> Search<T>(string? query = null);  // Lucene

    // Observe
    IDisposable Observe<T>(Func<ObjectChange<T>, Task> handler);

    // Maintain
    Task RebuildIndex(CancellationToken ct = default);
}
```

### Materialized Views via CreateView

```csharp
opts.CreateView<NoteView>(db =>
    from note in db.Query<Note>()
    join actor in db.Query<Actor>()
        on note.AuthorId equals actor.Username
    select new NoteView
    {
        NoteId = note.NoteId,
        AuthorUsername = actor.Username,
        AuthorDisplay = actor.DisplayName,
        Content = note.Content,
    }
);
```

Uses `db.Query<T>()` (table storage) for joins. After a source object is saved, LottaDB re-executes the expression and saves the derived objects.

Known limitation: `Where` clauses in `CreateView` expressions are not yet applied during execution.

### Explicit Builders

For custom logic that can't be a LINQ join:

```csharp
public interface IBuilder<TTrigger, TDerived>
{
    IAsyncEnumerable<BuildResult<TDerived>> BuildAsync(
        TTrigger entity, TriggerKind trigger, LottaDB db, CancellationToken ct);
}
```

Smart defaults: on delete with no builder output, LottaDB re-runs the builder with `Saved` to discover derived keys, then deletes them.

### Observers

```csharp
db.Observe<NoteView>(async change =>
{
    await hub.Clients.All.SendAsync("noteChanged", change);
});
```

Fires after each write for saved or deleted objects, including derived objects from builders.

### Error handling

Source save always succeeds. Builder failures captured in `ObjectResult.Errors` and reported to `IBuilderFailureSink` (if registered).

### Cycle detection

Builders that produce objects which trigger further builders are tracked via a visited set. Same object key processed twice in one chain = stop.

## Scaling Model

Per-user, per-tenant database instances. Each instance = one table + one Lucene index. All concerns bounded by tenant data size.

## Construction

DI-neutral. Constructor takes dependencies directly:

```csharp
var db = new LottaDB("myapp", tableServiceClient, directory, opts =>
{
    opts.Store<Actor>();
    opts.Store<Note>();
    opts.Store<NoteView>();

    opts.CreateView<NoteView>(db => ...);
    opts.Observe<NoteView>(async change => ...);
});
```

Optional DI extension: `services.AddLottaDB(...)`.

## Known Limitations

- `Search<BaseClass>()` polymorphic queries not yet implemented (requires `[DocumentKey]` in `Iciclecreek.Lucene.Net.Linq`). Use `Query<BaseClass>()` for polymorphic queries.
- `Where` clauses in `CreateView` expressions not applied during execution.
- `RebuildIndex()` uses `KeyConstraint.Unique` — requires `[Field(Key=true)]` on key properties.
- `[Key]` and `[Field(Key=true)]` are both required on key properties (LottaDB key + Lucene key). Future: unify into single attribute.
