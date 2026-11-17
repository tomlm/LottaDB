# LottaDB Architecture

## Overview

LottaDB is a .NET library that stores **POCOs in Azure Table Storage** and indexes them in **Lucene** for rich queries. A LottaDB instance is a **single database** — one Azure table, one Lucene index, identified by a name.

Each write opens a Lucene session, adds/deletes the document, and the session disposes — triggering commit and IndexSearcher refresh. `Search<T>()` always reflects the last committed state.

Reactive handlers (`On<T>`) run inline after each write and have full DB access — enabling materialized views, cascading updates, and side effects.

## The Database

A `LottaDB` instance represents a single database:

- **Name** — used as the Azure table name and Lucene index name.
- **One Azure table** — all types in the same table, `PartitionKey = "lotta"`.
- **One Lucene index** — all types in one index, scoped by type via `_type` field.
- **`_type` / `_types`** — stored in both table storage and Lucene. `_type` is the concrete type name, `_types` contains the full type hierarchy. Enables polymorphic queries.
- **`_json`** — full POCO serialized as JSON, stored in both table storage and Lucene. Provides full-fidelity roundtrip regardless of which fields are indexed.

```csharp
var db = new LottaDB("myapp", tableServiceClient, directory, options);
```

## Core Concepts

### Attributes

LottaDB uses two primary attributes. Users only need to think about **identity** and **queryability**.

#### `[Key]` — Identity

Marks the unique key property. Extends Lucene's `FieldAttribute` so it automatically becomes the Lucene document key — no separate `[Field(Key=true)]` needed.

```csharp
[Key]
public string Username { get; set; } = "";

[Key(Mode = KeyMode.Auto)]   // auto-generate ULID if empty at save time
public string Id { get; set; } = "";
```

#### `[Queryable]` — Make a property queryable

Promotes a property to **both** a Table Storage column (server-side filtering) **and** a Lucene indexed field (full-text or exact-match search). Smart defaults by type:

- `string` properties default to **Analyzed** (full-text searchable)
- Value types default to **NotAnalyzed** (exact match)

```csharp
[Queryable]                            // string → analyzed (full-text)
public string Content { get; set; }

[Queryable(QueryableMode.NotAnalyzed)] // exact match only
public string AuthorId { get; set; }

[Queryable]                            // DateTimeOffset → not analyzed (exact match, sortable ISO format)
public DateTimeOffset Published { get; set; }
```

#### Advanced: `[Tag]` and `[Field]`

For fine-grained control, `[Tag]` and `[Field]` can be used independently:

- `[Tag]` — Table Storage column only (no Lucene index)
- `[Field]` — Lucene index only (no Table Storage column, from `Lucene.Net.Linq`)

```csharp
[Tag]                            // filterable in Query<T>(), not searchable in Search<T>()
public string Category { get; set; }

[Field]                          // searchable in Search<T>(), not filterable server-side
public string Body { get; set; }

[Field(IndexMode.NotAnalyzed)]   // exact match in Lucene only
public string Status { get; set; }
```

### Example model

```csharp
public class Actor
{
    [Key]
    public string Username { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string DisplayName { get; set; } = "";

    public string Domain { get; set; } = "";    // stored in _json, not indexed
    public string AvatarUrl { get; set; } = "";  // stored in _json, not indexed
}
```

### How objects are stored

| Column | Value | Description |
|--------|-------|-------------|
| PartitionKey | `lotta` | Fixed partition key |
| RowKey | `alice` | The `[Key]` value |
| `_json` | `{"Username":"alice",...}` | Full POCO as JSON |
| `_type` | `Lotta.Tests.Actor` | Concrete CLR type |
| `_types` | `Lotta.Tests.Actor,...` | Full type hierarchy |
| *(queryable/tag)* | promoted values | `[Queryable]`/`[Tag]` properties as native columns |

### Key modes

| Mode | Behavior |
|------|----------|
| `KeyMode.Manual` (default) | Use the property value as-is |
| `KeyMode.Auto` | Generate a ULID if the property is empty at save time; use existing value if set |

Fluent `SetKey(expression)` supports computed keys:

```csharp
opts.Store<Actor>(s => s.SetKey(a => $"{a.Domain}/{a.Username}"));
```

### Polymorphic queries

Every object's `_types` field contains its full type hierarchy. `Query<T>()` filters by `_types contains typeof(T).FullName`, enabling polymorphic queries:

```csharp
db.Query<BaseEntity>();  // returns BaseEntity, Person, Employee
db.Query<Person>();      // returns Person, Employee
db.Query<Employee>();    // returns Employee only
```

## The API

```csharp
public class LottaDB
{
    // Write
    Task<ObjectResult> SaveAsync<T>(T entity, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(string key, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(T entity, CancellationToken ct = default);
    Task<ObjectResult> DeleteAsync<T>(Expression<Func<T, bool>> predicate, CancellationToken ct = default);
    Task<ObjectResult> ChangeAsync<T>(string key, Func<T, T> mutate, CancellationToken ct = default);

    // Read
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default);
    IQueryable<T> Query<T>();                                    // table storage, polymorphic
    IQueryable<T> Query<T>(Expression<Func<T, bool>> predicate); // table storage with filter
    IQueryable<T> Search<T>(string? query = null);               // Lucene
    IQueryable<T> Search<T>(Expression<Func<T, bool>> predicate);// Lucene with filter

    // Reactive handlers
    IDisposable On<T>(Func<T, TriggerKind, LottaDB, Task> handler);

    // Maintain
    Task RebuildIndex(CancellationToken ct = default);
}
```

## Configuration

### Attribute-based (primary)

```csharp
public class Note
{
    [Key]
    public string NoteId { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string AuthorId { get; set; } = "";

    [Queryable]
    public string Content { get; set; } = "";

    public DateTimeOffset Published { get; set; }
}
```

### Fluent (equivalent, for bare models or overrides)

```csharp
opts.Store<Note>(s =>
{
    s.SetKey(n => n.NoteId);
    s.AddQueryable(n => n.AuthorId).NotAnalyzed();
    s.AddQueryable(n => n.Content);
});
```

### Advanced fluent (fine-grained control)

```csharp
opts.Store<Note>(s =>
{
    s.SetKey(n => n.NoteId);
    s.AddTag(n => n.AuthorId);          // table storage column only
    s.AddField(n => n.Content);          // Lucene index only
    s.AddField(n => n.Status).NotAnalyzed();
});
```

## Reactive Handlers (`On<T>`)

Handlers run inline after each save or delete. They have full DB access and can trigger further saves/deletes.

```csharp
opts.On<Note>(async (note, kind, db) =>
{
    if (kind == TriggerKind.Deleted) return;
    var actor = await db.GetAsync<Actor>(note.AuthorId);
    await db.SaveAsync(new NoteView
    {
        Id = $"nv-{note.NoteId}",
        NoteId = note.NoteId,
        AuthorDisplay = actor?.DisplayName ?? "",
        Content = note.Content,
    });
});
```

Runtime registration returns a disposable handle:

```csharp
var handle = db.On<Actor>((actor, kind, db) => { ... });
handle.Dispose(); // stop receiving notifications
```

### Cycle detection

Handlers that produce objects which trigger further handlers are tracked by type name via `AsyncLocal<HashSet<string>>`. If the same type appears twice in one handler chain, processing stops — preventing A->B->A infinite loops.

### Change aggregation

All changes across a handler chain (root save + handler-triggered saves) are aggregated into a single `ObjectResult` via `AsyncLocal`. The root caller receives the complete set of changes and any handler errors.

## Queued Handlers (`OnAsync<T>`)

`On<T>` runs inside the save, under the write lease. `OnAsync<T>` is for work too slow for that: every save sends a small message (type and key only) to a queue per handler, and a pump runs the handlers later, outside the write lease, by priority, retrying failures and setting aside what keeps failing in a poison queue.

```csharp
opts.OnAsync<Note>("summarize", async (note, kind, db, ct) =>
{
    var summary = await model.SummarizeAsync(note.Content, ct);
    await db.ChangeAsync<Note>(note.NoteId, n => { n.Summary = summary; return n; }, ct);
}, priority: 10, latest: true);
```

It runs from queue storage, the third Azure Storage primitive beside tables and blobs, which the storage provider supplies. See `OnAsync-Design.md` for the behaviour in full.

## Internals

### LottaDocumentMapper

`LottaDocumentMapper<T>` extends `DocumentMapperBase<T>` from Lucene.Net.Linq. It uses the library's `ClassMap<T>` fluent API to build field mappers with correct type-aware converters (DateTimeOffset, numerics, etc.), then extracts them via `ToDocumentMapper()`.

Key behaviors:
- **Opt-in indexing** — only properties with `[Key]`, `[Queryable]`, `[Field]`, or fluent config are indexed. Unmapped properties are stored in `_json` but not in Lucene fields.
- **`_json` roundtrip** — `MapFieldsToDocument` adds a `StoredField("_json_", ...)` with the full POCO. `CreateFromDocument` deserializes from `_json` for full-fidelity roundtrip.
- **`IsModified`** — compares `_json` strings for change detection.

### TypeMetadata

`TypeMetadata` resolves all configuration (attributes + fluent) into a uniform structure:
- `KeyProperty` — the `PropertyInfo` for the key (from `[Key]` or `SetKey`)
- `KeyMode` — Manual or Auto
- `Tags` — properties promoted to Table Storage columns (from `[Queryable]`, `[Tag]`, or `AddQueryable`/`AddTag`)
- `IndexedProperties` — properties indexed in Lucene (from `[Queryable]`, `[Field]`, or `AddQueryable`/`AddField`)

### TableStorageAdapter

Wraps `Azure.Data.Tables`. Each row stores `_json` (full POCO), `_type` (concrete type), `_types` (type hierarchy), and promoted tag columns. Polymorphic queries filter by `_types.Contains(typeof(T).FullName)` and deserialize from `_json` using the concrete type from `_type`.

### Storage providers

Every `Use…()` provider funnels through `LottaCatalog.UseStorage(provider, tables, blobs, queues, directories)`, which sets the factories together and records `StorageProvider` (an overload without `queues` is for storage that has no queues; `OnAsync<T>` is then not available). A catalog has exactly one provider: a second `UseStorage` call throws, rather than silently replacing some of the first provider's factories. Setting a factory property directly is still allowed as a deliberate override. The Lucene directory factories the providers use are public in `LuceneDirectories` (`Memory`, `FileSystem`, `Azure`), so a `UseLucene` catalog can put its Lucene `Directory` objects in any of those stores.

### Lucene storage (`UseLucene`)

`catalog.UseLucene(directoryFactory)` registers Lucene as the catalog's only storage provider (`UseLucene()` is shorthand for `LuceneDirectories.Memory()`). Tables, blobs and queues are the clients of the `Iciclecreek.Azure.Storage.Lucene` package (`LuceneTableServiceClient`, `LuceneBlobServiceClient`, `LuceneQueueServiceClient`), all given the same directory factory as the search index. Everything is kept through the Lucene `Directory` abstraction, and each named thing is a Lucene `Directory` of its own:

| Path given to the factory | What it holds |
|---|---|
| `tables/{catalog}` | The catalog's table: a Lucene index with a document per row. |
| `blobs/{catalog}` | The catalog's blob container: files in a Lucene `Directory`. |
| `queues/{queue}` | One `OnAsync<T>` handler queue: a file per message in a Lucene `Directory`. |
| `tables/__registry__`, `queues/__registry__` | Which tables and queues exist. |
| `{catalog}/{database}/Search` | A database's search index. |

- It works like every other provider: the table and the blobs are the record, and the search index is written by the `On<T>` indexing handlers after the table write. `RebuildSearchIndex` clears the index and builds it again from the table.
- A table, blob or queue write is durable when it returns. Search index writes are durable when committed (`AutoCommitDelay`).
- The catalog keeps one client of each kind and disposes them, with their Lucene `Directory` objects, when it is disposed or deleted. With `LuceneDirectories.Memory()` the data lives as long as the catalog does.
- Two catalogs (or two processes) can open the same folder: the storage clients share it, and the search index has one writer at a time (see *Running Multiple Servers Against One Database* in the README).
- A queued message is a file with a name of about 100 characters. Under `LuceneDirectories.FileSystem(rootPath)` the whole path has to fit what the file system allows, so keep `rootPath` short on Windows.

### No modifications to Lucene.Net.Linq

LottaDB uses Lucene.Net.Linq's public API as designed:
- `ClassMap<T>` for building field mappers programmatically
- `ToDocumentMapper()` + `GetMappingInfo()` to extract configured field mappers
- `DocumentMapperBase<T>` as the base class for custom mapper behavior
- `LuceneDataProvider` for sessions and queryable access

## Scaling Model

Per-user, per-tenant database instances. Each instance = one table + one Lucene index. All concerns bounded by tenant data size.

## Construction

DI-neutral. Constructor takes dependencies directly:

```csharp
var options = new LottaConfiguration();
options.Store<Actor>();
options.Store<Note>();
options.On<Note>(async (note, kind, db) => { ... });

var db = new LottaDB("myapp", tableServiceClient, directory, options);
```
