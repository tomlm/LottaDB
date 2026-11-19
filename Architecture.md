# LottaDB Architecture

## Overview

LottaDB is a .NET library that stores **POCOs in Azure Table Storage** and indexes them in **Lucene** for rich queries. A LottaDB instance is a **single database** — one Azure table, one Lucene index, identified by a name.

Each write opens a Lucene session, adds/deletes the document, and the session disposes — triggering commit and IndexSearcher refresh. `Search<T>()` always reflects the last committed state.

Rules (`config.Rules.When<T>(filter).Do(...)`) run inline after each write and have full DB access — enabling materialized views, cascading updates, and side effects. `.ModifyQueued(...)` and `.DoQueued(...)` run slow work later, from a queue.

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

    // Rules added at runtime (inline only): db.Rules.When<T>(filter).Do(handler) returns IDisposable
    LiveRuleSet Rules { get; }

    // Queued rules: also db.Rules.StatusAsync(), RetryAsync(rule), DiscardAsync(rule), BackfillAsync(rule)
    Task<int> DrainAsync(int? max = null, CancellationToken ct = default);

    // Materialize
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

## Rules

Rules are like database triggers, except that they never roll back the save (an inline rule's exception goes into the save's `ObjectResult.Errors` and the object stays saved) and they can also queue durable work.

A rule names a type, an optional filter, and one or more actions. `.Do` actions run inline after each save (`When`) or delete (`WhenDeleted`). They have full DB access and can trigger further saves/deletes.

```csharp
opts.Rules.When<Note>().Do(async (note, context, ct) =>
{
    var actor = await context.Db.GetAsync<Actor>(note.AuthorId, ct);
    await context.Db.SaveAsync(new NoteView
    {
        Id = $"nv-{note.NoteId}",
        NoteId = note.NoteId,
        AuthorDisplay = actor?.DisplayName ?? "",
        Content = note.Content,
    }, ct);
});
opts.Rules.WhenDeleted<Note>().Do((note, context, ct) => context.Db.DeleteAsync<NoteView>($"nv-{note.NoteId}", ct));
```

Every handler has the shape `(item, context, cancellationToken)`. `RuleContext` carries the database (`Db`) and, for queued rules, the rule's name, the attempt number and an idempotency key.

Inline actions are adapted to the internal handler delegate the built-in index handlers use, stored per type, and run in registration order; saving a derived type runs the rules of its base types too. Built-in handlers are stored apart and run first, for every type in the hierarchy, so inside an inline rule search already reflects the change that triggered it.

Rules added at runtime are inline only and return a disposable handle:

```csharp
var handle = db.Rules.When<Actor>().Do((actor, context, ct) => { ... });
handle.Dispose(); // remove the rule
```

### Cycle detection

Handlers that produce objects which trigger further handlers are tracked by type name via `AsyncLocal<HashSet<string>>`. If the same type appears twice in one handler chain, processing stops — preventing A->B->A infinite loops.

### Change aggregation

All changes across a handler chain (root save + handler-triggered saves) are aggregated into a single `ObjectResult` via `AsyncLocal`. The root caller receives the complete set of changes and any handler errors.

## Queued Rules (`.ModifyQueued`, `.DoQueued`)

`.Do` runs inside the save, under the write lease. `.ModifyQueued` and `.DoQueued` are for work too slow for that. The verb says what kind of work it is: `.ModifyQueued` changes the object itself, `.DoQueued` does something outside the database.

```csharp
opts.Rules.When<Note>(n => Changed(n.Content)).ModifyQueued(Summarize);   // rule "Note/summarize", named after the method
opts.Rules.When<Note>(n => Changed(n.IsPublished) && n.IsPublished).DoQueued(NotifyFollowers);
```

- **One queue per object type.** A save sends one small message to the queue of the object's concrete type: the key, the ETag and the names of the rules the save matched. The object itself is read when the rules run.
- **A pump per database** runs the rules later, outside the write lease, `MaxConcurrency` objects at a time. For each message it runs the `ModifyQueued` rules, then `MaterializeQueued`, then the `DoQueued` rules.
- **`Changed(...)`** in a filter compares the save with the version it replaces. An object LottaDB handed out carries the JSON it was loaded with, so nothing extra is stored.
- **`.ModifyQueued`** hands the handler a copy and writes it back with a conditional write. If the object changed meanwhile, the result is applied to the newer version when the `Changed(...)` inputs are the same, and dropped otherwise.
- **Failures are per rule.** A rule that throws is retried by itself: the message is replaced in place by one listing only the rules still to do, with their attempt counts. A rule that keeps failing is set aside in the type's poison queue.
- **All state is in the queues.** There are no rows per rule or per object.

It runs from queue storage, the third Azure Storage primitive beside tables and blobs, which the storage provider supplies. `db.Rules.StatusAsync()`, `RetryAsync`, `DiscardAsync` and `BackfillAsync` operate it by rule name. See `Rules-Design.md` for the behaviour in full, and `Rules-Next.md` for what is planned.

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

Every `Use…()` provider funnels through `LottaCatalog.UseStorage(provider, tables, blobs, queues, directories)`, which sets the factories together and records `StorageProvider` (an overload without `queues` is for storage that has no queues; queued rules are then not available). A catalog has exactly one provider: a second `UseStorage` call throws, rather than silently replacing some of the first provider's factories. Setting a factory property directly is still allowed as a deliberate override. The Lucene directory factories the providers use are public in `LuceneDirectories` (`Memory`, `FileSystem`, `Azure`), so a `UseLucene` catalog can put its Lucene `Directory` objects in any of those stores.

### Lucene storage (`UseLucene`)

`catalog.UseLucene(directoryFactory)` registers Lucene as the catalog's only storage provider (`UseLucene()` is shorthand for `LuceneDirectories.Memory()`). Tables, blobs and queues are the clients of the `Iciclecreek.Azure.Storage.Lucene` package (`LuceneTableServiceClient`, `LuceneBlobServiceClient`, `LuceneQueueServiceClient`), all given the same directory factory as the search index. Everything is kept through the Lucene `Directory` abstraction, and each named thing is a Lucene `Directory` of its own:

| Path given to the factory | What it holds |
|---|---|
| `tables/{catalog}` | The catalog's table: a Lucene index with a document per row. |
| `blobs/{catalog}` | The catalog's blob container: files in a Lucene `Directory`. |
| `queues/{queue}` | One object type's rule queue: a file per message in a Lucene `Directory`. |
| `tables/__registry__`, `queues/__registry__` | Which tables and queues exist. |
| `{catalog}/{database}/Search` | A database's search index. |

- It works like every other provider: the table and the blobs are the record, and the search index is written by the built-in indexing handlers after the table write. `RebuildSearchIndex` clears the index and builds it again from the table.
- A table, blob or queue write is durable when it returns. Search index writes are durable when committed (`AutoCommitDelay`).
- The catalog keeps one client of each kind and disposes them, with their Lucene `Directory` objects, when it is disposed or deleted. With `LuceneDirectories.Memory()` the data lives as long as the catalog does.
- Two catalogs (or two processes) can open the same folder: the storage clients share it, and the search index has one writer at a time (see *Running Multiple Servers Against One Database* in the README).
- On Windows, `LuceneDirectories.FileSystem(rootPath)` opens each `FSDirectory` by its extended path (`\\?\C:\…`), so `rootPath` can be as deep as you like. Lucene.Net syncs each file through its own call to Windows, which fails past 260 characters unless the program is declared long-path aware, and a queued message's file name alone is about 100 characters. A factory of your own should do the same.

### No modifications to Lucene.Net.Linq

LottaDB uses Lucene.Net.Linq's public API as designed:
- `ClassMap<T>` for building field mappers programmatically
- `ToDocumentMapper()` + `GetMappingInfo()` to extract configured field mappers
- `DocumentMapperBase<T>` as the base class for custom mapper behavior
- `LuceneDataProvider` for sessions and queryable access

## Scaling Model

Per-user, per-tenant database instances. Each instance = one table + one Lucene index. All concerns bounded by tenant data size.

## Construction

DI-neutral. A catalog takes its storage provider, and a database is opened from it with its configuration:

```csharp
var catalog = new LottaCatalog("myapp", c => c.UseSQLite(@"C:\data"));
var db = await catalog.GetDatabaseAsync("default", options =>
{
    options.Store<Actor>();
    options.Store<Note>();
    options.Rules.When<Note>().Do(async (note, db, ct) => { ... });
});
```
