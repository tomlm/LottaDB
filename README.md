[![Build and Test](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml)
[![NuGet](https://img.shields.io/nuget/v/LottaDB.svg)](https://www.nuget.org/packages/LottaDB)

![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB

**LottaDB**  is a .NET library that makes it easy to store any **POCO** in **Azure Table Storage** with full **Lucene** search, all with the goodness of **LINQ**.
* One line to save
* One line to search

## Overview

**LottaDB** gives you a document database built on **Azure Table Storage** with full-text search via **Lucene Search Engine**. A **catalog** groups multiple **databases** under a single Azure Table, with each database isolated via partition keys and its own Lucene index. Objects are stored with full POCO fidelity, with LINQ as the query language.

## Why LottaDB?

- **A lotta bang for a little buck.** Table Storage is the cheapest durable storage in Azure. LottaDB adds Lucene so you get rich queries without the rich pricing.
- **A lotta LINQ.** `Query<T>()` and `Search<T>()`, .Where(), .OrderBy() etc.
- **A lotta fidelity.** Full JSON roundtrip. Lists, dictionaries, nested objects -- everything survives.
- **A lotta views.** `On<T>` triggers build materialized views with plain C#. No event buses, no eventual consistency -- just inline code.
- **A lotta tenants.** One catalog per tenant with multiple databases. Natural isolation, simple cleanup -- delete the catalog and everything goes with it.
- **A lotta nothing to operate.** Table Storage is serverless. Lucene runs in-process. No clusters, no connection pools, no ops team required.
- **A lotta schema safety.** Schema changes are detected automatically -- when your type registrations change, the Lucene index is rebuilt on startup.

### Sweet spot

LottaDB is ideal for **per-user or per-tenant workloads** -- think user profiles, settings, activity feeds, personal knowledge bases, mailboxes, or per-project data. Thousands of objects per tenant, thousands of tenants per deployment. Each tenant gets its own isolated catalog for pennies/month. It's not designed for billion-row analytics or high-throughput write-heavy pipelines.

## Installation

```
dotnet add package LottaDB
```

## Architecture: Catalogs and Databases

LottaDB uses a two-level hierarchy:

- **Catalog** (`LottaCatalog`) -- a grouping that maps to one Azure Table and one blob container. Owns shared infrastructure: storage clients, analyzer, embedding generator.
- **Database** (`LottaDB`) -- an isolated partition within a catalog, with its own type registrations.

```
Catalog ("userX")          ← one Azure Table "userX", one blob container"/userX"
├── Database "notes"            ← PartitionKey="notes", Lucene index at userX/notes
│   ├── Store<Note>()
│   └── On<Note>(handler)
├── Database "todos"            ← PartitionKey="todos", Lucene index at userX/todos
│   └── Store<Todo>()
└── Database "settings"         ← PartitionKey="settings", Lucene index at userX/settings
    └── Store<UserPrefs>()
```

Multiple databases in a catalog share a table but are fully isolated -- data in one database is invisible to another.

## Quick Example

```csharp
// Define your model
public class Actor
{
    [Key]
    public string Username { get; set; } = "";

    [Queryable]
    public string DisplayName { get; set; } = "";

    public string AvatarUrl { get; set; } = "";
}

// Create a catalog and get a database
var catalog = new LottaCatalog("myapp", "<your Azure Storage connection string>");
var db = await catalog.GetDatabaseAsync("default", config =>
{
    config.Store<Actor>();
});

// Save
await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

// Point read
var actor = await db.GetAsync<Actor>("alice");

// Query (Table Storage -- server-side filter on [Queryable] properties)
var results = db.Query<Actor>(a => a.DisplayName == "Alice")
    .ToList();

// Search (Lucene -- full-text search on [Queryable] properties)
var found = db.Search<Actor>()
    .Where(a => a.DisplayName == "Alice")
    .ToList();
```

## Multi-Tenancy

Each tenant gets their own catalog. Multiple databases within a catalog provide logical separation for different data types or use cases.

```csharp
// Per-tenant catalog
var catalog = new LottaCatalog(tenantId, connectionString);

// Separate databases for different concerns
var notesDb = await catalog.GetDatabaseAsync("notes", c => c.Store<Note>());
var todosDb = await catalog.GetDatabaseAsync("todos", c => c.Store<Todo>());

// Tenant leaves -- drop all databases for the tenant
await catalog.DeleteAsync();
```

### Listing and managing databases

```csharp
// Discover all databases in a catalog
var databases = await catalog.ListAsync();
// ["notes", "todos", "settings"]

// Delete a single database (other databases unaffected)
await notesDb.DeleteDatabaseAsync();
```

## Storing POCO objects

LottaDB needs to know about types you want to store and the metadata about your type to store it and query it.

* **Key** -- the Key to store/retrieve objects under
* **Queryable** -- promote a property to be queryable using LINQ expressions

When you get a database you tell it about your types:
```csharp
var db = await catalog.GetDatabaseAsync("mydb", config =>
{
    config.Store<Actor>();
    config.Store<Note>();
});
```

### Attribute-based modeling

If it's a POCO object you own you can add attributes to describe metadata on how to store the object in LottaDB.

* **`[Key]`** marks the unique identity property. Supports manual values or auto-generated ULIDs.
* **`[Queryable]`** makes a property queryable via LINQ.
* **`[DefaultSearch]`** (class-level) sets which property is the default target for free-text queries and `.Query()`/`.Similar()`.

```csharp
public class Note
{
    [Key]
    public string NoteId { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]  // exact match
    public string AuthorId { get; set; } = "";

    [Queryable]                              // full-text search (string default)
    public string Content { get; set; } = "";

    [Queryable(Vector = true)]                // full-text + vector similarity search
    public string Summary { get; set; } = "";

    public DateTimeOffset Published { get; set; }  // stored in JSON, not indexed
    public List<string> Tags { get; set; } = new(); // complex types just work
}
```

### Fluent modeling

LottaDB can store and retrieve POCO objects you don't own via fluent configuration.

* **SetKey()** define the property which is the key for storage and retrieval
* **AddQueryable()** -- defines a property as queryable via LINQ

```csharp
public class BareNote
{
    public string NoteId { get; set; } = "";
    public string AuthorId { get; set; } = "";
    public string Content { get; set; } = "";
}

var db = await catalog.GetDatabaseAsync("mydb", config =>
{
    config.Store<BareNote>(s =>
    {
        s.SetKey(n => n.NoteId);
        s.AddQueryable(n => n.AuthorId).NotAnalyzed();
        s.AddQueryable(n => n.Content);
    });
});
```

### Default Search Property

Automatically LottaDB creates a synthetic content field that concatenates all analyzed string properties for free-text search. When you call `Search<T>("some text")` or use `.Query("...")` on the object, it searches this combined field.

You can override this behavior by adding the **`[DefaultSearch(nameof(MyContent))]`** attribute to your class or calling `s.DefaultSearch(a => a.MyContent)` to set that a specific property should be used instead.
When set, the automatic property is not created -- your chosen property becomes the default search target for object operations.

This is especially powerful with **computed properties** that compose exactly the content you want searchable:

**Attribute-based:**
```csharp
[DefaultSearch(nameof(Content))]
public class Article
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable]
    public string Title { get; set; } = "";

    [Queryable]
    public string Body { get; set; } = "";

    [Queryable(Vector = true)]
    public string Content { get => $"{Title} {Body}"; }  // composed search field
}
```

**Fluent:**
```csharp
config.Store<Article>(s =>
{
    s.SetKey(a => a.Id);
    s.AddQueryable(a => a.Title);
    s.AddQueryable(a => a.Body);
    s.AddQueryable(a => a.Content).Vector();
    s.DefaultSearch(a => a.Content);
});
```

Now `Search<Article>("lucene")` and `a.Query("lucene")` target the `Content` property, while `a.Title.Query("lucene")` still targets `Title` directly:

```csharp
// Free-text search targets Content (Title + Body)
var results = db.Search<Article>("lucene").ToList();

// Object-level Query/Similar also targets Content
var results = db.Search<Article>(a => a.Query("lucene")).ToList();
var results = db.Search<Article>(a => a.Similar("search engines")).ToList();

// Property-level queries still target the named field
var results = db.Search<Article>(a => a.Title.Query("lucene")).ToList();
var results = db.Search<Article>(a => a.Title.Similar("search engines")).ToList();
```

The referenced property must be indexed via `[Queryable]`, `[Field]`, or the fluent API. An invalid reference throws at initialization.

## LottaCatalog Operations

| Operation | Description |
| --- | --- |
| **GetDatabaseAsync()** | Get or create a database within the catalog |
| **ListAsync()** | List all database IDs in the catalog |
| **DeleteAsync()** | Delete the entire catalog (all databases) |
| **Dispose()** | Dispose all managed database instances |

### Catalog-level settings

Infrastructure settings live on the catalog and are shared across all databases:

```csharp
var catalog = new LottaCatalog("myapp", connectionString, catalog =>
{
    catalog.Analyzer = new StandardAnalyzer(LuceneVersion.LUCENE_48);
    catalog.EmbeddingGenerator = myEmbeddingGenerator;
});
```

| Setting | Description | Default |
| --- | --- | --- |
| **TableServiceClientFactory** | Factory for Azure Table Storage client | `new TableServiceClient(connectionString)` |
| **LuceneDirectoryFactory** | Factory for Lucene index directory | `AzureDirectory` backed by blob storage |
| **Analyzer** | Lucene analyzer for text analysis | `EnglishAnalyzer` |
| **EmbeddingGenerator** | Embedding generator for vector search | `null` (disabled) |

## LottaDB Operations

| Operation | Description |
| --- | --- |
| **SaveAsync()** | Save (upsert) a single object |
| **SaveManyAsync()** | Save (upsert) multiple objects in bulk |
| **ChangeAsync<T>()** | Read-modify-write with optimistic concurrency |
| **GetAsync<T>()** | Point-read a single object by key |
| **GetManyAsync<T>()** | Get multiple items from table storage |
| **DeleteAsync<T>()** | Delete a single object by key or entity |
| **DeleteManyAsync<T>()** | Delete by predicate, by entities, or all of a type |
| **Search<T>()** | Full-text search via Lucene with LINQ |
| **ResetDatabaseAsync()** | Clear this database's data (other databases unaffected) |
| **DeleteDatabaseAsync()** | Delete this database and its index permanently |

### SaveAsync()

Save a POCO object using upsert semantics.

```csharp
await db.SaveAsync(actor);
```

### SaveManyAsync()

Save multiple objects in bulk. Table storage writes are batched transactionally (auto-flushed at 100 ops). On<T> handlers run after each batch commit.

```csharp
await db.SaveManyAsync([actor1, actor2, note1 ]);
```

### ChangeAsync<T>()

Read-modify-write with optimistic concurrency. Fetches the object, applies the mutation, and commits with ETag concurrency. Retries automatically on conflicts.

```csharp
await db.ChangeAsync<Actor>(key, actor => actor.Counter++);
```

### GetAsync<T>()

Point-read a single object by key from table storage.

```csharp
var actor = await db.GetAsync<Actor>("alice");
```

### GetManyAsync<T>()

Query table storage with an optional predicate filter. Filters on `[Queryable]` properties are executed server-side.

```csharp
// All actors
await foreach (var actor in db.GetManyAsync<Actor>()) { ... }

// Server-side filter
await foreach (var note in db.GetManyAsync<Note>(n => n.AuthorId == "alice")) { ... }
```

### DeleteAsync<T>()

Delete a single object by key or entity.

```csharp
await db.DeleteAsync<Note>(key);
await db.DeleteAsync<Note>(note);
```

### DeleteManyAsync<T>()

Delete multiple objects. Pass a predicate to delete matching objects, entities to delete specific ones, or no arguments to delete all objects of that type.

```csharp
// Delete all notes by a specific author
await db.DeleteManyAsync<Note>(n => n.AuthorId == "alice");

// Delete specific entities
await db.DeleteManyAsync<Note>(notesToDelete);

// Delete ALL objects of a type
await db.DeleteManyAsync<Note>();
```

### Search<T>()

Full-text search against the Lucene index. Returns `IQueryable<T>` with full LINQ support.

```csharp
// Free-text query
var results = db.Search<Note>("lucene").ToList();

// LINQ predicate
var results = db.Search<Note>(n => n.Content.Contains("lucene")).ToList();

// Lucene query syntax with field qualifier
var results = db.Search<Note>("AuthorId:alice AND Content:lucene").ToList();
```


## LINQ in LottaDB

LottaDB stores 2 representations of every object: one in **table storage** (the source of truth) and one in a **Lucene** index (for fast search). **Query()** gives you a LINQ query over table storage and **Search()** uses the **LINQ to Lucene** library to query the search engine with LINQ expressions.

### Query (Table Storage)

Filters on `[Queryable]` properties are executed by table storage server-side.

```csharp
// All actors
var all = db.Query<Actor>().ToList();

// Server-side filter (AuthorId is [Queryable])
var aliceNotes = db.Query<Note>(n => n.AuthorId == "alice")
    .ToList();

// Polymorphic query -- returns Person and Employee
var people = db.Query<Person>().ToList();
```

### Search (Lucene)

Filters on `[Queryable]` properties are executed against the Lucene index, supporting full-text search.

```csharp
// Full-text search
var results = db.Search<Note>()
    .Where(n => n.Content.Contains("lucene"))
    .ToList();

// Exact match on NotAnalyzed field
var active = db.Search<Note>()
    .Where(n => n.AuthorId == "alice")
    .ToList();

// Free-text query
var results = db.Search<Note>("foo bar").ToList();

// Lucene query syntax
var results = db.Search<Note>("Title:foo AND bar").ToList();
```

## Vector Similarity Search

LottaDB supports **vector similarity search** using embeddings. Mark string properties with `QueryableMode.Vector` and LottaDB will automatically generate embeddings at index time and support `.Similar()` queries for semantic search.

By default, LottaDB uses [ElBruno.LocalEmbeddings](https://github.com/elbruno/LocalEmbeddings) with the `SmartComponents/bge-micro-v2` model -- no external API calls needed. You can override this by setting `EmbeddingGenerator` on the catalog.

### Making a property vector-searchable

**Attribute-based:**
```csharp
public class Article
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable(Vector = true)]                              // analyzed (default) + vector
    public string Title { get; set; } = "";

    [Queryable(Vector = true)]
    public string Body { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed, Vector = true)]   // exact match + vector
    public string Slug { get; set; } = "";

    [Queryable]                                              // full-text only, no embeddings
    public string Category { get; set; } = "";
}
```

**Fluent:**
```csharp
config.Store<Article>(s =>
{
    s.SetKey(a => a.Id);
    s.AddQueryable(a => a.Title).Vector();              // analyzed + vector
    s.AddQueryable(a => a.Body).Vector();
    s.AddQueryable(a => a.Slug).NotAnalyzed().Vector(); // exact match + vector
    s.AddQueryable(a => a.Category);                     // full-text only
});
```

`Vector` is composable with any `QueryableMode` -- it adds vector embeddings on top of whatever analysis mode you choose.

### Querying with `.Similar()`

**Property-level** -- search against a specific field's embeddings:
```csharp
// Find articles with titles semantically similar to "cute cat napping"
var results = db.Search<Article>(a => a.Title.Similar("cute cat napping")).ToList();
```

**Object-level** -- search against the default search property (the `_content_` composite field, or your `[DefaultSearch]` property):
```csharp
// Semantic search across default search content
var results = db.Search<Article>(a => a.Similar("machine learning breakthroughs")).ToList();
```

**Hybrid** -- combine vector similarity with filters:
```csharp
// Semantic search + exact filter
var results = db.Search<Article>(a => a.Title.Similar("furry animals") && a.Category == "pets")
    .ToList();
```

**Limit results:**
```csharp
var top5 = db.Search<Article>(a => a.Similar("quantum physics"))
    .Take(5)
    .ToList();
```

### Custom embedding generator

To use a different embedding model or an external API, set `EmbeddingGenerator` on the catalog:
```csharp
var catalog = new LottaCatalog("myapp", connectionString, catalog =>
{
    catalog.EmbeddingGenerator = myCustomEmbeddingGenerator; // IEmbeddingGenerator<string, Embedding<float>>
});
```

Set `EmbeddingGenerator` to `null` to disable vector support entirely.

## Triggers via `On<T>`

You can have code run when an object is saved/changed/deleted. That trigger can create new objects for cascading views.

Triggers run inline after each save or delete. They receive the object, the trigger kind (`Saved` or `Deleted`), and the full DB instance.

```csharp
var db = await catalog.GetDatabaseAsync("mydb", config =>
{
    config.Store<Note>();
    config.On<Note>(async (note, kind, db) =>
    {
        Console.WriteLine($"Note {note.NoteId} was {kind}");
    });
});
```

### Materialized views via `On<T>`

You can use On<T> triggers to build derived objects that stay in sync automatically. Whenever the trigger runs you can create/update/delete other objects.

```csharp
public class NoteView
{
    [Key]
    public string Id { get; set; } = "";

    [Queryable(QueryableMode.NotAnalyzed)]
    public string NoteId { get; set; } = "";

    [Queryable]
    public string AuthorDisplay { get; set; } = "";

    [Queryable]
    public string Content { get; set; } = "";
}

var db = await catalog.GetDatabaseAsync("mydb", config =>
{
    config.Store<Note>();
    config.Store<NoteView>();
    config.On<Note>(async (note, kind, db) =>
    {
        if (kind == TriggerKind.Deleted)
        {
            await db.DeleteManyAsync<NoteView>(nv => nv.NoteId == note.NoteId);
            return;
        }

        var actor = await db.GetAsync<Actor>(note.AuthorId);
        await db.SaveAsync(new NoteView
        {
            Id = $"nv-{note.NoteId}",
            NoteId = note.NoteId,
            AuthorDisplay = actor?.DisplayName ?? "",
            Content = note.Content,
        });
    });
});
```

Handlers can trigger further handlers (cascading views). Cycle detection prevents infinite loops -- if the same type appears twice in a handler chain, processing stops.

### Error handling

Handler errors never block the source save/delete. They are captured in `ObjectResult.Errors`:

```csharp
var result = await db.SaveAsync(note);
if (result.Errors.Count > 0)
{
    // log handler failures
}
```

### Automatic schema migration

When you change your type registrations (add a new `[Queryable]` property, register a new type, etc.), LottaDB detects the schema change on startup and automatically rebuilds the Lucene index from table storage.

```csharp
// v1: only Actor registered
var db = await catalog.GetDatabaseAsync("main", c => c.Store<Actor>());

// v2: added Note -- schema changed, index auto-rebuilt
var db = await catalog.GetDatabaseAsync("main", c =>
{
    c.Store<Actor>();
    c.Store<Note>();  // new! triggers rebuild
});
```
