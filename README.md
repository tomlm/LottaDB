![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB

**LottaDB**  is a .NET library that makes it easy to story any **POCO** in **Azure Table Storage** with full **Lucene** search, all with the goodness of **LINQ**.
* One line to save
* One line to search

## Overview

**LottaDB** gives you a document database built using **Azure Table Storage** with full-text search via **Lucene Search Engine**. Each LottaDB instance is a single table/lucene catalog. Objects are stored with full POCO fidelity, with efficient LINQ expressions as the query language.

## Why LottaDB?

- **A lotta bang for a little buck.** Table Storage is the cheapest durable storage in Azure. LottaDB adds Lucene so you get rich queries without the rich pricing.
- **A lotta LINQ.** `Query<T>()` and `Search<T>()`, .Where(), .OrderBy() etc.
- **A lotta fidelity.** Full JSON roundtrip. Lists, dictionaries, nested objects -- everything survives. 
- **A lotta views.** `On<T>` triggers build materialized views with plain C#. No event buses, no eventual consistency -- just inline code.
- **A lotta tenants.** One instance per tenant. Natural isolation, simple backup, no noisy neighbors.
- **A lotta nothing to operate.** Table Storage is serverless. Lucene runs in-process. No clusters, no connection pools, no ops team required.

### Sweet spot

LottaDB is ideal for **per-user or per-tenant workloads** -- think user profiles, settings, activity feeds, personal knowledge bases, mailboxes, or per-project data. Thousands of objects per tenant, thousands of tenants per deployment. Each tenant gets its own isolated database for pennies/month. It's not designed for billion-row analytics or high-throughput write-heavy pipelines.

## Installation

```
dotnet add package LottaDB
```

## Features

- **Plain POCOs** -- no base classes, no interfaces. Have an object? Store an object.
- **Full POCO roundtrip** -- objects are serialized as JSON. Complex properties (lists, dictionaries, nested objects) survive storage and retrieval intact.
- **LINQ** -- Rich Linq against typed objects makes it so easy.
- **Vector similarity search** -- mark properties with `QueryableMode.Vector` for semantic search via `.Similar()`. Built-in local embeddings, no API calls needed.
- **Polymorphic queries** -- `Query<Base>()/Search<Base>()` returns all derived types, correctly deserialized into their correct typed objects.
- **Triggers** -- `On<T>` triggers run inline after saves/deletes with full DB access. Build your materialized views with plain C#.
- **Fluent or attribute configuration** -- annotate your models, or configure foreign POCOs entirely via fluent API.
- **Per-tenant scaling** -- one LottaDB instance per tenant. 

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

using var db = new LottaDB("myapp", "<your Azure Storage connection string>", luceneDirectory, config =>
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

## Storing POCO objects

Lotta needs to know about types you want to store and the metadata about your type to store it and query it.

* **Key** -  the Key to store/retrieve objects under

* **Queryable** - Promote a property to be queryable using Linq expressions

When you instantiate a DB you tell the data base about your type:
```csharp
 var db = LottaDBFixture.CreateDb(opts =>
 {
     opts.Store<Actor>();
     opts.Store<Note>();
 });
```

### Attribute-based modeling

If it's a POCO object you own you can add attributes to describe metadata on how to store the object in Lotta.

* **`[Key]`** marks the unique identity property. Supports manual values or auto-generated ULIDs.
* **`[Queryable]`** makes a property as queryable via Linq. .

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

Lotta can store and retrieve POCO objects you don't own via fluent configuration.

* **SetKey()** define the property which is the key for storage and retrieve
* **AddQueryable()** - defines a property as queryable via Linq.

```csharp
public class BareNote
{
    public string NoteId { get; set; } = "";
    public string AuthorId { get; set; } = "";
    public string Content { get; set; } = "";
}

 var db = LottaDBFixture.CreateDb(options =>
 {
    options.Store<BareNote>(s =>
    {
        s.SetKey(n => n.NoteId);
        s.AddQueryable(n => n.AuthorId).NotAnalyzed();
        s.AddQueryable(n => n.Content);  
    });
 }
```


## Lotta Operations

| Operation            | Description                                       |
| -------------------- | ------------------------------------------------- |
| **SaveAsync<T>()**   | Save T instance using Upsert semantics            |
| **ChangeAsync<T>()** | Apply changes to T instance via lamda             |
| **DeleteAsync<T>()** | Delete T instance object                          |
| **QueryAsync<T>()**  | Query against table storage for objects of type T |
| **SearchAsync<T>()** | Search against lucene for objects of type T       |

### SaveAsync<T>() 

Save a POCO object into a Lotta DB using it's Key

```csharp
await db.SaveAsync<Actor>(actor);
```

### ChangeAsync<T>()

Apply change to T with ETag concurrency. It will fetch the object, call the lamda to change and attempt to save it with ETag concurrency. If the object fails, it will loop until it succeeds to mutate it.

```csharp
await db.ChangeAsync<Actor>(key, actor => actor.Movies++);
```

### DeleteAsync<T>()

Delete an object from a Lotta DB.

```csharp
await db.DeleteAsync<Note>(key);
await db.DeleteAsync<Note>(note);
```

### QueryAsync<T>()
Search table storage using linq
> NOTE: only filter passed to QueryAsync is processed server side by table storage and it needs to be on [Queryable] properties. 
> All other linq operations are processed client side after fetching the data from table storage.

```csharp
foreach(var actor in db.QueryAsync<Actor>(actor => actor.Age > 50)
{
    ...
}
```

### SearchAsync<T>()
Search lucene index using linq search syntax. 
> NOTE: only [Queryable] properties are searchable in lucene and string properties support full text search with Contains.

```csharp
foreach(var actor in db.SearchAsync<Actor>("name:bob*")
                       .Where(actor => actor.Age > 50)
{
    ...
}
```


## LINQ in Lotta

Lotta stores 2 representations of every object, one in **table storage** (for the truth), and one a **Lucene** index (for fast access). **Query()** gives you a Linq query over table storage and **.Search()** uses the **Linq To Lucene** library to query the search engine with linq expressions

### Query (Table Storage)

Filters on `[Queryable]` properties are executed by table storage server-side. 

```csharp
// All actors
var all = db.Query<Actor>().ToList();

// Server-side filter (AuthorId is [Queryable])
var aliceNotes = db.Query<Note>(n => n.AuthorId == "alice")
    .ToList();

// Predicate shorthand
var aliceNotes = db.Query<Note>(n => n.AuthorId == "alice").ToList();

// Polymorphic query -- returns Person and Employee
var people = db.Query<Person>().ToList();
```

### Search (Lucene)

Filters on `[Queryable]` properties are executed against Lucene catalog supporting full-text search with `Contains`.

```csharp
// Full-text search
var results = db.Search<Note>()
    .Where(n => n.Content.Contains("lucene"))
    .ToList();

// Exact match on NotAnalyzed field
var active = db.Search<Note>()
    .Where(n => n.AuthorId == "alice")
    .ToList();

// FREETEXT query
var results = db.Search<Note>("foo bar").ToList();

// Lucene Query syntax
var results = db.Search<Note>("Title:foo AND bar").ToList();
```

## Vector Similarity Search

LottaDB supports **vector similarity search** using embeddings. Mark string properties with `QueryableMode.Vector` and LottaDB will automatically generate embeddings at index time and support `.Similar()` queries for semantic search.

By default, LottaDB uses [ElBruno.LocalEmbeddings](https://github.com/elbruno/LocalEmbeddings) with the `SmartComponents/bge-micro-v2` model -- no external API calls needed. You can override this by setting `EmbeddingGenerator` on the configuration.

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
options.Store<Article>(s =>
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

**Object-level** -- search against the combined content of all analyzed string fields:
```csharp
// Semantic search across all text content (Title + Body + Category)
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

To use a different embedding model or an external API:
```csharp
var db = new LottaDB("myapp", connectionString, config =>
{
    config.EmbeddingGenerator = myCustomEmbeddingGenerator; // IEmbeddingGenerator<string, Embedding<float>>
    config.Store<Article>();
});
```

Set `EmbeddingGenerator` to `null` to disable vector support entirely.

## Triggers via `On<T>`

You can have code run when an object is saved/changed/deleted. That trigger can create new objects for cascading views.

Triggers run inline after each save or delete. They receive the object, the trigger kind (`Saved` or `Deleted`), and the full DB instance.

```csharp
options.On<Note>(async (note, kind, db) =>
{
    Console.WriteLine($"Note {note.NoteId} was {kind}");
});
```

### Materialized views via `On<T>`

You can use On<T> triggers to build derived objects that stay in sync automatically, whenever the trigger runs you can create/update/delete other objects.

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

options.On<Note>(async (note, kind, db) =>
{
    // maintain NoteView as materialized view that's updated when Note changes.
    if (kind == TriggerKind.Deleted)
    {
        await db.DeleteAsync<NoteView>(nv => nv.NoteId == note.NoteId);
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

