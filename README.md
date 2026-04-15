![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB

**LottaDB**  is a .NET library that stores **POCO** => **Table Storage** and **Lucene** catalogs. 

* One line to save
* One line to query
* One line to search.

## Overview

**LottaDB** gives you a document database built on **Azure Table Storage** with automatic full-text search via **Lucene Search Engine**. Each LottaDB instance is a single database. Objects are stored with full POCO fidelity, while selected properties are promoted into Table Storage/Lucene for efficient querying using full typed Linq expressions.

Reactive handlers (`On<T>`) let you build materialized views, cascading updates, and side effects that run inline after each write.

## Why LottaDB?

- **A lotta bang for a little buck.** Table Storage is the cheapest durable storage in Azure. LottaDB adds Lucene so you get rich queries without the rich pricing.
- **A lotta LINQ.** `Query<T>()` and `Search<T>()` both return `IQueryable<T>`. Same `.Where()` expressions, two backends.
- **A lotta fidelity.** Full JSON roundtrip. Lists, dictionaries, nested objects -- everything survives. Queryable properties are promoted *alongside* the JSON, not instead of it.
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

- **Plain POCOs** -- no base classes, no interfaces. Just `[Key]` and `[Queryable]`.
- **Dual-backend querying** -- `Query<T>()` hits Table Storage (server-side filters), `Search<T>()` hits Lucene (full-text search). Both return `IQueryable<T>` with full LINQ support.
- **Full POCO roundtrip** -- objects are serialized as JSON. Complex properties (lists, dictionaries, nested objects) survive storage and retrieval intact.
- **Polymorphic queries** -- `Query<BaseClass>()/Search<BaseClass>()` returns all derived types, correctly deserialized.
- **Reactive handlers** -- `On<T>` handlers run inline after saves/deletes with full DB access. Build materialized views with plain C#.
- **Fluent or attribute configuration** -- annotate your models, or configure bare POCOs entirely via fluent API.
- **Per-tenant scaling** -- one LottaDB instance per tenant. All data bounded by tenant size.

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

// Configure and create the database
var options = new LottaConfiguration();
options.Store<Actor>();

var db = new LottaDB("myapp", tableServiceClient, luceneDirectory, options);

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

// Predicate shorthand
var results = db.Search<Note>(n => n.Content.Contains("search")).ToList();
```

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

You can use On<T> triggers to build derived objects that stay in sync automatically:

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
await db.ChangeAsync<Actor>(key, actor =>
{
    actor.DisplayName = "Alice Updated";
    return actor;
});
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

