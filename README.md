[![Build and Test](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml)
[![NuGet](https://img.shields.io/nuget/v/LottaDB.svg)](https://www.nuget.org/packages/LottaDB)

![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB

**LottaDB** is a .NET library that makes it easy to store any **POCO** in **Azure Table Storage** with full **Lucene** search, all with the goodness of **LINQ**. No schema required -- just define a class and go.

- **A lotta power, a little work.** Store any C# class with zero attributes. All properties are automatically queryable and searchable.
- **A lotta bang for a little buck.** Table Storage is the cheapest durable storage in Azure. LottaDB adds Lucene so you get rich queries without the rich pricing.
- **A lotta LINQ.** `GetManyAsync<T>()` and `Search<T>()`, .Where(), .OrderBy() etc.
- **A lotta fidelity.** Full JSON roundtrip. Lists, dictionaries, nested objects -- everything survives.
- **A lotta views.** `On<T>` triggers build materialized views with plain C#.
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
| `[NotQueryable]` | Excludes a property from automatic indexing (useful for large strings). |
| `[DefaultSearch]` | (class-level) Sets the default property for free-text queries. |

## Concurrency

`ChangeAsync<T>()` provides safe read-modify-write with automatic retry on conflict:

```csharp
await db.ChangeAsync<Actor>("alice", actor =>
{
    actor.DisplayName = "Alice Updated";
});
```

Multiple concurrent writers on the same key are handled correctly -- ETag-based optimistic concurrency ensures no updates are lost.

## Storage Providers

LottaDB works with Azure Table Storage out of the box. For local development and testing, install a provider package:

| Package | Install | Usage | Description |
|---------|---------|-------| ---------|
| **LottaDB** | `dotnet add package LottaDB` | `catalog.UseAzure(connectionString)` | The default provider. Uses Azure Table Storage API for durability. |
| **LottaDB.Memory** | `dotnet add package LottaDB.Memory` | `catalog.UseMemory()` | In-memory provider for unit testing. No durability, but lightning fast and supports all features. |
| **LottaDB.SQLite** | `dotnet add package LottaDB.SQLite` | `catalog.UseSQLite(path)` | Local storage with SQLite. Durable and supports all features, but no serverless scaling. |

```csharp
// Local development with SQLite
var catalog = new LottaCatalog("myapp", catalog => catalog.UseSQLite(@"C:\data"));

// Unit tests with in-memory storage
var catalog = new LottaCatalog("myapp", catalog => catalog.UseMemory());
```

## Documentation

Full documentation is available in the [wiki](https://github.com/tomlm/LottaDB/wiki):
