[![Build and Test](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml/badge.svg)](https://github.com/tomlm/LottaDB/actions/workflows/BuildAndRunTests.yml)
[![NuGet](https://img.shields.io/nuget/v/LottaDB.svg)](https://www.nuget.org/packages/LottaDB)

![Logo](https://raw.githubusercontent.com/tomlm/LottaDB/refs/heads/main/icon.png)

# LottaDB

**LottaDB** is a .NET library that makes it easy to store any **POCO** in **Azure Table Storage** with full **Lucene** search, all with the goodness of **LINQ**.

- **A lotta bang for a little buck.** Table Storage is the cheapest durable storage in Azure. LottaDB adds Lucene so you get rich queries without the rich pricing.
- **A lotta LINQ.** `GetManyAsync<T>()` and `Search<T>()`, .Where(), .OrderBy() etc.
- **A lotta fidelity.** Full JSON roundtrip. Lists, dictionaries, nested objects -- everything survives.
- **A lotta views.** `On<T>` triggers build materialized views with plain C#.
- **A lotta tenants.** One catalog per tenant with multiple databases. Natural isolation, simple cleanup.
- **A lotta nothing to operate.** Table Storage is serverless. Lucene runs in-process.
- **A lotta schema safety.** Schema changes are detected automatically -- Lucene index is rebuilt on startup.

### Sweet spot

LottaDB is ideal for **per-user or per-tenant workloads** -- think user profiles, settings, activity feeds, personal knowledge bases, mailboxes, or per-project data. Thousands of objects per tenant, thousands of tenants per deployment.

## Installation

```
dotnet add package LottaDB
```

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

// Search (Lucene -- full-text search on [Queryable] properties)
var found = db.Search<Actor>()
    .Where(a => a.DisplayName == "Alice")
    .ToList();
```

## Documentation

Full documentation is available in the [wiki](https://github.com/tomlm/LottaDB/wiki):

- [Architecture: Catalogs and Databases](https://github.com/tomlm/LottaDB/wiki/Architecture) -- multi-tenancy, catalog settings
- [Modeling: Keys and Queryable Properties](https://github.com/tomlm/LottaDB/wiki/Modeling) -- attributes, fluent config, default search
- [CRUD Operations](https://github.com/tomlm/LottaDB/wiki/CRUD-Operations) -- Save, Get, Delete, Change, bulk operations
- [Search and LINQ](https://github.com/tomlm/LottaDB/wiki/Search-and-LINQ) -- full-text search, LINQ queries
- [Vector Similarity Search](https://github.com/tomlm/LottaDB/wiki/Vector-Search) -- embeddings, .Similar() queries
- [Dynamic Schemas](https://github.com/tomlm/LottaDB/wiki/Dynamic-Schemas) -- JSON Schema types, runtime schema updates
- [Object Metadata and Concurrency](https://github.com/tomlm/LottaDB/wiki/Object-Metadata) -- ETags, keys, cached JSON
- [Blob Storage](https://github.com/tomlm/LottaDB/wiki/Blob-Storage) -- upload, download, metadata extraction
- [Triggers and Materialized Views](https://github.com/tomlm/LottaDB/wiki/Triggers-and-Views) -- On<T> handlers, cascading views
- [Schema Migration](https://github.com/tomlm/LottaDB/wiki/Schema-Migration) -- automatic index rebuilds
