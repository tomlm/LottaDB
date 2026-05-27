# LottaDB.SQLite

SQLite storage provider for [LottaDB](https://github.com/tomlm/LottaDB). Adds the `UseSQLite()` extension method for local development with a single portable `.db` file.

## Installation

```
dotnet add package LottaDB.SQLite
```

## Usage

```csharp
var catalog = new LottaCatalog("myapp", catalog => catalog.UseSQLite(@"C:\data"));
var db = await catalog.GetDatabaseAsync("default", config =>
{
    config.Store<MyEntity>();
});
```

All data is stored in a single `.db` file -- portable, atomic, and easy to manage.
