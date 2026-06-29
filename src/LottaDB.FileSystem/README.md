# LottaDB.FileSystem

Filesystem storage provider for [LottaDB](https://github.com/tomlm/LottaDB). Adds the `UseFileSystem()` extension method for local development with inspectable files on disk.

## Installation

```
dotnet add package LottaDB.FileSystem
```

## Usage

```csharp
var catalog = new LottaCatalog("myapp", catalog => catalog.UseFileSystem(@"C:\data\myapp"));
var db = await catalog.GetDatabaseAsync("default", config =>
{
    config.Store<MyEntity>();
});
```

Data is stored as files on disk -- human-inspectable and survives process restarts.
