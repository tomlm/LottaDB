# LottaDB.Memory

In-memory storage provider for [LottaDB](https://github.com/tomlm/LottaDB). Adds the `UseMemory()` extension method for fast unit testing with no disk I/O.

## Installation

```
dotnet add package LottaDB.Memory
```

## Usage

```csharp
var catalog = new LottaCatalog("myapp", catalog => catalog.UseMemory());
var db = await catalog.GetDatabaseAsync("default", config =>
{
    config.Store<MyEntity>();
});
```

All data lives in RAM and is lost when the process exits. Ideal for unit tests.
