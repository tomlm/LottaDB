# LottaDB Analysis

## What it is

A .NET library that stores POCOs in Azure Table Storage and indexes them in Lucene. One class (`LottaDB`), one table, one index, per-tenant instances. 98 passing tests.

## What makes it good

**The API is tiny.** 8 methods on the main class: `SaveAsync`, `DeleteAsync`, `ChangeAsync`, `GetAsync`, `Query`, `Search`, `On`, `RebuildIndex`. That's the entire surface. Compare to CosmosDB SDK, EF Core, or Marten — orders of magnitude less surface area.

**`On<T>` is the right primitive.** One concept replaced three (IBuilder, CreateView, Observe). A lambda with full DB access handles everything: materialized views, cascading deletes, notifications, projections. No interfaces to implement, no expression trees to parse, no magic. The user writes the logic they want, explicitly.

**`_json` everywhere.** Both table storage and Lucene store the full POCO as JSON. `Query<T>()` and `Search<T>()` both return complete objects — nested collections, dictionaries, decimals all survive the roundtrip. No "Lucene returns partial objects" problem.

**`_type` hierarchy.** Polymorphic queries work: `Query<BaseClass>()` returns derived types with full fidelity. Deserialization resolves the concrete type from `_type` field. DocumentKey in Lucene scopes `Search<T>()` to the correct type.

**DI-neutral.** Constructor takes dependencies directly. No service provider required. Optional `AddLottaDB` extension for DI users.

**Predicate-based delete.** `DeleteAsync<T>(n => n.AuthorId == "alice")` queries and deletes matching objects, running `On<T>` handlers for each. Same pattern as LINQ terminal operators.

**The cost story.** Azure Table Storage is pennies. Lucene is local disk. For per-tenant workloads this is orders of magnitude cheaper than CosmosDB.

## What's honest about the limitations

**Lucene is local.** Single-writer, single-process. Scaling is horizontal (one LottaDB instance per tenant), not vertical. No distributed search.

**`[Key]` + `[Field(Key=true)]` duplication.** Two attributes on the same property — one for LottaDB, one for Lucene. Needs a fix in `Iciclecreek.Lucene.Net.Linq` to unify.

**No transactions.** Save a Note and its NoteView are two separate table operations. Crash between them = inconsistent state. `On<T>` handlers are eventually consistent — errors are captured but the source save succeeds.

**Search query translation.** Lucene's LINQ provider doesn't support `string.Contains()`. Analyzed fields need `==` for term queries. Full-text search semantics differ from LINQ-to-Objects.

**`IDocumentMapper` wrapping is fragile.** The `LottaDocumentMapper` wrapper works, but we discovered that wrapping breaks the provider in certain configurations. The current implementation works but needs careful testing if `Iciclecreek.Lucene.Net.Linq` is updated.

## Where it sits vs alternatives

| Library | LottaDB advantage | LottaDB disadvantage |
|---------|------------------|---------------------|
| **CosmosDB** | 10-100x cheaper. `On<T>` > Change Feed plumbing. Full-text search. | No geo-distribution. No SLAs. |
| **EF Core + SQL** | Document flexibility. No schema migrations. Simpler for denormalized data. | No SQL. No transactions. Smaller ecosystem. |
| **Marten** | No PostgreSQL server. `On<T>` > imperative projections. Cheaper. | No transactions. Less mature. |
| **RavenDB** | No server to operate. `On<T>` > map-reduce DSL. | No clustering, no ACID. |

## Architecture summary

```
LottaDB instance
├── Azure Table Storage (one table)
│   ├── PartitionKey = type name
│   ├── RowKey = [Key] value
│   ├── _json = full POCO
│   └── _type = type hierarchy
├── Lucene Index (one directory)
│   ├── [Field] properties indexed for search
│   ├── _json stored for full POCO deserialization
│   └── _type DocumentKey for type scoping
└── On<T> handlers
    ├── Run inline after every save/delete
    ├── Full DB access (save, delete, query, search)
    ├── Cycle detection via AsyncLocal
    └── Changes aggregate into root ObjectResult
```

## Test coverage

98 tests across 15 test files:
- CRUD (save, get, delete by key/entity/predicate)
- Change (read-modify-write)
- Query (table storage, tags, polymorphic)
- Search (Lucene, field values, type scoping)
- On<T> handlers (create/delete derived objects, error handling, cascading)
- Cycle detection
- JSON roundtrip (Query AND Search, nested collections, dictionaries, decimals)
- Polymorphism (Query returns derived types with concrete deserialization)
- Rebuild index
- Ad-hoc joins
- Store registration (attributes, fluent, custom keys)

## What's next

1. **Unify `[Key]` and `[Field(Key=true)]`** — fix in Iciclecreek.Lucene.Net.Linq so one attribute handles both
2. **Lucene query string support** — `Search<T>("content:lucene*")` parameter is accepted but not yet wired to the query
3. **Polymorphic Search** — `Search<BaseClass>()` scopes by concrete type via DocumentKey, but doesn't yet return all derived types (needs multi-value DocumentKey support)
4. **Batch operations** — session-based writes for bulk import performance
5. **Background `On<T>` dispatch** — optional async handlers for high-throughput scenarios
