# LottaDB Competitive Analysis

## What's genuinely novel

**`CreateView<T>(db => from...join...select)` is the standout.** Comparing to Marten, RavenDB, CosmosDB + Change Feed, EF Core, Event Store — none of them have declarative LINQ-based materialized view maintenance that extracts join keys and incrementally maintains itself.

- Marten has projections, but they're imperative C# classes
- RavenDB has map-reduce indexes, but they're a custom DSL
- CosmosDB requires manual Change Feed → Azure Function plumbing
- SQL Server has indexed views, but they're SQL and don't cascade
- EF Core doesn't even try

This is a genuinely new idea in this space. A developer looks at that LINQ expression and immediately understands what the view is. And they didn't have to write a builder class, wire up triggers, or think about what happens when Actor changes.

## What's strong

- **9-method API.** Save, Change, Delete, Get, Query, Search, Observe, RebuildIndex, Table. That's it. Compare to Cosmos SDK or EF Core's surface area.
- **`Store<T>` + `CreateView<T>` separation** is the right cut. Storage metadata vs. derivation logic. Clean.
- **The cost story.** Table Storage is pennies. Cosmos is dollars. For ActivityPub workloads (high read, moderate write, lots of denormalization) this is orders of magnitude cheaper.
- **Concurrency model is honest.** Save clobbers, Change retries. No pretending you have transactions when you don't.
- **Everything-is-an-object** eliminates a whole category of "where does this live?" questions.

## What concerns me

### 1. CreateView expression tree parsing is HARD

This is the biggest risk. Extracting join keys from arbitrary LINQ expressions and generating incremental maintenance logic is exactly what database query optimizers do — and they have teams of PhDs. EF Core's expression visitor is ~50K lines. We don't need to handle the general case (just joins + where + select), but even that subset has edge cases: composite keys, multiple joins, nullable navigation, type conversions.

**Recommendation:** ship explicit builders first, add CreateView as the v2 killer feature. The builder model works today. CreateView is the thing that makes people say "holy shit" — but it needs to be rock-solid when it ships, not half-baked.

### 2. Inline write amplification at scale

An Actor with 10,000 NoteViews gets updated. That's 10,000 table writes + 10,000 Lucene updates, inline, before `SaveAsync` returns. That could be seconds. The background dispatcher is mentioned but hand-waved. For the ActivityPub use case, a popular account updating their avatar could stall.

### 3. Single-writer Lucene

Lucene indexes are single-writer. Two app instances writing to the same `FSDirectory` = corruption. This is fine for a single-server app but blocks horizontal scaling. The doc mentions it in out-of-scope but it's a real deployment constraint that needs an answer before production.

### 4. No transactions across objects

Save a Note, builder saves a NoteView — two separate table operations. Crash between them = Note exists, NoteView doesn't. The error handling model (eventually consistent, retry via sink) is the right answer, but users coming from SQL will feel the gap.

## Where it sits in the market

| Solution | LottaDB's advantage | LottaDB's disadvantage |
|---|---|---|
| **Marten** (PostgreSQL) | CreateView > imperative projections. Cheaper (no PostgreSQL). | No transactions. No SQL. Less mature. |
| **RavenDB** | Not a server to operate. CreateView > map-reduce DSL. | RavenDB has built-in clustering, replication, full ACID. |
| **CosmosDB** | 10-100x cheaper. CreateView > manual Change Feed. Full-text search. | No geo-replication. No RU guarantees. Less mature. |
| **EF Core + SQL** | Document flexibility. Materialized views. | No joins at query time. No transactions. Limited ecosystem. |
| **Firebase/Firestore** | Richer queries (Lucene). Materialized views. | Firebase has real-time sync, auth, hosting — full platform. |

## Bottom line

The architecture is clean, the API is elegant, and `CreateView` is a genuine innovation. The risk is all in execution.

## Recommended shipping sequence

1. **v1:** `Store<T>`, `SaveAsync`/`GetAsync`/`QueryAsync`/`SearchAsync`, explicit `IBuilder<T,U>`, `Observe<T>`. This is usable today.
2. **v2:** `CreateView<T>` with expression tree parsing. This is the "wow" feature that differentiates.
3. **v3:** Background dispatcher for write amplification, multi-writer Lucene story.

Ship the useful thing, then ship the magical thing.
