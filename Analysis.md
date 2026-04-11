# LottaDB Analysis

## What makes this different

**`CreateView<T>(db => from...join...select)`** — declarative materialized views as LINQ joins, incrementally maintained. No other library in this space does this:

- Marten: imperative C# projection classes
- RavenDB: custom map-reduce DSL
- CosmosDB: manual Change Feed → Azure Function wiring
- SQL Server: indexed views in SQL, don't cascade
- EF Core: no materialized view concept

A developer reads the LINQ expression and immediately understands the view. No builder classes, no trigger wiring, no "what happens when Actor changes" — LottaDB infers it from the join keys.

The execution model is clean: `Iciclecreek.Lucene.Net.Linq` handles each Lucene query, standard LINQ does the in-memory hash join. The only custom work is walking the expression tree at registration time to extract join keys. This is bounded work, not an open-ended query translation problem.

## Strengths

- **9-method API.** Save, Change, Delete, Get, Query, Search, Observe, RebuildIndex, Table. Compare to Cosmos SDK or EF Core's surface area.
- **`Store<T>` + `CreateView<T>` separation.** Storage metadata vs. derivation logic. One is about how to persist and index; the other is about relationships between objects. Clean cut.
- **Everything is an object.** No entity/view split. Derived objects are stored and indexed like any other. Cascading views fall out naturally. Eliminates "where does this live?" questions.
- **Cost.** Table Storage is pennies. Cosmos is dollars. For read-heavy, moderate-write workloads this is orders of magnitude cheaper.
- **Concurrency model.** `SaveAsync` clobbers (fast path). `ChangeAsync` retries with ETags (safe path). Honest about what it offers — no fake transactions.
- **Attributes + fluent.** `[PartitionKey]`, `[RowKey]`, `[Tag]`, `[Field]` on the POCO for the common case. `Store<T>(s => ...)` for overrides. Convention defaults fill gaps. Three tiers, one mental model.
- **Ad-hoc joins at query time.** `SearchAsync<Note>() join SearchAsync<Actor>()` works naturally via client-side hash join since Lucene is in-process. CreateView for hot paths, ad-hoc joins for everything else.
- **Fully in-process testing.** `Spotflow.InMemory.Azure.Storage` for Table Storage fakes + Lucene `RAMDirectory` for search. No Docker, no Azurite, no external processes. Unit tests run fast and anywhere.

## Scaling model

LottaDB targets **small-to-medium workloads** — per-user, per-tenant, per-instance. Scaling is horizontal by creating **separate LottaDB instances per tenant**, not by scaling a single database.

This is deliberate. Within a tenant-scoped instance:
- Write amplification is bounded by one tenant's data
- Single-writer Lucene is the natural model — one process per instance
- Transaction scope is small
- Cost scales linearly, each tenant's footprint is cheap

Fits: ActivityPub instances, per-user personal apps, per-tenant SaaS, edge/offline nodes.

## Concerns

### 1. Expression tree walking for CreateView

The execution model is simple (just LINQ), but the **registration-time expression tree walker** still needs to handle: composite join keys, multiple joins in one expression, `where` clauses that filter before the join, nullable properties, and type conversions in the `on` clause. This is a constrained problem but it has real edge cases. Good test coverage is critical — a bug here means views silently don't rebuild when they should.

### 2. Fan-out on popular objects

Within the per-tenant scaling model, some objects are still "hot." An ActivityPub Actor with 5,000 Notes means an Actor display name change triggers 5,000 NoteView rebuilds inline. Even at 1ms per rebuild, that's 5 seconds blocking `SaveAsync`. The architecture mentions a background dispatcher but doesn't define it. This needs a concrete design before production use with any object that has high fan-out.

### 3. Lucene index durability

Lucene indexes are "disposable" (rebuildable from table storage), but rebuilding a large index is slow. If the Lucene directory is on ephemeral storage (containers, app restarts), frequent rebuilds degrade the experience. The architecture needs guidance on when to use persistent vs. ephemeral directories and what the cold-start story looks like.

### 4. Builder failure retry semantics

`IBuilderFailureSink` captures failures, but the retry path isn't defined. What does a retry look like? Re-save the source object? Re-run just the failed builder? What if the source object has changed since the failure? This needs a concrete design — "retry later" is too vague for production reliability.

## Competitive positioning

| Solution | LottaDB advantage | LottaDB disadvantage |
|---|---|---|
| **Marten** (PostgreSQL) | Declarative CreateView > imperative projections. No server to operate. Cheaper. | No transactions. No SQL. Less mature. |
| **RavenDB** | No server. CreateView > map-reduce DSL. Cheaper. | No clustering, no replication, no ACID. |
| **CosmosDB** | 10-100x cheaper. CreateView > Change Feed plumbing. Full-text search built in. | No geo-distribution. No SLAs. Single-region. |
| **EF Core + SQL** | Document flexibility. Materialized views. Simpler for denormalized data. | No ad-hoc joins at query time beyond search. No transactions. Smaller ecosystem. |
| **Firebase/Firestore** | Richer queries via Lucene. Materialized views. | Firebase is a full platform (auth, hosting, real-time sync). LottaDB is just storage. |

## Shipping sequence

1. **v1:** Full API — `Store<T>`, `SaveAsync`/`ChangeAsync`/`DeleteAsync`/`GetAsync`/`QueryAsync`/`SearchAsync`, `CreateView<T>`, `IBuilder<T,U>`, `Observe<T>`, `RebuildIndex<T>`. CreateView expression tree walker scoped to single joins + where + select.
2. **v2:** Background dispatcher for fan-out. Builder retry mechanics. Multi-join CreateView support. Cold-start / rebuild performance.
3. **v3:** Multi-instance coordination. Distributed Lucene (or alternative search backend).
