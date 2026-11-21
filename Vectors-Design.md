# Stored, queued embeddings

LottaDB embeds a `[Vector]` property inside the save, one document at a time, and keeps the vector only in the Lucene index. This note proposes that a vector be stored on the object's row, computed by a queued rule in batches, and copied into Lucene when the document is indexed.

Status: steps 1 and 2 built (2026-11-20, branch `stored-vectors`, not committed). A save never calls the model: one `embed` rule per type hierarchy embeds every stale field of an object in one call and writes it back once. Without queue storage, a save embeds new text itself. JSON documents still embed inside indexing, as before.

## Why

Today `VectorFieldMapper.CopyToDocument` (Lucene.Net.Linq) calls the embedding generator synchronously for each vector field of each document. It writes the result as the `{Field}_vector` binary doc-values field, and the vector is mapped to no property and stored nowhere else. That causes four problems:

| Problem | Effect |
| --- | --- |
| The model is called inside the save, under the index lock | A save waits for the model; a slow or remote generator slows every write, and a failing one fails the save |
| One call per field per document, also in `SaveManyAsync` | No batching, so a GPU or a remote API never reaches its throughput. Symplist Reno had to add `PrimedEmbeddingGenerator`, which embeds in batches before the save, as a workaround |
| The vector exists only in the index | `RebuildSearchIndex`, and so every schema change, embeds the whole corpus again |
| Identical text is embedded again | Re-saving an object whose text did not change, or `Materialize` re-producing the same passages, calls the model again |

## Design

### 1. Vectors live on the object's row

Each `[Vector]` property gets two columns on the object's own table row, plus one per row:

| Column | Contents |
| --- | --- |
| `_{Prop}_vector` | The vector bytes, in the format `VectorSerializer.ToBytesRef` writes to Lucene |
| `_{Prop}_hash` | Hash of (embedding model id + the property's text) |
| `_model` | Embedding model id (diagnostics and status) |

- The embedded `_content` field is treated the same way: `__content_vector` / `__content_hash`.
- A leading `_` is reserved for LottaDB columns (`_object`, `_type`, `_source`, `_produced`, and these). `Store<T>()` rejects a user property whose name starts with `_`.
- The columns are not part of the object's JSON and never reach the POCO, which is how `_source` and `_produced` behave already.
- Limits are generous: 64 KB per binary property (about 16,000 float32 dimensions) and 1 MB per row.

Because vectors share the object's row:

- a delete is one write;
- rebuild reads one row per object;
- there is nothing to orphan and no partition to clean up.

### 2. Saves carry vectors forward

`UpsertAsync` replaces the whole row (`TableUpdateMode.Replace`), so a save must write the vector columns again or lose them. The path to follow is the one `_produced` uses:

- **Carried with the object:** the vectors travel in `ObjectMetadata`, set when the object is read from its row.
- **Read when unknown:** if the object in hand was not read from its row (it is new to this process, or it came from a search), its row is read first (`LottaDB.Materialize.cs:95-99`).
- **Read when stale:** the row is also read when the vectors the object carries leave a field without one. The embed rule works on its own copy, so an object saved before its embedding ran still believes it has none; without this, saving that same object again would embed again.
- **Written back:** `EntityMapper.ToTableEntity` writes them back as columns (`EntityMapper.cs:27-28`).
- **Only while still valid:** a vector is written back only while its `_{Prop}_hash` still matches hash(model + current text). Otherwise it is dropped and the rule in §3 recomputes it.

### 3. Embedding is a rule

For every type with `[Vector]` properties, LottaDB registers:

```csharp
Rules.When<T>(x => VectorsStale(x)).ModifyQueued("embed", EmbedVectors);
```

- **`VectorsStale(x)`** is true when any vector field has no stored vector, or its stored hash does not match hash(model + current text). It is a condition on state, not an event:

| Case | Stale |
| --- | --- |
| Text edited | yes |
| An object saved before this feature (existing data) | yes |
| The embedding model changed | yes (the model is in the hash) |
| A retry of a vector that was set aside | yes |
| A save with the same text | no |

- **`EmbedVectors`** embeds the stale fields and writes their columns.
- **Free from the Rules machinery:** retries, set-aside, `StatusAsync` ("Note/embed: 1,240 waiting"), `DrainAsync` for tests, and `BackfillAsync` for existing data.
- **Write-back guard:** `ModifyQueued` writes back only if its inputs (the text) are unchanged. If the text moved on, the result is discarded; the newer save made the object stale again.
- Both `VectorsStale` and `EmbedVectors` are public. An app can register its own rule instead (another provider, a filter such as `n.IsPublished && VectorsStale(n)`, another batch size), and LottaDB then registers none.

### 4. Two additions to the Rules API

The built-in rule needs two things `ModifyQueued` does not have yet. Both are general features, not special cases for vectors.

1. **Batches.** Built for the `embed` rule only, not as a general batch handler: its handler is known (`EmbedVectors`), so the queue can do for many objects what the handler does for one. Any rule registered as `ModifyQueued("embed", EmbedVectors)`, LottaDB's own or an app's narrower one, has a queue of its own and is run up to 32 objects at a time:

   - **One call and one write.** Every stale text of the batch goes to the model in **one** `GenerateAsync` call (a text that occurs twice is embedded once). The rows are written in **one** transaction, each only if it is still the version that was read (`UpdateReplace` with its ETag), then indexed.
   - **Measured need.** In Symplist Reno, one passage at a time, the GPU sat at 1-3%: the time went to reading the row, writing it, indexing it and settling its message, not to the model.
   - **Per object, as before.** Each object has its own turn (no two runs for one object overlap; a batch takes its turns for all its objects at once, so batches cannot wait on each other in a circle), its own filter check, and its own message, settled by itself: retried and set aside on its own.
   - **Conflicts.** If any row was saved by someone else meanwhile, the transaction writes nothing, and each object is written by itself: the copy if its row is unchanged, otherwise the vectors put on the newer version. Storage without transactions takes the same path.
   - **Quantized models.** A full-precision model gives a text the same vector alone or in a batch (checked with all-MiniLM-L6-v2). A quantized one (`model_quantized.onnx`, as LottaDB's tests use) scales by the whole batch, so a stored vector can differ slightly from the same text embedded alone as a query: close (cosine above 0.95 in the tests), not equal.
   - **Not batched:** a drain with a limit (`DrainAsync(max)`), which runs one object at a time, and a message the batch cannot take as it is (a second one for the same object, a delete's), which is run by itself.

   A general batch handler for any rule (`ModifyQueued(name, (IReadOnlyList<T> items, ...) => ...)`) can still come later if another rule needs it.

2. **Internal writes that do not dispatch rules.** A `ModifyQueued` write-back is an ordinary save, so every unconditional rule on the type would run again. For example, `When<Message>().Materialize("passages", ...)` would re-produce every message's passages after its embedding. The embed rule writes only `_`-columns, which nothing user-visible depends on, so its write skips rule dispatch. The ETag still changes (see Costs).

### 5. Indexing copies, never computes

- **On indexing:** `VectorFieldMapper.CopyToDocument` stops calling the embedding generator. It is given the stored vectors and adds `{Field}_vector` when the stored hash matches the current text. It leaves the field out otherwise, and the object then is stale, so the rule runs.
- **Rebuild:** `RebuildSearchIndex` copies stored vectors. No model is called on rebuild, on a schema change, or on a re-save of unchanged text.
- **Query text:** `Similar("...")` still embeds its own search text synchronously. The user is waiting for that result, and it is one short string.

After the embed rule writes vectors, the document is re-indexed like any other save. Its new version lands in a small new segment, which gets its own HNSW graph; the old version is a deleted document in its old segment.

Updating the doc-value in place (`IndexWriter.UpdateBinaryDocValue`) is deliberately not used. Lucene.Net.Vector caches each segment's graph by the segment's core, and an in-place update keeps the core, so the approximate `Similar` would go on searching the old vectors. Re-indexing keeps every layer doing what it already does, with no changes to Lucene.Net.Vector.

## Costs and what users will notice

| | Before | After |
| --- | --- | --- |
| Save | Waits for the model | Returns without calling it |
| `Similar` on a new or edited object | Sees it immediately | Sees it once embedded; status shows what is waiting |
| Embedding calls | One per field per document | One per batch |
| Embedding failure | Fails the save | Retried, then set aside; the object stays saved and keyword-searchable |
| Rebuild, schema change | Embeds everything again | Copies stored vectors |
| Model change | Old vectors silently mixed with new ones | Everything becomes stale and is re-embedded through the queue |
| Table reads of a vector type | Object only | Object plus vector bytes (4 KB for 1,024 dimensions): free on SQLite, bandwidth on Azure scans. Searches are unchanged; they read Lucene's stored JSON |
| ETag | Changes when the user saves | Also changes when the embed rule writes. A user's own optimistic save (read, edit, save with ETag) can get a `ConcurrencyException` if an embedding finished in between, which can only happen shortly after the text changed. `ChangeAsync` and `ModifyQueued` already retry or merge |

## Build order

Each step is useful on its own.

| Step | What | Where | Status |
| --- | --- | --- | --- |
| 1 | Vector columns on the row, carried forward on save; indexing copies stored vectors; rebuild stops embedding | LottaDB (Lucene.Net.Linq unchanged) | Built |
| 2 | `VectorsStale`, `EmbedVectors`, the auto-registered `embed` rule (one item per call at first); saves stop calling the model | LottaDB | Built |
| 3 | The `embed` rule runs in batches of 32: one model call, one conditional write | LottaDB | Built |
| 4 | Rule-silent internal writes for `_`-columns | LottaDB | Proposed |

## Open questions

1. **`Similar` by vector.** With vectors stored, `Similar(float[])` and "more like this object" (`SimilarTo(key)`) cost almost nothing to add.
2. **Format versioning.** The stored bytes are `VectorSerializer`'s format. If it ever changes, put a version in `_model` (or the hash) so old rows read as stale rather than wrong.
3. **JSON-stored types.** Vector fields there are named by JSON path. Their column names need the same cleaning LottaDB applies to promoted JSON columns.
