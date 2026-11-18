# Rules: the design, and what comes next

Status: the design for rules in 6.0, with its reasoning. What is built is described in [Rules-Design.md](Rules-Design.md).

| Build step | State |
| --- | --- |
| 1. Per-type queue, `Then`, `Update`, `Changed(...)`, `RuleContext` | Built |
| 2. `Maintain`, `RefreshWhen`, backfill for projections | Not built |
| 3. `After`, `At` | Not built |
| 4. `Every` | Not built |

## The idea

The first draft of 6.0 had `.Queue(name, handler)`: a handler, a queue of its own, and settings for retries, priority and latest-only. That still made you think in queues.

This design replaces `.Queue` with a few verbs, one for each kind of deferred work. You say what the work is; LottaDB owns the queues, the pump, retries, poison handling and status.

| Verb | Kind of work | Example |
| --- | --- | --- |
| `Do` | Quick work inside the save | Update a counter field |
| `Update` | Slow work that changes the object itself | Embeddings, summaries, classification |
| `Maintain` | Other objects kept in sync with this one (projections) | Mention links, a `NoteView` joined to `Actor` |
| `Then` | Side effects outside the database | Email, webhooks, Graph writes |

Timing is separate from the verb:

- `.After(TimeSpan)`: some time after the event.
- `.At(item => DateTimeOffset)`: at a time taken from the object.
- `Rules.Every(...)`: on a schedule, with no object.

## Handlers

Every verb takes the same handler shape:

```csharp
public delegate Task RuleHandler<in T>(T item, RuleContext context, CancellationToken cancellationToken);
```

| `RuleContext` member | What it is |
| --- | --- |
| `Db` | The database the rule runs on |
| `Rule` | The rule's name, e.g. `summarize` |
| `Attempt` | 1 on the first run, 2 on the first retry, and so on (always 1 for `Do`) |
| `IdempotencyKey` | Built from the type, key, ETag and rule name; the same on every retry |

The item carries its key and ETag like any object LottaDB hands out: `item.GetKey()` and `item.GetETag()`. So the context does not repeat them.

`Do` takes it as well, so there is one delegate type for every verb.

## Model

A rule is **event, condition, action**, the model active databases use:

| Part | API |
| --- | --- |
| Event | `When<T>` (saved) or `WhenDeleted<T>` (deleted) |
| Condition | The expression filter: `When<Note>(n => Changed(n.Content))` |
| Action | `Do`, `Update`, `Maintain`, `Then` |
| Timing | `After`, `At` (optional) |

They are called rules, not triggers, because they never roll back the save: a rule that fails is retried or reported, and the object stays saved.

### Changes, not just states

A plain filter is true for every save while its condition holds. `When<Note>(n => n.IsPublished).Then(NotifyFollowers)` would email followers again each time a published note is edited. To react to a change, a filter uses `Changed(...)`:

```csharp
using static Lotta.Rule;

// Fires in the save where IsPublished turns true, not on later edits
config.Rules.When<Note>(n => Changed(n.IsPublished) && n.IsPublished).Then(NotifyFollowers);

// Runs only when its inputs change; this also tells Update what its inputs are
config.Rules.When<Note>(n => Changed(n.Content)).Update(Summarize);
config.Rules.When<Mail>(m => Changed(m.Subject, m.Body)).Update(Classify);
```

- **Meaning:** "any of these values differs in this save from the version that was loaded".
- **No extra storage.** An object LottaDB hands out already carries the JSON it was loaded with (`ObjectMetadata`), so a save has both the old and the new version in hand. Nothing is stored per rule or per object.
- **Checked on save, before queuing.** A save that changes none of a rule's `Changed(...)` values queues nothing for that rule.
- **A new object** counts as changed.
- **An object saved without having been loaded,** for example `new Note { NoteId = "n1", ... }` for a key that already exists, has no loaded version. If the type has rules that use `Changed`, LottaDB reads the stored version first, for that save only. Otherwise an edge-triggered `Then` could fire twice.
- **Works with every verb,** including inline `Do`, since it needs nothing beyond the save itself.
- **A marker, not a function.** `Changed` only means something inside a rule filter; called anywhere else, it throws.
- **Backfill** cannot evaluate `Changed` through the index, so it treats it as true and queries the rest of the filter.

## The verbs

### `Update`: change the object in the background

```csharp
config.Rules.When<Note>(n => Changed(n.Content))
    .Update(async (note, context, ct) =>
    {
        note.Vector = await embedder.EmbedAsync(note.Content, ct);
    });
```

- **You edit a copy.** The function changes the copy it is given; it returns nothing. LottaDB compares the copy with the original afterwards, and writes only if something changed.
- **Runs outside the write lock.** The slow part never blocks other writes.
- **Writes back safely.** The copy carries the original's ETag (`note.GetETag()`). At write-back LottaDB reads the object again:
  - **Same ETag:** nobody saved it meanwhile; the copy is written.
  - **Different ETag, inputs unchanged, outputs untouched:** someone saved the object without changing the `Changed(...)` values, here `Content`, and without touching the fields the function set, here `Vector`. The result is still valid, so only those fields are applied to the newer version, with a conditional write. On another conflict this step repeats, without calling the function again.
  - **Inputs changed:** the result is stale and is dropped. The newer save queued a run that sees the new content.
  - **Outputs changed by someone else,** say a user edited the field by hand: the result is dropped, and the user's edit wins.
  - **No `Changed(...)` in the filter:** LottaDB does not know the inputs, so any save in the meantime drops the result.
- **Frequent saves don't starve it.** Saves that don't touch the inputs no longer throw the work away.
- **Its own write doesn't trigger it again.** The write changes the outputs, not the inputs, so `Changed(n.Content)` is false. Two `Update` rules can't ping-pong for the same reason, and a nondeterministic summary isn't regenerated because some other field changed.
- **May read other objects** through `context.Db`, for example a list of categories while classifying. It writes only its own object; keeping other objects in sync is `Maintain`'s job.
- **No conflict loop.** The function runs once per run; it is not called again on a write conflict, unlike a `ChangeAsync` lambda.

| | `ChangeAsync` | `Update` |
| --- | --- | --- |
| Function | Synchronous | `async`: model calls, HTTP |
| Runs | Under the write lease | Outside any lock |
| On a conflict | Calls your function again on the new version | Applies the result to the newer version if its inputs are unchanged; otherwise drops it |
| Gets | The object | A copy of the object, and `RuleContext` |

Today the same work takes a handler that calls `ChangeAsync` and compares fields by hand to avoid writing a stale result. `Update` does that for you.

### `Maintain`: keep other objects in sync (projections)

```csharp
// One view per note (return null for none)
config.Rules.When<Note>()
    .Maintain(async (note, context, ct) =>
    {
        var author = await context.Db.GetAsync<Actor>(note.AuthorId, ct);
        return author == null ? null : new NoteView { Id = $"nv-{note.NoteId}", AuthorDisplay = author.DisplayName, Content = note.Content };
    })
    .RefreshWhen<Actor>(actor => note => note.AuthorId == actor.Username);

// Many objects per note
config.Rules.When<Note>()
    .Maintain(note => note.Mentions.Select(m => new Link { Id = $"{note.NoteId}->{m}", From = note.NoteId, To = m }))
    .Queued();
```

- **Inline by default, even when the projection reads other objects.** A `GetAsync` is a single table read, and inline is what makes views readable the moment the save returns, as with today's hand-written views. `.Queued()` runs the projection in the background; use it for projections that make many reads or do slow work.
- **You choose the keys.** Projected objects use their own `[Key]`, set by the projection, so a view can be read by a key you know (`nv-{noteId}`).
- **LottaDB records ownership.** Each projected object is stamped with its source, for example `(Note, n42)`, in an indexed `_source` field.
- **Each run diffs.** One index query finds what the source produced last time. LottaDB saves what is new or changed and deletes what is no longer produced. A note that stops mentioning someone loses that `Link`.
- **Deleting the source deletes everything it owns.** No snapshot of the deleted object is needed; the ownership field is enough.
- **One owner per object.** Producing a key that another source owns is an error.
- **Dependencies.** `.RefreshWhen<Actor>(actor => note => note.AuthorId == actor.Username)` says: when an actor changes, rerun the projection for the notes the expression matches. The expression becomes an index query. A refresh always runs queued, even when the projection runs inline: one actor can have thousands of notes, and that work does not belong inside the actor's save.
- **One writing instance.** Diffs, `RefreshWhen` and `Backfill` find objects through the search index. Within one process the index includes every write as it happens. With several processes writing one database, each one's index catches up with the others' writes only within `MaxSearchStaleness`, so a diff could miss objects or a refresh could miss sources. The first version assumes a single writing instance, as Symplist runs; keeping ownership in the table instead would lift that limit.
- **Rebuild.** `db.Rules.Backfill(...)` reruns a projection over every source, for example after its code changes.
- **Not covered: aggregates.** "Notes per author" is many sources feeding one object, which needs concurrent updates to combine. That is a different pattern, left for later.

Today (`src/LottaDB.Tests/CreateViewTestBase.cs`, `CascadingViewTestBase.cs`) the same `NoteView` takes two handlers:

- one on `Note`, with a hand-written delete branch;
- one on `Actor`, which searches for the affected views and builds each `NoteView` a second time.

Nothing removes objects a source stops producing, and nothing rebuilds views after the code changes.

### `Then`: side effects outside the database

```csharp
config.Rules.When<Note>(n => n.IsPublished)
    .Then(NotifyFollowers);

static async Task NotifyFollowers(Note note, RuleContext context, CancellationToken ct)
    => await mail.SendAsync(..., idempotencyKey: context.IdempotencyKey, ct);
```

- Delivered **at least once**, and retried with back-off; then poisoned.
- Given `context.IdempotencyKey`, built from the type, key, ETag and rule name. It is the same on every retry, so an external service can drop duplicates.

### `Do`: inline

Quick work inside the save: no name, no queue. Its handler takes `RuleContext` like the other verbs.

## Timing

```csharp
config.Rules.When<Invite>(i => Changed(i.SentAt) && !i.Accepted)
    .After(TimeSpan.FromDays(3))
    .Then(SendReminder);                     // accepted in the meantime: nothing is sent

config.Rules.When<Session>()
    .At(s => s.ExpiresAt)
    .Then(async (s, context, ct) => await context.Db.DeleteAsync(s, ct));

config.Rules.Every(TimeSpan.FromHours(24))
    .Then(SyncWithGraph);

config.Rules.Every("0 2 * * *", TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time"))
    .Then(NightlyCleanup);                   // 02:00 local time, every day
```

- **`After` and `At`** delay the action. The condition is checked again when it fires, on the object as it is then.
  - Queues can hide a message for at most 7 days (Azure's limit). A longer delay is chained: the message is hidden for 7 days, comes up, sees its time has not arrived, and is hidden again.
  - `After` is usually edge-triggered: `When<Invite>(i => Changed(i.SentAt) && !i.Accepted).After(TimeSpan.FromDays(3))`. Without `Changed`, every save of an unaccepted invite would schedule another reminder.
- **`Every`** runs on a schedule, with no object.
  - It takes a `TimeSpan`, or a cron expression with a time zone (UTC when none is given). Cron expressions are parsed by a small library such as Cronos, a new package dependency.
  - A schedule's handler gets no item: `(RuleContext context, CancellationToken ct)`.
  - Each schedule has one table row holding its next run time.
  - A process claims a run with an ETag-conditional update of that row, so exactly one process runs it. Every provider supports ETags on tables.
  - After downtime, a missed schedule runs once, not once per missed tick. Its next time is then set from now.

## Queues underneath

### One queue per type

- A save sends one message to the queue of the object's **concrete type**: saving a `BlobPhoto` uses the `BlobPhoto` queue, and runs the rules for `BlobPhoto` and for its base types, such as `BlobFile`.
- The message lists the rules that matched at save time: `{ key, etag, rules: ["summarize", "notify"] }`. It has to, because `Changed(...)` cannot be checked again later, once the old values are gone.
- The pump reads the object once and runs the listed rules, after checking the rest of each filter against the object as it is then.
- Compared with a queue per rule: one message and one read per save instead of one per rule, and adding or renaming a rule does not create or orphan a queue.

### A run, in phases

1. **`Update` rules.**
2. **Queued `Maintain` rules.**
3. **`Then` rules.**

When a rule saves the object its run is for (an `Update` write-back, for one), the run reads the object again and carries on, so the later phases run on the updated object, not a stale one. That save queues neither the rule that made it nor the rules the run has yet to do; it does queue the ones that already ran, if it matches them.

The first draft stopped the run after an `Update` wrote, and let the write's own message do the rest. Carrying on is simpler: no second message, and no marker in it for the rule to skip.

### Failures, one rule at a time

All retry state lives in the message itself. Example: object `X1` has rules A, B and C, and B fails.

1. Saving `X1` queues `{ key: X1, etag: "e7", rules: ["a", "b", "c"] }`.
2. The pump reads `X1` once and runs A, B and C. A and C succeed; B throws.
3. **The message is replaced in place,** in one `UpdateMessage` call that sets new content and a visibility delay:

   ```json
   { "key": "X1", "etag": "e7", "rules": ["b"], "attempts": { "b": 1 }, "lastError": { "b": "timeout calling model" } }
   ```

   It is hidden for B's retry delay. A and C are done and never run again for this ETag.
4. When it comes up again, only B runs, after its filter is checked against `X1` as it is then. Success deletes the message. Another failure raises the count and doubles the wait.

Replacing is one storage operation. Sending a new message and deleting the old one would be two, and a crash between them would leave both.

- **A rule that reaches its attempt limit** goes to the type's poison queue as `(type, key, rule, error)`, where it can be retried or discarded. It is then removed from the message; when no rules are left, the message is deleted.
- **A stale message still runs.** Suppose `e7` changed `Content` and queued `summarize`, then `e8` changed only the tags and queued nothing for `summarize`. Dropping the `e7` message would lose the summary. So a message whose ETag no longer matches the object is not dropped: its listed rules run on the object as it is now. `Update`'s write-back check keeps that safe, and a rule listed by two messages may run twice, which rules must tolerate anyway. ETags are opaque, so only equality is checked.
- **A crash during a run** leaves the message as it was. It becomes visible again when its lease runs out, and every rule in it runs again: at-least-once delivery. A later optimisation could update the message after each successful rule, so a crash repeats only the unfinished ones. That costs one more storage call per rule, so it is not the default.
- **Poison records are the only other storage.** There are no bookkeeping rows, per rule or per object.

### Rule identity

A retry names rules, so every queued rule needs a name that stays the same across builds and deploys.

- **Identity is `(type, name)`,** for example `Note/summarize`. Names only have to be unique within a type.
- **The default comes from the method group:** `.Update(Summarize)` is named `summarize`, taken from the argument's source text (`[CallerArgumentExpression]`).
- **A lambda must be named:** `.Then("notify", async (note, context, ct) => ...)`. An unnamed lambda throws at startup.
- **Inline `Do` needs no name.**
- **Ruled out:** identity from position ("the second rule on `Note`") or from a hash of the code. Both change silently when code is edited or reordered, and a waiting retry would run the wrong rule.

**Safety valve for renames and removals.** A retry that names a rule that no longer exists reruns all of the type's queued rules for that object. Rules must be safe to run twice anyway, so a rename or removal costs at most one extra run per waiting retry, never lost or misrouted work. Poison records with unknown names show in status as orphaned.

### No priority; concurrency instead

- With one queue per type, priority cannot mean what it does with a queue per rule, where a high-priority rule jumps ahead of other objects' work.
- Instead, a per-database `MaxConcurrency` processes several objects at once, so slow work stops blocking fast work.
- Two runs for the same object never overlap.
- Priority can come back if a real need appears. So can `.Isolated()`, giving one slow rule a queue of its own.

## No public raw queues

Work that is not "an object changed" is modelled as an object:

```csharp
config.Rules.When<SyncJob>(j => j.State == JobState.Pending)
    .Update(RunSyncJob);

await db.SaveAsync(new SyncJob { Folder = "Inbox" });
```

The job is queryable and durable like any other object, and its progress is visible in its own fields. The cost is a table write per job on top of the queue message. Raw queues (`db.Queues`) are left out unless very high volumes of short-lived messages need them.

## Operating it

You see rules, not queues. Waiting work is counted per type, not per rule: each message lists its rules, so splitting the count by rule would mean reading every message.

| Method | What it does |
| --- | --- |
| `db.Rules.StatusAsync()` | Per rule: runs in progress, poisoned count, last error. Per type: messages waiting (approximate). Orphaned poison records. |
| `db.Rules.RetryAsync(rule)` | Retry a rule's poisoned work |
| `db.Rules.DiscardAsync(rule)` | Delete a rule's poisoned work |
| `db.Rules.BackfillAsync(rule)` | Run a rule over every object it matches now, found through the index when the filter allows |
| `db.DrainAsync()` | Run what is due now (manual mode, tests) |

## Indexing

Search indexing stays inline, so a search inside a rule already sees the change that triggered it. Embedding generation is the one part that may move to a queue later: text is searchable at once, and vectors follow.

## Build order

Each step is usable on its own:

1. Per-type queue, with `Then` and `Update`.
2. `Maintain`, `RefreshWhen` and `Backfill`.
3. `After` and `At`.
4. `Every`.

From the first draft of 6.0, the grammar, expression filters, `Do`, `db.Rules`, and index-first ordering stay. `.Queue`, its modifiers, and the queue-level operations (`PauseQueue`, `MoveQueuedAsync`, `DeleteQueueAsync`) are gone.

## Hard parts

- **At least once, everywhere.** Every verb can run twice, so `Update` and `Maintain` are safe to repeat by design, and `Then` gets an idempotency key.
- **The `Update` guard.** The ETag check, applying a result to a newer version when its inputs are unchanged, and reading the object again after a write so later phases see the new object.
- **`Changed` on unloaded objects.** The extra read when an object is saved without having been loaded, only for types with `Changed` rules.
- **Projection diffs.** The cost of each diff on objects that produce many views.
- **Concurrency.** Two runs for the same object must not overlap.
- **Scope.** This is not a workflow engine. A multi-step process is a chain of objects and rules, not new verbs.

## Decided

- **Handler shape:** `(item, RuleContext context, ct)` for every verb, including `Do`.
- **`RuleContext`:** `Db`, `Rule`, `Attempt`, `IdempotencyKey`. Key and ETag come from the item (`GetKey()`, `GetETag()`), the names the existing API uses.
- **`Update`:** edits a copy; written only if changed, conditional on the copy's ETag; may read other objects through `context.Db`.
- **`Maintain`:** inline by default even when it reads other objects; `.Queued()` for heavy projections.
- **`Every`:** `TimeSpan` and cron with a time zone (UTC by default) in the first version.
- **`StatusAsync()`:** per rule (running, poisoned, last error), plus approximate waiting counts per type.
- **Messages carry the ETag,** compared for equality only, and the rules that matched at save time. A stale message runs its rules on the current object rather than being dropped.
- **`Changed(...)` in filters** for edge-triggering and for declaring `Update`'s inputs: compared with the loaded version at save time, no per-rule or per-object state.
- **Queues per concrete type,** running rules for the type and its base types.
- **`RefreshWhen` always queued.** Projections assume one writing instance for now.
- **Delays beyond 7 days are chained.**
