# Rules

Status: as built in 6.0, for every built-in storage provider: Memory, SQLite, FileSystem, Lucene and Azure. Rules replace `On<T>` and `OnAsync<T>`. The reasoning behind the design, and the parts not built yet, are in [Rules-Next.md](Rules-Next.md).

## Summary

A rule says what to do when an object is saved or deleted. Rules are like database triggers, except that they never roll back the save: a rule that fails is reported or retried, and the object stays saved.

A rule is **event, condition, action**:

| Part | API |
| --- | --- |
| Event | `When<T>` (saved) or `WhenDeleted<T>` (deleted) |
| Condition | An optional expression filter: `When<Note>(n => n.IsPublished)` |
| Action | A verb: `Do`, `Update`, `Maintain`, `Then` |

The verb says what kind of work it is. LottaDB owns the queues, the pump, retries and status.

| Verb | Kind of work | Runs | On failure |
| --- | --- | --- | --- |
| `Do` | Quick work | Inside the save, under the write lease | Reported in the save's `ObjectResult.Errors` |
| `Update` | Slow work that changes the object itself | Later, outside the write lease | Retried with back-off, then set aside |
| `Maintain` | Other objects kept in step with this one | Inside the save, or later with `.Queued()` | In the save's `Errors`, or retried when queued |
| `Then` | Side effects outside the database | Later, outside the write lease | Retried with back-off, then set aside |

```csharp
using static Lotta.Rule;

config.Rules.When<Note>().Do(UpdateFeedView);                                 // in the save
config.Rules.When<Note>(n => Changed(n.Content)).Update(Summarize);           // rule "Note/summarize"
config.Rules.When<Note>().Maintain(note => new NoteView { Id = $"nv-{note.NoteId}", Content = note.Content });   // rule "Note/note-view"
config.Rules.When<Note>(n => Changed(n.IsPublished) && n.IsPublished)
      .Then("notify", async (note, context, ct) =>
          await mail.SendAsync(note, idempotencyKey: context.IdempotencyKey, ct));
config.Rules.WhenDeleted<Note>().Do(RemoveFeedView);

static async Task Summarize(Note note, RuleContext context, CancellationToken ct)
{
    note.Summary = await model.SummarizeAsync(note.Content, ct);   // edits a copy; LottaDB writes it back
}
```

## Registration

```csharp
public delegate Task RuleHandler<in T>(T item, RuleContext context, CancellationToken cancellationToken);

// config.Rules (RuleSet)
RuleBuilder<T>        When<T>(Expression<Func<T, bool>>? filter = null);
DeletedRuleBuilder<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null);
ScheduleBuilder       Every(TimeSpan interval);
ScheduleBuilder       Every(string cron, TimeZoneInfo? timeZone = null);

// RuleBuilder<T>: each returns the builder, so one rule can have several actions
RuleBuilder<T> Do(RuleHandler<T> handler);
RuleBuilder<T> Update(string name, RuleHandler<T> handler);
RuleBuilder<T> Update(RuleHandler<T> handler);   // named after the method
RuleBuilder<T> Then(string name, RuleHandler<T> handler);
RuleBuilder<T> Then(RuleHandler<T> handler);     // named after the method
RuleBuilder<T> After(TimeSpan delay);                    // delays the Then and Update actions that follow
RuleBuilder<T> At(Func<T, DateTimeOffset?> time);        // runs them at a time taken from the object

// Maintain: projection is (item) or (item, context, ct), returning TView?, IEnumerable<TView> or a Task of either
MaintainBuilder<T> Maintain<TView>(string name, projection);
MaintainBuilder<T> Maintain<TView>(projection);  // named after the method, or the type a lambda produces

// MaintainBuilder<T>
MaintainBuilder<T> Queued();
MaintainBuilder<T> RefreshWhen<TOther>(Func<TOther, Expression<Func<T, bool>>> sources);

// ScheduleBuilder: handler is (RuleContext context, CancellationToken ct)
ScheduleBuilder Then(string name, ScheduleHandler handler);
ScheduleBuilder Then(ScheduleHandler handler);   // named after the method

// DeletedRuleBuilder<T>
DeletedRuleBuilder<T> Do(RuleHandler<T> handler);

// db.Rules (LiveRuleSet): inline only; Do returns an IDisposable that removes the rule
LiveRule<T>        When<T>(Expression<Func<T, bool>>? filter = null);
LiveDeletedRule<T> WhenDeleted<T>(Expression<Func<T, bool>>? filter = null);
```

- **A rule registers when `When` is called.** A rule with no action makes opening the database throw `InvalidOperationException` naming the type.
- **Rules are read when the database opens.** Adding to one after that throws `InvalidOperationException`.
- **A rule for a base type also fires for derived types,** from every save path: `When<BlobFile>` fires for an uploaded `BlobPhoto`.
- **`JsonDocument` objects** are not given to queued rules.
- **Deletes have inline rules only:** `WhenDeleted<T>(filter).Do(...)`, given the object as it was.

### Handlers

Every verb takes the same handler shape, `(item, context, cancellationToken)`.

| `RuleContext` member | What it is |
| --- | --- |
| `Db` | The database the rule runs on |
| `Rule` | The rule's name, such as `summarize`; empty for `Do` |
| `Attempt` | 1 on the first run, 2 on the first retry, and so on; always 1 for `Do` |
| `IdempotencyKey` | A hash of the type, key, ETag and rule; the same on every retry |

The item carries its key and ETag like any object LottaDB hands out: `item.GetKey()`, `item.GetETag()`.

### Rule names

A retry names the rule to run again, so every rule that can run from a queue (`Update`, `Maintain`, `Then`) needs a name that stays the same across builds.

- **Identity is the type and the name:** `Note/summarize`. Names are unique within a type, whatever the verb.
- **A name** is 3 to 40 lower-case letters and digits, with single hyphens between them.
- **The default comes from the method:** `.Update(Summarize)` is `summarize`, `handlers.NotifyFollowersAsync` is `notify-followers`, `Handle<Note>` is `handle`. It is taken from the source text of the argument (`[CallerArgumentExpression]`).
- **A lambda must be named:** `.Then("notify", async (note, context, ct) => ...)`. Without a name it throws `ArgumentException` when the rule is registered.
- **A `Maintain` lambda is named after the type it produces:** `NoteView`, or a collection of them, gives `note-view`. Two lambdas producing one type for one source type must be named.
- **Inline `Do` has no name.**

### Filters

The filter says which objects the rule is for. Without one, every object of the type. It is an expression, compiled once.

| Verb | When the filter is checked |
| --- | --- |
| `Do` | Before the handler, in the save |
| `Update`, `Then` | In the save: false queues nothing. Again when the rule runs, on the object as it is then: false skips the rule, which is not counted as a run |
| `Maintain` | The same moments, but false (apart from its `Changed(...)` parts) deletes what the object produced |

- **Quick and pure:** it runs inside the save, under the write lease, so it should look only at the object.
- **When it throws** in a save, the error goes into the save's `Errors` and the save still happens. When a queued rule comes up, it counts as a failed run.

### `Changed(...)`

A plain filter is true for every save while its condition holds. To react to a change, a filter uses `Changed(...)` (`using static Lotta.Rule;`):

```csharp
config.Rules.When<Note>(n => Changed(n.IsPublished) && n.IsPublished).Then(NotifyFollowers);
config.Rules.When<Mail>(m => Changed(m.Subject, m.Body)).Update(Classify);
```

- **Meaning:** any of these values differs in this save from the version the save replaces.
- **No extra storage.** An object LottaDB handed out carries the JSON it was loaded with, so a save has both versions. Nothing is stored per rule or per object.
- **A new object** counts as changed.
- **An object saved without having been loaded** (`new Note { NoteId = "n1" }` for a key that exists) has no loaded version. For types that have a `Changed` rule, the save reads the stored version first.
- **Works with every verb,** inline `Do` and live rules included.
- **When a queued rule runs,** `Changed(...)` is taken to be true: the old values are gone by then. The rest of the filter is checked on the object as it is.
- **It also names an `Update` rule's inputs** (see below).
- **A marker, not a function.** Called anywhere but a rule filter, it throws. `Changed()` with no values, and `Changed` in a `WhenDeleted` filter, throw when the rule is registered.

## The verbs

### `Do`: in the save

- Runs after the object is written and indexed, so a search inside the handler already sees the change. The built-in index handlers run before any rule, for the object's type and its base types.
- What it saves is part of the save's `ObjectResult.Changes`; what it throws is in `ObjectResult.Errors`.
- A chain of `Do` rules stops when it reaches a type already being handled in the chain.
- On an open database: `using var rule = db.Rules.When<Note>().Do(Refresh);`

### `Update`: change the object in the background

- **The handler edits a copy** and returns nothing. LottaDB compares the copy with the original afterwards and writes only if something changed.
- **Runs outside the write lease,** so slow work never blocks other writes.
- **The handler is called once per run.** It is not called again on a write conflict, unlike a `ChangeAsync` lambda.
- **It may read other objects** through `context.Db`.

The write-back is conditional on the ETag the copy was made from. If someone else saved the object meanwhile:

| What happened meanwhile | The result |
| --- | --- |
| Nothing | Written |
| A save that left the `Changed(...)` values alone, and did not touch the properties the handler set | The handler's properties are applied to the newer version, with a conditional write (repeated on another conflict, up to 10 times) |
| A save that changed a `Changed(...)` value | Dropped: that save queued a fresh run |
| A save that changed a property the handler set | Dropped: the other writer's value stands |
| The filter has no `Changed(...)` | Dropped: LottaDB does not know the inputs |

"The properties the handler set" are the top-level properties that differ between the original and the copy.

### `Maintain`: keep other objects in step

```csharp
// One view per note, built with a read of another object
config.Rules.When<Note>()
    .Maintain(ProjectNoteView)                                             // rule "Note/project-note-view"
    .RefreshWhen<Actor>(actor => note => note.AuthorId == actor.Username);

// Several objects per note, in the background
config.Rules.When<Note>()
    .Maintain(note => note.Mentions.Select(m => new Link { Id = $"{note.NoteId}->{m}", From = note.NoteId, To = m }))
    .Queued();                                                             // rule "Note/link"

static async Task<NoteView?> ProjectNoteView(Note note, RuleContext context, CancellationToken ct)
{
    var author = await context.Db.GetAsync<Actor>(note.AuthorId, ct);
    return author == null ? null : new NoteView { Id = $"nv-{note.NoteId}", AuthorDisplay = author.DisplayName, Content = note.Content };
}
```

The projection returns what should exist for the object: one object, a collection, or null for nothing. It may be synchronous `(item)` or asynchronous `(item, context, ct)`.

- **Each run compares.** LottaDB finds what the object produced before, saves what is new or different, and deletes what is no longer produced. An object that is the same is not written again.
- **Inside the save by default.** What it saves and deletes is in the save's `Changes`; what it throws is in `Errors`. `.Queued()` runs it from the queue instead, with retries like `Then`.
- **Deleting the object deletes what it produced,** inside the delete, also for a `.Queued()` rule.
- **The filter says which objects have a projection.** When it becomes false for an object, what the object produced is deleted. A filter that is false only in its `Changed(...)` part skips the run and deletes nothing.
- **You choose the keys.** A produced object is stored under its own `[Key]`, which the projection sets and keeps the same from run to run.
- **One owner per object.** Producing a key that another object or another rule owns is an error. An object saved by hand, or left by a rule that no longer exists, is taken over.
- **A projection cannot produce objects of the type it is for.**
- **Produced objects are ordinary objects:** their own rules run when they are saved and deleted.

**Ownership** is a `_source` value stored and indexed beside each produced object, next to `_type` and the ETag, not in the object's JSON: the rule and the key of the object it was produced from, `Note/note-view n42`. Nothing is added to the source object. Loading a produced object and saving it keeps the value; `RebuildSearchIndex` restores it in the index.

**`RefreshWhen<TOther>(other => item => ...)`** runs the projection again when an object it reads changes.

- Every save or delete of a `TOther` evaluates the condition and queues the rule for each object it is true for.
- The objects are found through the search index when it can express the condition (`[Queryable]` properties compared with values), and by reading every object when it cannot.
- **A refresh always runs from the queue,** also for a rule that otherwise runs inside the save: one actor can have thousands of notes.
- A run from the queue takes the object's lock and reads it again before projecting, so a refresh never overwrites the result of a newer save.

**`db.Rules.BackfillAsync(rule)`** runs a projection again for every stored object its filter is true for, for example after its code changes.

### `Then`: side effects outside the database

- Delivered **at least once**: retried with back-off, then set aside.
- `context.IdempotencyKey` is the same on every retry, so an outside service can drop duplicates.

## Timing: `After` and `At`

```csharp
config.Rules.When<Invite>(i => Changed(i.SentAt) && !i.Accepted)
    .After(TimeSpan.FromDays(3))
    .Then(SendReminder);                     // accepted in the meantime: nothing is sent

config.Rules.When<Session>()
    .At(s => s.ExpiresAt)
    .Then(EndSession);
```

`After` and `At` delay the `Then` and `Update` actions that follow them on the rule. `Do` and `Maintain` cannot be delayed; either one after `After` or `At` throws when the rule is registered.

| | When it runs | When the time comes |
| --- | --- | --- |
| `After(delay)` | `delay` after the save that matched the filter | The filter is checked again, without its `Changed(...)` parts; false drops the work |
| `At(item => time)` | At the time the object gives in that save; a time already past runs as soon as it can; null queues nothing | The filter is checked again, and the time is read again: a later time waits for it, null drops the work |

- **Every matching save starts a wait of its own.** So an `After` filter usually names the change that starts it with `Changed(...)`; without it, each save of an unaccepted invite would schedule another reminder.
- **A delayed rule has a message of its own,** with the time in it (`"due"`), hidden until then. The rules of the save that are not delayed go in the usual message.
- **Waits longer than 7 days** (the longest a queue hides a message) are made in steps: the message comes up, is not due yet, and is hidden again with nothing run.
- **A changed `At` time.** A save that gives a new time queues a new message; the old one comes up at the old time, finds the later time, and waits for it too. So the action can run more than once for one time, like any queued rule.
- **Failures** are retried and set aside like any queued rule.
- **`BackfillAsync` and `RetryAsync`** run an `After` rule at once, since there is no save to count from. An `At` rule still waits for the object's time.

## Schedules: `Every`

```csharp
config.Rules.Every(TimeSpan.FromHours(24)).Then(SyncWithGraph);              // schedule "sync-with-graph"

config.Rules.Every("0 2 * * *", TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time"))
    .Then(NightlyCleanup);                                                   // 02:00 local time, every day

static async Task NightlyCleanup(RuleContext context, CancellationToken ct) { ... }
```

A schedule is work done at times, for no object. Its handler is `(RuleContext context, CancellationToken ct)`.

- **Times.** A `TimeSpan`, or a cron expression of five fields (six with seconds first), in a time zone (UTC when none is given). Cron expressions are parsed by the Cronos package.
- **Names.** Like queued rules: from the method, or given for a lambda, unique among schedules. Status lists a schedule as `schedule/nightly-cleanup`.
- **One stored record per schedule:** a `RuleSchedule` under the key `RuleSchedule.KeyOf(name)`, with `NextRun`, `LastRun` and `LastError`. It is an ordinary object: `await db.GetAsync<RuleSchedule>(RuleSchedule.KeyOf("nightly-cleanup"))`.
- **One process runs each time.** A process claims a run by moving `NextRun` on with an ETag-conditional write; the others see the conflict, or the new time, and do nothing.
- **The first run** is when the schedule's time first comes round after it is first seen, not at once.
- **Missed times run once.** After downtime the work runs one time, and the next time is counted from then.
- **A run that fails** is recorded in `LastError` (in the record and in `StatusAsync`) and is not retried: the work runs again at the next time.
- **`context.IdempotencyKey`** is the same for one scheduled time. `context.Attempt` is always 1.
- **Running.** With `QueueMode.Auto` a loop runs schedules while the database is open, looking at the records at least every 30 seconds. With `QueueMode.Manual`, `DrainAsync` runs the ones whose time has come, before the queued rules; each counts as a run.
- **A changed schedule** takes effect when the database opens with it: a stored time later than the new schedule gives is moved up.
- Schedules need no queue storage.

## Queues underneath

### One queue per type

- A save sends **one message** to the queue of the object's **concrete type**, listing the rules the save matched: `{ "key": "n1", "etag": "e7", "rules": ["Note/summarize", "Note/notify"] }`.
- Saving a `BlobPhoto` uses the `BlobPhoto` queue and lists rules for `BlobPhoto` and for its base types.
- The message never holds the object. The object is read when the rules run.
- A save that matches no queued rule sends nothing. A `Maintain` rule that runs inside the save is not in the message.
- The queues are the only stored state. There are no rows per rule or per object.

### A run

The pump takes a message, reads the object once, and runs the listed rules one after another:

1. **`Update` rules,** in the order registered.
2. **`Maintain` rules,** so what they produce is made from the updated object.
3. **`Then` rules,** in the order registered.

- **A rule's own save does not queue it again.** When a rule saves the object its run is for (an `Update` write-back, or a handler calling `SaveAsync`), that save queues neither the rule itself nor the rules the run has yet to do. Rules that already ran are queued if the save matches them.
- **Later rules see the saved object.** After such a save, the run reads the object again before the next rule.
- **A missing object** (deleted since, or replaced by an object of another type) ends the run and deletes the message.
- **Two runs for the same object never overlap** within a database.

### Failures, one rule at a time

All retry state is in the message. Example: rules A, B and C are listed, and B throws.

1. A and C are done. The message is **replaced in place**, in one `UpdateMessage` call, by one that lists only B:

   ```json
   { "key": "X1", "etag": "e7", "rules": ["Note/b"], "attempts": { "Note/b": 1 }, "lastError": { "Note/b": "timeout calling model" } }
   ```

2. It is hidden for `RuleRetryDelay`, doubling with each failure, up to five minutes.
3. When it comes up again, only B runs. Success deletes the message.

- **A rule that fails `RuleMaxAttempts` times** is set aside in the type's poison queue as `(type, key, etag, rule, error, attempts)`, and removed from the message. The other rules carry on.
- **A stale message still runs.** A message whose ETag no longer matches the object runs its rules on the object as it is now. Dropping it could lose work: a later save that did not change a rule's inputs queued nothing for it.
- **A crash during a run** leaves the message as it was. It comes up again when its lease runs out, and every rule in it runs again.
- **A renamed or removed rule.** A message naming a rule the configuration does not have reruns all of the type's queued rules for that object. That costs at most one extra run, and loses no work.
- **A message that cannot be read** is moved to the poison queue as it is.

### Loop prevention

- A rule's own save does not queue the rule again (above).
- `Changed(...)` keeps an `Update` from being triggered by saves that leave its inputs alone, another rule's write included.
- A write that changes nothing queues nothing. `ChangeAsync` whose lambda leaves the object as it is writes nothing at all; `SaveAsync` of an object unchanged since it was loaded writes, but queues nothing.
- **`MaxQueueHops`** (default 16) is the backstop. A save made more than that many queued rules deep is set aside without running.

## Settings

| `config.` | Default | What it is |
| --- | --- | --- |
| `QueueMode` | `Auto` | `Manual` runs queued rules only in `DrainAsync` |
| `MaxConcurrency` | 4 | How many objects are worked on at once |
| `RuleMaxAttempts` | 5 | Runs of a rule for an object before it is set aside |
| `RuleRetryDelay` | 2 s | The wait before the second run; doubles each time, up to 5 minutes |
| `RuleTimeout` | 5 min | How long one run of a rule may take |
| `MaxQueueHops` | 16 | How long a chain of queued rules may get |

There is no priority. Several objects are worked on at once, so slow work does not hold up fast work.

## The pump

- One per open, writable database that has queued rules. It starts when the catalog has fully opened the database and kept it, never for a read-only one.
- It looks at the type queues in turn, so none is starved.
- A run holds its message with a lease (the smaller of `RuleTimeout` and 60 seconds), renewed while it runs. A run that loses its lease is stopped and nothing is recorded; the message comes up again.
- A rule that outlasts `RuleTimeout` has its token cancelled and counts as failed.
- **Disposing the database** cancels the runs in progress and waits up to 10 seconds in all for them to end. A run stopped this way, or by cancelling a drain, counts no failure and its message is given back at once.
- **A run that finds the write lock held by another process** (`WriteLockUnavailableException`) counts no failure. Its message is hidden for the retry delay, doubling each time, so it does not spin while the lock is held.
- Delivery is at least once.

The catalog hands out a database only once it is fully open. If opening fails, the half-open database is disposed and the next `GetDatabaseAsync` opens it again. An open still in progress when the catalog is disposed or deleted keeps nothing and starts no pump.

## Operating it

You see rules, not queues.

| Method | What it does |
| --- | --- |
| `db.Rules.StatusAsync()` | Per rule: runs in progress, set-aside count, last error. Per type: messages waiting and set aside (approximate) |
| `db.Rules.RetryAsync(rule)` | Puts a rule's set-aside work back in its queue; returns how many |
| `db.Rules.DiscardAsync(rule)` | Deletes a rule's set-aside work; returns how many |
| `db.Rules.BackfillAsync(rule)` | Queues a rule for every stored object its filter is true for; returns how many |
| `db.DrainAsync(max)` | Runs what is ready now; returns how many rules were run, failed runs included |

- **`rule`** is a full identity (`Note/summarize`), or just the name (`summarize`) when only one rule has it.
- **Waiting work is counted per type,** not per rule: each message lists its rules, so a count per rule would mean reading every message.
- **Set-aside work under a name no rule has** (after a rename) shows in `StatusAsync` with `Orphaned = true`. `RetryAsync` and `DiscardAsync` accept its full identity.
- **A queue for a type that has no rules any more** shows with `Orphaned = true`. Its work is listed, not run.
- **`DrainAsync`** cannot be called from inside a handler or a save. Work waiting out a retry delay is not ready and is left. With `max`, objects are worked on one at a time.

`BackfillAsync` is how work is redone after a handler changes, or how a new rule is run for objects that already exist.

- **With a filter the index can express** (`[Queryable]` properties compared with constants), it asks the search index for the matching objects.
- **Otherwise** (no filter, a type that is not searchable, a filter comparing two fields or calling a method) it reads every object in the database.
- `Changed(...)` is taken to be true.
- Every candidate is checked again with the compiled filter before it is queued.

## Storage

- `LottaCatalog.UseStorage(provider, tables, blobs, queues, directories)` takes a queue factory. Every built-in `Use…()` provider passes one.
- An `Update`, `Maintain` or `Then` rule on a catalog with no queue storage throws `NotSupportedException` when the database is opened, before anything is created.
- **Queue names** are `l{catalog hash}{database hash}-{type}-{hash}` and `…-poison`: `note-view-k3x9aa`. The type's full name, the catalog and the database are in each queue's metadata.
- A type's queue is made the first time work is queued for the type. Opening the database finds the queues left by earlier runs.
- **FileSystem** needs `Iciclecreek.Azure.Storage.FileSystem` 2.1.0 or later.
- **Lucene:** each queue is a Lucene `Directory` of its own (`queues/{queue}`) with a file per message. `LuceneDirectories.FileSystem(rootPath)` opens each `FSDirectory` by its extended path (`\\?\C:\…`), since a message's file name is about 100 characters; a factory of your own should do the same.
- `ResetDatabaseAsync` empties the database's queues. `DeleteDatabaseAsync` and deleting the catalog delete them.
- `RebuildSearchIndex` queues nothing.
- **`_source`** is one extra column on objects a `Maintain` rule produced, and one indexed field in their index documents. No other object has it.

## Known limits

- A save in the instant before a crash may not be queued, the same window in which inline rules and indexing are lost.
- Sends for a batch save go one at a time.
- The set-aside count per rule is taken from the first 32 records of each poison queue.
- Queues made by another process after this one opened the database are not run here until it is reopened.
- "Two runs for the same object never overlap" holds within one process.
- **`Maintain` assumes one writing process.** What an object produced, and the objects a refresh is for, are found through the search index, which always includes this process's own writes but catches up with another process's only within `MaxSearchStaleness`.
- A `Maintain` rule with a filter queues or runs a clean-up for every save of an object the filter is false for.
- A backfill of a `Maintain` rule skips objects its filter is false for, so it does not clean up after them.
- Aggregates (many objects feeding one) are not covered by `Maintain`.
- A schedule's failed run is not retried before its next time.
- A schedule's handler is run by the process that claims the time; if that process stops mid-run, the time is not run again.

## Not built yet

See [Rules-Next.md](Rules-Next.md):

- Queued rules for deletes.
