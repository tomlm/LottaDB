using Azure;
using Azure.Data.Tables;
using Azure.Data.Tables.Models;
using Iciclecreek.Azure.Storage.OData;
using Lotta.Internal;
using Lucene.Net.Analysis.Core;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Util;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using LuceneDirectory = Lucene.Net.Store.Directory;

namespace Lotta;

/// <summary>
/// A table whose rows live in Lucene indexes. See <see cref="LuceneTableServiceClient"/>.
/// Supports the TableEntity operations LottaDB uses: point reads, upsert/update/add with
/// If-Match, delete, OData-filtered queries and same-partition transactions.
/// </summary>
internal sealed class LuceneTableClient : TableClient
{
    private readonly LuceneTableServiceClient _service;
    private readonly string _tableName;
    private readonly CatalogRows _catalogRows;

    // Serializes the read-check-write of conditional operations per partition. LottaDB already
    // holds its per-key locks around these calls; this keeps the client correct on its own.
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _partitionGates = new();

    // Per database instance, so a reopened database never inherits another one's state.
    private readonly System.Runtime.CompilerServices.ConditionalWeakTable<LottaDB, RecentWrites> _recentWrites = new();

    internal LuceneTableClient(LuceneTableServiceClient service, string tableName) : base()
    {
        _service = service;
        _tableName = tableName;
        _catalogRows = new CatalogRows(() => service.Catalog.LuceneDirectoryFactory($"{service.Catalog.Name}/__catalog__/Tables"));
    }

    /// <inheritdoc/>
    public override string Name => _tableName;

    /// <inheritdoc/>
    public override string AccountName => _service.AccountName;

    // ---- Table ----

    public override Response<TableItem> Create(CancellationToken cancellationToken = default)
        => Response.FromValue(new TableItem(_tableName), LuceneTableResponse.NoContent());

    public override Task<Response<TableItem>> CreateAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(Create(cancellationToken));

    public override Response<TableItem> CreateIfNotExists(CancellationToken cancellationToken = default)
        => Create(cancellationToken);

    public override Task<Response<TableItem>> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        => CreateAsync(cancellationToken);

    public override Response Delete(CancellationToken cancellationToken = default)
        => DeleteAsync(cancellationToken).GetAwaiter().GetResult();

    /// <summary>
    /// Dropping the table removes every database's rows — their indexes — and the catalog rows.
    /// </summary>
    public override async Task<Response> DeleteAsync(CancellationToken cancellationToken = default)
    {
        var databaseIds = _catalogRows.GetAll()
            .Where(r => r.PartitionKey == LottaCatalog.ManifestPartitionKey)
            .Select(r => r.RowKey)
            .Concat(_service.Databases.Select(d => d.Key))
            .Distinct()
            .ToList();

        foreach (var databaseId in databaseIds)
        {
            if (_service.GetDatabase(databaseId) is { } db)
            {
                await RemoveAllRowsAsync(db, cancellationToken).ConfigureAwait(false);
                continue;
            }

            // Not open in this process — clear its index directly.
            try
            {
                var directory = _service.Catalog.LuceneDirectoryFactory($"{_service.Catalog.Name}/{databaseId}/Search");
                if (DirectoryReader.IndexExists(directory))
                {
                    using var writer = new IndexWriter(directory, CatalogRows.NewWriterConfig());
                    writer.DeleteAll();
                    writer.Commit();
                }
            }
            catch (ObjectDisposedException)
            {
                // A disposed in-memory directory holds nothing that outlives this process.
            }
        }

        _catalogRows.DeleteAll();
        return LuceneTableResponse.NoContent();
    }

    // ---- Point reads ----

    public override Task<Response<T>> GetEntityAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => Task.FromResult(GetEntity<T>(partitionKey, rowKey, select, cancellationToken));

    public override Response<T> GetEntity<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var row = GetRow(partitionKey, rowKey)
            ?? throw new RequestFailedException(404, "The specified resource does not exist.", "ResourceNotFound", null);
        return Response.FromValue(Convert<T>(row), LuceneTableResponse.Ok());
    }

    public override Task<NullableResponse<T>> GetEntityIfExistsAsync<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => Task.FromResult(GetEntityIfExists<T>(partitionKey, rowKey, select, cancellationToken));

    public override NullableResponse<T> GetEntityIfExists<T>(string partitionKey, string rowKey, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
    {
        var row = GetRow(partitionKey, rowKey);
        return row == null
            ? new LuceneTableNoValue<T>()
            : Response.FromValue(Convert<T>(row), LuceneTableResponse.Ok());
    }

    // ---- Writes ----

    public override async Task<Response> AddEntityAsync<T>(T entity, CancellationToken cancellationToken = default)
        => LuceneTableResponse.NoContent(await WriteAsync(entity, WriteKind.Add, default, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false));

    public override Response AddEntity<T>(T entity, CancellationToken cancellationToken = default)
        => AddEntityAsync(entity, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> UpsertEntityAsync<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => LuceneTableResponse.NoContent(await WriteAsync(entity, WriteKind.Upsert, default, mode, cancellationToken).ConfigureAwait(false));

    public override Response UpsertEntity<T>(T entity, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpsertEntityAsync(entity, mode, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> UpdateEntityAsync<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => LuceneTableResponse.NoContent(await WriteAsync(entity, WriteKind.Update, ifMatch, mode, cancellationToken).ConfigureAwait(false));

    public override Response UpdateEntity<T>(T entity, ETag ifMatch, TableUpdateMode mode = TableUpdateMode.Merge, CancellationToken cancellationToken = default)
        => UpdateEntityAsync(entity, ifMatch, mode, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> DeleteEntityAsync(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
    {
        var gate = Gate(partitionKey);
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var current = GetETag(partitionKey, rowKey) ?? throw NotFound();
            if (!Matches(current, ifMatch)) throw PreconditionFailed();
            await RemoveRowAsync(partitionKey, rowKey, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
        return LuceneTableResponse.NoContent();
    }

    public override Response DeleteEntity(string partitionKey, string rowKey, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntityAsync(partitionKey, rowKey, ifMatch, cancellationToken).GetAwaiter().GetResult();

    public override Task<Response> DeleteEntityAsync(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntityAsync(entity.PartitionKey, entity.RowKey, ifMatch, cancellationToken);

    public override Response DeleteEntity(ITableEntity entity, ETag ifMatch = default, CancellationToken cancellationToken = default)
        => DeleteEntity(entity.PartitionKey, entity.RowKey, ifMatch, cancellationToken);

    private enum WriteKind { Add, Upsert, Update }

    private async Task<ETag> WriteAsync(ITableEntity entity, WriteKind kind, ETag ifMatch, TableUpdateMode mode, CancellationToken cancellationToken)
    {
        var gate = Gate(entity.PartitionKey);
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // A merge needs the whole current row. Every other condition only needs its current
            // ETag (null = no row), and a plain replace-upsert has no condition to check at all.
            TableEntity? existing = null;
            ETag? current = null;
            if (mode == TableUpdateMode.Merge)
                current = (existing = GetRow(entity.PartitionKey, entity.RowKey))?.ETag;
            else if (kind != WriteKind.Upsert)
                current = GetETag(entity.PartitionKey, entity.RowKey);

            if (kind == WriteKind.Add && current != null)
                throw new RequestFailedException(409, "The specified entity already exists.", "EntityAlreadyExists", null);
            if (kind == WriteKind.Update)
            {
                if (current == null) throw NotFound();
                if (!Matches(current.Value, ifMatch)) throw PreconditionFailed();
            }

            var row = NewRow(entity, mode == TableUpdateMode.Merge ? existing : null);
            await PutRowAsync(row, cancellationToken).ConfigureAwait(false);
            return row.ETag;
        }
        finally
        {
            gate.Release();
        }
    }

    // ---- Queries ----

    public override AsyncPageable<T> QueryAsync<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => AsyncPageable<T>.FromPages(new[] { Page<T>.FromValues(QueryRows<T>(filter), null, LuceneTableResponse.Ok()) });

    public override Pageable<T> Query<T>(string? filter = null, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => Pageable<T>.FromPages(new[] { Page<T>.FromValues(QueryRows<T>(filter), null, LuceneTableResponse.Ok()) });

    public override AsyncPageable<T> QueryAsync<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => QueryAsync<T>(CreateQueryFilter(filter), maxPerPage, select, cancellationToken);

    public override Pageable<T> Query<T>(Expression<Func<T, bool>> filter, int? maxPerPage = null, IEnumerable<string>? select = null, CancellationToken cancellationToken = default)
        => Query<T>(CreateQueryFilter(filter), maxPerPage, select, cancellationToken);

    /// <summary>
    /// Narrow the rows to read using what the filter pins down — partition, row keys, row types —
    /// then apply the full filter to those candidates.
    /// </summary>
    private List<T> QueryRows<T>(string? filter)
    {
        var predicate = ODataFilterParser.Parse(filter ?? "");
        return CandidateRows(ScopeOf(filter))
            .Where(predicate)
            .Select(Convert<T>)
            .ToList();
    }

    /// <summary>
    /// What a filter is limited to by its top-level <c>and</c> clauses: a single partition, a
    /// set of row keys, a set of <c>_type</c> values. Each is null when the filter does not limit it.
    /// </summary>
    internal sealed record FilterScope(string? PartitionKey, IReadOnlySet<string>? RowKeys, IReadOnlySet<string>? Types);

    private static readonly Regex s_equality = new(@"^(PartitionKey|RowKey|" + StorageFields.Type + @") eq '((?:[^']|'')*)'$", RegexOptions.Compiled);

    internal static FilterScope ScopeOf(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
            return new FilterScope(null, null, null);

        // "a and b or c" is "(a and b) or c": with a top-level or, the whole filter is one clause.
        var clauses = SplitTopLevel(filter, "or").Count > 1 ? [filter] : SplitTopLevel(filter, "and");

        string? partitionKey = null;
        HashSet<string>? rowKeys = null, types = null;
        foreach (var clause in clauses)
        {
            // A clause limits the rows if it is one equality, or an or of equalities on one column.
            var matches = SplitTopLevel(StripParens(clause), "or")
                .Select(alternative => s_equality.Match(StripParens(alternative)))
                .ToList();
            if (!matches.All(m => m.Success)) continue;
            var column = matches[0].Groups[1].Value;
            if (matches.Any(m => m.Groups[1].Value != column)) continue;
            var values = matches.Select(m => m.Groups[2].Value.Replace("''", "'")).ToHashSet();

            switch (column)
            {
                case "PartitionKey" when values.Count == 1:
                    partitionKey = values.First();
                    break;
                case "RowKey":
                    rowKeys = rowKeys == null ? values : rowKeys.Intersect(values).ToHashSet();
                    break;
                case StorageFields.Type:
                    types = types == null ? values : types.Intersect(values).ToHashSet();
                    break;
            }
        }
        return new FilterScope(partitionKey, rowKeys, types);
    }

    /// <summary>Split on a keyword (<c>and</c>/<c>or</c>) that is outside parentheses and string literals.</summary>
    private static List<string> SplitTopLevel(string filter, string keyword)
    {
        var parts = new List<string>();
        var depth = 0;
        var inString = false;
        var start = 0;
        for (var i = 0; i < filter.Length; i++)
        {
            var c = filter[i];
            if (c == '\'') inString = !inString;
            else if (inString) continue;
            else if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && i > 0 && char.IsWhiteSpace(filter[i - 1])
                && string.Compare(filter, i, keyword, 0, keyword.Length, StringComparison.Ordinal) == 0
                && i + keyword.Length < filter.Length && char.IsWhiteSpace(filter[i + keyword.Length]))
            {
                parts.Add(filter[start..i].Trim());
                start = i + keyword.Length;
            }
        }
        parts.Add(filter[start..].Trim());
        return parts;
    }

    /// <summary>Remove parentheses that enclose the whole expression.</summary>
    private static string StripParens(string expression)
    {
        while (expression.Length >= 2 && expression[0] == '(' && expression[^1] == ')')
        {
            var depth = 0;
            var inString = false;
            for (var i = 0; i < expression.Length; i++)
            {
                var c = expression[i];
                if (c == '\'') inString = !inString;
                else if (inString) continue;
                else if (c == '(') depth++;
                else if (c == ')' && --depth == 0 && i < expression.Length - 1)
                    return expression; // the opening paren closes before the end: "(a) and (b)"
            }
            expression = expression[1..^1].Trim();
        }
        return expression;
    }

    // ---- Transactions ----

    public override Response<IReadOnlyList<Response>> SubmitTransaction(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
        => SubmitTransactionAsync(transactionActions, cancellationToken).GetAwaiter().GetResult();

    /// <summary>
    /// Checks every action against the current rows before applying any of them, so a failed
    /// condition leaves the partition untouched.
    /// </summary>
    public override async Task<Response<IReadOnlyList<Response>>> SubmitTransactionAsync(IEnumerable<TableTransactionAction> transactionActions, CancellationToken cancellationToken = default)
    {
        var actions = transactionActions.ToList();
        if (actions.Count == 0)
            throw new ArgumentException("At least one action is required.", nameof(transactionActions));
        var partitionKey = actions[0].Entity.PartitionKey;
        if (actions.Any(a => a.Entity.PartitionKey != partitionKey))
            throw new TableTransactionFailedException(new RequestFailedException(400, "All entities in a transaction must share a PartitionKey.", "InvalidInput", null));

        var gate = Gate(partitionKey);
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Plan against the rows as they will be after each earlier action.
            var state = new Dictionary<string, TableEntity?>();
            TableEntity? Current(string rowKey) => state.TryGetValue(rowKey, out var row) ? row : GetRow(partitionKey, rowKey);
            ETag? CurrentETag(string rowKey) => state.TryGetValue(rowKey, out var row) ? row?.ETag : GetETag(partitionKey, rowKey);

            var planned = new List<(string rowKey, TableEntity? row)>();
            foreach (var action in actions)
            {
                var rowKey = action.Entity.RowKey;
                TableEntity? next;
                try
                {
                    // Replace-upserts overwrite unconditionally; merges read the whole row; the
                    // remaining conditions only need the current ETag.
                    switch (action.ActionType)
                    {
                        case TableTransactionActionType.Add:
                            if (CurrentETag(rowKey) != null)
                                throw new RequestFailedException(409, "The specified entity already exists.", "EntityAlreadyExists", null);
                            next = NewRow(action.Entity, null);
                            break;
                        case TableTransactionActionType.UpsertReplace:
                            next = NewRow(action.Entity, null);
                            break;
                        case TableTransactionActionType.UpsertMerge:
                            next = NewRow(action.Entity, Current(rowKey));
                            break;
                        case TableTransactionActionType.UpdateReplace:
                        {
                            var current = CurrentETag(rowKey) ?? throw NotFound();
                            if (!Matches(current, action.ETag)) throw PreconditionFailed();
                            next = NewRow(action.Entity, null);
                            break;
                        }
                        case TableTransactionActionType.UpdateMerge:
                        {
                            var existing = Current(rowKey) ?? throw NotFound();
                            if (!Matches(existing.ETag, action.ETag)) throw PreconditionFailed();
                            next = NewRow(action.Entity, existing);
                            break;
                        }
                        case TableTransactionActionType.Delete:
                        {
                            var current = CurrentETag(rowKey) ?? throw NotFound();
                            if (!Matches(current, action.ETag == default ? action.Entity.ETag : action.ETag)) throw PreconditionFailed();
                            next = null;
                            break;
                        }
                        default:
                            throw new NotSupportedException($"Transaction action {action.ActionType} is not supported.");
                    }
                }
                catch (RequestFailedException ex)
                {
                    throw new TableTransactionFailedException(ex);
                }
                state[rowKey] = next;
                planned.Add((rowKey, next));
            }

            await ApplyRowsAsync(partitionKey, planned, cancellationToken).ConfigureAwait(false);

            var responses = planned
                .Select(p => p.row == null ? LuceneTableResponse.NoContent() : LuceneTableResponse.NoContent(p.row.ETag))
                .ToList();
            return Response.FromValue<IReadOnlyList<Response>>(responses, LuceneTableResponse.Accepted());
        }
        finally
        {
            gate.Release();
        }
    }

    // ---- Row routing ----
    // A database's partition is stored in its own index, written through its indexing pipeline:
    // take the write lease, build the row's index document, replace the document for its key.
    // Reads never reopen the index — see RecentWrites.

    private TableEntity? GetRow(string partitionKey, string rowKey)
        => _service.GetDatabase(partitionKey) is { } db
            ? GetRow(db, Recent(db), rowKey)
            : _catalogRows.Get(partitionKey, rowKey);

    private static TableEntity? GetRow(LottaDB db, RecentWrites recent, string rowKey)
    {
        recent.Sync(db);
        if (recent.TryGet(rowKey, out var written))
            return written == null ? null : new TableEntity(written);

        return db.ReadIndex(searcher =>
        {
            var doc = IndexKeyLookup.For(searcher.IndexReader, StorageFields.Key).Find(TableStorageAdapter.DecodeKey(rowKey));
            return doc < 0 ? null : LuceneRowCodec.ToRow(searcher.Doc(doc));
        }, null, refresh: false);
    }

    /// <summary>
    /// The row's current ETag, or null if there is no row — all an If-Match or existence check
    /// needs, without loading and rebuilding the whole row.
    /// </summary>
    private ETag? GetETag(string partitionKey, string rowKey)
        => _service.GetDatabase(partitionKey) is { } db
            ? GetETag(db, Recent(db), rowKey)
            : _catalogRows.Get(partitionKey, rowKey)?.ETag;

    private static readonly ISet<string> s_etagField = new HashSet<string> { StorageFields.ETag };

    private static ETag? GetETag(LottaDB db, RecentWrites recent, string rowKey)
    {
        recent.Sync(db);
        if (recent.TryGet(rowKey, out var written))
            return written?.ETag;

        return db.ReadIndex<ETag?>(searcher =>
        {
            var doc = IndexKeyLookup.For(searcher.IndexReader, StorageFields.Key).Find(TableStorageAdapter.DecodeKey(rowKey));
            return doc >= 0 && searcher.Doc(doc, s_etagField).Get(StorageFields.ETag) is { } etag ? new ETag(etag) : null;
        }, null, refresh: false);
    }

    /// <summary>
    /// The rows whose documents match <paramref name="query"/>, plus every row written since the
    /// last refresh — the caller's filter decides which of those it wants.
    /// </summary>
    private static IEnumerable<TableEntity> GetRows(LottaDB db, RecentWrites recent, Query query)
    {
        recent.Sync(db);
        var written = recent.Snapshot();
        var rows = db.ReadIndex(searcher =>
        {
            var hits = searcher.Search(query, Math.Max(1, searcher.IndexReader.MaxDoc));
            return hits.ScoreDocs
                .Select(h => LuceneRowCodec.ToRow(searcher.Doc(h.Doc)))
                .OfType<TableEntity>()
                .ToDictionary(r => r.RowKey);
        }, new Dictionary<string, TableEntity>(), refresh: false);

        // The searcher is as of its last refresh; lay the writes made since over it.
        foreach (var (rowKey, row) in written)
        {
            if (row == null) rows.Remove(rowKey);
            else rows[rowKey] = new TableEntity(row);
        }
        return rows.Values;
    }

    private Task PutRowAsync(TableEntity row, CancellationToken cancellationToken)
        => ApplyRowsAsync(row.PartitionKey, [(row.RowKey, row)], cancellationToken);

    private Task RemoveRowAsync(string partitionKey, string rowKey, CancellationToken cancellationToken)
        => ApplyRowsAsync(partitionKey, [(rowKey, null)], cancellationToken);

    /// <summary>
    /// Write rows (null = delete) to one partition as a unit. For a database partition every
    /// index document is built before any is written, so a mapping failure leaves the partition
    /// untouched; catalog rows are applied under a single commit.
    /// </summary>
    private async Task ApplyRowsAsync(string partitionKey, IReadOnlyList<(string rowKey, TableEntity? row)> rows, CancellationToken cancellationToken)
    {
        if (_service.GetDatabase(partitionKey) is not { } db)
        {
            _catalogRows.Apply(partitionKey, rows);
            return;
        }
        var recent = Recent(db);
        using var lease = await db.AcquireWriterAsync(cancellationToken).ConfigureAwait(false);
        recent.Sync(db);
        db.WriteIndexDocuments(rows.Select(r => (TableStorageAdapter.DecodeKey(r.rowKey), Build(r.row))).ToList());
        foreach (var (rowKey, row) in rows)
            recent.Record(rowKey, row);
        recent.RefreshIfFull(db);

        Func<Document?> Build(TableEntity? row) => row == null ? () => null : () => db.BuildIndexDocument(row);
    }

    private static async Task RemoveAllRowsAsync(LottaDB db, CancellationToken cancellationToken)
    {
        using var lease = await db.AcquireWriterAsync(cancellationToken).ConfigureAwait(false);
        db.DeleteAllIndexDocuments();
    }

    /// <summary>The rows a filter of this scope could match — a superset; the filter itself still applies.</summary>
    private IEnumerable<TableEntity> CandidateRows(FilterScope scope)
    {
        if (scope.PartitionKey != null)
        {
            return _service.GetDatabase(scope.PartitionKey) is { } db
                ? CandidateRows(db, scope)
                : _catalogRows.GetAll().Where(r => r.PartitionKey == scope.PartitionKey);
        }
        return _service.Databases.SelectMany(d => CandidateRows(d.Value, scope)).Concat(_catalogRows.GetAll());
    }

    private IEnumerable<TableEntity> CandidateRows(LottaDB db, FilterScope scope)
    {
        var recent = Recent(db);

        // Point lookups by key.
        if (scope.RowKeys != null)
            return scope.RowKeys.Select(rowKey => GetRow(db, recent, rowKey)).OfType<TableEntity>().ToList();

        // Documents of the given row types, or every document.
        Query query = new MatchAllDocsQuery();
        if (scope.Types != null)
        {
            var anyType = new BooleanQuery();
            foreach (var type in scope.Types)
                anyType.Add(new TermQuery(new Term(LuceneRowCodec.RowTypeField, type)), Occur.SHOULD);
            query = anyType;
        }
        return GetRows(db, recent, query);
    }

    private RecentWrites Recent(LottaDB db) => _recentWrites.GetValue(db, _ => new RecentWrites());

    /// <summary>
    /// The rows this process wrote to a database's index since its near-real-time searcher was
    /// last refreshed (null = deleted). An IndexWriter cannot be read, and reopening a searcher
    /// over it flushes a segment, so reads do not reopen: a key in here is answered from here,
    /// and any other key is unchanged since the searcher's last refresh, so the searcher is
    /// current for it. Every <see cref="RefreshThreshold"/> writes the searcher is refreshed and
    /// the writes it now covers are dropped. When the database's <see cref="LottaDB.IndexEpoch"/>
    /// moves — the writer changed hands, or the index was cleared — everything is dropped and
    /// the searcher refreshed.
    /// </summary>
    private sealed class RecentWrites
    {
        private const int RefreshThreshold = 1000;

        private readonly ConcurrentDictionary<string, (TableEntity? Row, long Seq)> _rows = new();
        private readonly object _syncLock = new();
        private long _epoch = -1;
        private long _seq;

        /// <summary>Catch up with index changes made other than through this client.</summary>
        public void Sync(LottaDB db)
        {
            var epoch = db.IndexEpoch;
            if (Interlocked.Read(ref _epoch) == epoch) return;
            lock (_syncLock)
            {
                if (_epoch == epoch) return;
                // Clear before refreshing: a write recorded after the clear is kept, and one
                // recorded before it was already in the writer, so the refresh picks it up.
                _rows.Clear();
                db.ReadIndex(_ => 0, 0, refresh: true);
                Interlocked.Exchange(ref _epoch, epoch);
            }
        }

        public bool TryGet(string rowKey, out TableEntity? row)
        {
            var found = _rows.TryGetValue(rowKey, out var entry);
            row = entry.Row;
            return found;
        }

        public List<KeyValuePair<string, TableEntity?>> Snapshot()
            => _rows.Select(e => KeyValuePair.Create(e.Key, e.Value.Row)).ToList();

        /// <summary>Record a write. Called under the partition gate, after the index write.</summary>
        public void Record(string rowKey, TableEntity? row) => _rows[rowKey] = (row, Interlocked.Increment(ref _seq));

        /// <summary>Called under the partition gate while holding the write lease.</summary>
        public void RefreshIfFull(LottaDB db)
        {
            if (_rows.Count < RefreshThreshold) return;
            var covered = Interlocked.Read(ref _seq);
            db.ReadIndex(_ => 0, 0, refresh: true);
            foreach (var entry in _rows)
            {
                if (entry.Value.Seq <= covered)
                    _rows.TryRemove(entry);
            }
        }
    }

    private SemaphoreSlim Gate(string partitionKey) => _partitionGates.GetOrAdd(partitionKey, _ => new SemaphoreSlim(1, 1));

    // ---- Helpers ----

    /// <summary>The row to store: a copy of <paramref name="entity"/>, merged onto <paramref name="mergeInto"/> if given, with a fresh ETag.</summary>
    private static TableEntity NewRow(ITableEntity entity, TableEntity? mergeInto)
    {
        if (entity is not TableEntity source)
            throw new NotSupportedException($"{nameof(LuceneTableClient)} stores {nameof(TableEntity)} rows only, not {entity.GetType().Name}.");

        var row = mergeInto != null ? new TableEntity(mergeInto) : new TableEntity(source.PartitionKey, source.RowKey);
        foreach (var (name, value) in source)
        {
            if (name is "PartitionKey" or "RowKey" or "Timestamp" or "odata.etag") continue;
            row[name] = value;
        }
        row.ETag = new ETag(Ulid.NewUlid().ToString());
        row.Timestamp = DateTimeOffset.UtcNow;
        return row;
    }

    private static T Convert<T>(TableEntity row)
        => row is T typed
            ? typed
            : throw new NotSupportedException($"{nameof(LuceneTableClient)} returns {nameof(TableEntity)} rows only, not {typeof(T).Name}.");

    private static bool Matches(ETag current, ETag ifMatch)
        => ifMatch == default || ifMatch == ETag.All || Normalize(ifMatch.ToString()) == Normalize(current.ToString());

    private static string Normalize(string etag) => (etag.StartsWith("W/", StringComparison.Ordinal) ? etag[2..] : etag).Trim('"');

    private static RequestFailedException NotFound()
        => new(404, "The specified resource does not exist.", "ResourceNotFound", null);

    private static RequestFailedException PreconditionFailed()
        => new(412, "The update condition specified in the request was not satisfied.", "UpdateConditionNotSatisfied", null);

    /// <summary>
    /// Rows that belong to no database (the catalog manifest). Written rarely, so each write opens
    /// a short-lived IndexWriter and commits — durable at once, and the write lock is never held.
    /// </summary>
    private sealed class CatalogRows
    {
        private readonly Lazy<LuceneDirectory> _directory;
        private readonly object _lock = new();

        public CatalogRows(Func<LuceneDirectory> directoryFactory)
        {
            _directory = new Lazy<LuceneDirectory>(directoryFactory);
        }

        public static IndexWriterConfig NewWriterConfig() => new(LuceneVersion.LUCENE_48, new KeywordAnalyzer())
        {
            OpenMode = OpenMode.CREATE_OR_APPEND,
            WriteLockTimeout = 5000,
        };

        public TableEntity? Get(string partitionKey, string rowKey)
            => Read(searcher =>
            {
                var hits = searcher.Search(new TermQuery(new Term(LuceneRowCodec.IdField, LuceneRowCodec.Id(partitionKey, rowKey))), 1);
                return hits.TotalHits == 0 ? null : LuceneRowCodec.ToRow(searcher.Doc(hits.ScoreDocs[0].Doc));
            }, null);

        public IReadOnlyList<TableEntity> GetAll()
            => Read<IReadOnlyList<TableEntity>>(searcher =>
            {
                var hits = searcher.Search(new MatchAllDocsQuery(), Math.Max(1, searcher.IndexReader.MaxDoc));
                return hits.ScoreDocs.Select(h => LuceneRowCodec.ToRow(searcher.Doc(h.Doc))).OfType<TableEntity>().ToList();
            }, Array.Empty<TableEntity>());

        /// <summary>Write rows (null = delete) under a single commit.</summary>
        public void Apply(string partitionKey, IReadOnlyList<(string rowKey, TableEntity? row)> rows)
        {
            var documents = rows.Select(r => r.row == null ? null : ToDocument(r.row)).ToList();
            Write(writer =>
            {
                for (var i = 0; i < rows.Count; i++)
                {
                    var id = new Term(LuceneRowCodec.IdField, LuceneRowCodec.Id(partitionKey, rows[i].rowKey));
                    if (documents[i] is { } document)
                        writer.UpdateDocument(id, document);
                    else
                        writer.DeleteDocuments(id);
                }
            });
        }

        private static Document ToDocument(TableEntity row)
        {
            var document = LuceneRowCodec.CreateRawDocument(row);
            document.Add(new StringField(LuceneRowCodec.IdField, LuceneRowCodec.Id(row.PartitionKey, row.RowKey), Field.Store.NO));
            LuceneRowCodec.AddRowFields(document, row);
            return document;
        }

        public void DeleteAll() => Write(writer => writer.DeleteAll());

        private T Read<T>(Func<IndexSearcher, T> read, T empty)
        {
            lock (_lock)
            {
                if (!DirectoryReader.IndexExists(_directory.Value)) return empty;
                using var reader = DirectoryReader.Open(_directory.Value);
                return read(new IndexSearcher(reader));
            }
        }

        private void Write(Action<IndexWriter> write)
        {
            lock (_lock)
            {
                using var writer = new IndexWriter(_directory.Value, NewWriterConfig());
                write(writer);
                writer.Commit();
            }
        }
    }
}
