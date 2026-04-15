using Lotta;
using Lucene.Net.Store;
using Spotflow.InMemory.Azure.Storage;
using Spotflow.InMemory.Azure.Storage.Tables;
using TodoApp.Models;

namespace TodoApp.Services;

/// <summary>
/// Thin wrapper around a LottaDB instance scoped to <see cref="TodoItem"/>.
///
/// Demo mode: in-memory Azure Tables (Spotflow) + RAM Lucene directory — zero setup,
/// but todos are lost when the app exits. For persistence, swap the table client for
/// <c>new TableServiceClient("UseDevelopmentStorage=true")</c> (Azurite) or a real
/// connection string, and the Lucene directory for <c>FSDirectory.Open(path)</c>.
/// </summary>
public class TodoStore
{
    private readonly Lotta.LottaDB _db;

    public TodoStore()
    {
        var provider = new InMemoryStorageProvider();
        var account = provider.AddAccount("todoapp");
        var tables = InMemoryTableServiceClient.FromAccount(account);

        var directory = new RAMDirectory();
        directory.SetLockFactory(NoLockFactory.GetNoLockFactory());

        var options = new LottaConfiguration();
        options.Store<TodoItem>();

        _db = new Lotta.LottaDB("todos", tables, directory, options);
    }

    /// <summary>Create or replace a todo (used for initial insert and edits).</summary>
    public Task<ObjectResult> SaveAsync(TodoItem todo, CancellationToken ct = default)
        => _db.SaveAsync(todo, ct);

    /// <summary>
    /// Toggle the done flag using the ETag-aware ChangeAsync so concurrent edits
    /// to Title/Notes never clobber (or get clobbered by) a checkbox toggle.
    /// </summary>
    public Task<ObjectResult> ToggleDoneAsync(string id, bool isDone, CancellationToken ct = default)
        => _db.ChangeAsync<TodoItem>(id, t =>
        {
            t.IsDone = isDone;
            t.CompletedAt = isDone ? DateTimeOffset.UtcNow : null;
            return t;
        }, ct);

    public Task<ObjectResult> DeleteAsync(string id, CancellationToken ct = default)
        => _db.DeleteAsync<TodoItem>(id, ct);

    /// <summary>
    /// Lucene-backed live search. Empty query returns everything (still through the
    /// index, not a table scan). Status filter uses the NotAnalyzed IsDone field.
    /// </summary>
    public IReadOnlyList<TodoItem> Search(string? query, TodoFilter filter)
    {
        var q = _db.Search<TodoItem>();

        q = filter switch
        {
            TodoFilter.Open => q.Where(t => t.IsDone == false),
            TodoFilter.Done => q.Where(t => t.IsDone == true),
            _ => q
        };

        if (!string.IsNullOrWhiteSpace(query))
        {
            var term = query.Trim().ToLowerInvariant();
            // Analyzed fields -> token/substring match via Lucene.Net.Linq's Contains translation.
            q = q.Where(t => t.Title.Contains(term) || t.Notes.Contains(term));
        }

        return q.ToList()
            .OrderBy(t => t.IsDone)
            .ThenByDescending(t => t.Created)
            .ToList();
    }
}

public enum TodoFilter
{
    All = 0,
    Open = 1,
    Done = 2,
}
