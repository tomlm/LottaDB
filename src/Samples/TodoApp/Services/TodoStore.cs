using Lotta;
using Lucene.Net.Linq;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
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
public class TodoStore : IDisposable
{
    private readonly Task<LottaDB> _dbTask;

    public TodoStore(IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("TableStorage");

        var catalog = new LottaCatalog("todos", connectionString!);
        _dbTask = catalog.GetDatabaseAsync("default", config =>
        {
            config.Store<TodoItem>();
        });
    }

    /// <summary>Create or replace a todo (used for initial insert and edits).</summary>
    public async Task<ObjectResult> SaveAsync(TodoItem todo, CancellationToken cancellationToken = default)
    {
        var db = await _dbTask;
        return await db.SaveAsync(todo, cancellationToken);
    }

    /// <summary>
    /// Toggle the done flag using the ETag-aware ChangeAsync so concurrent edits
    /// to Title/Notes never clobber (or get clobbered by) a checkbox toggle.
    /// </summary>
    public async Task<ObjectResult> ToggleDoneAsync(string id, bool isDone, CancellationToken cancellationTokent = default)
    {
        var db = await _dbTask;
        return await db.ChangeAsync<TodoItem>(id, t =>
        {
            t.IsDone = isDone;
            t.CompletedAt = isDone ? DateTimeOffset.UtcNow : null;
            return t;
        }, cancellationTokent);
    }

    public async Task<ObjectResult> DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        var db = await _dbTask;
        return await db.DeleteAsync<TodoItem>(id, cancellationToken);
    }

    /// <summary>
    /// Lucene-backed live search. Empty query returns everything (still through the
    /// index, not a table scan). Status filter uses the NotAnalyzed IsDone field.
    /// </summary>
    public async Task<IReadOnlyList<TodoItem>> SearchAsync(string? query, TodoFilter filter)
    {
        try
        {
            var db = await _dbTask;
            var q = db.Search<TodoItem>(query);

            q = filter switch
            {
                TodoFilter.Open => q.Where(t => t.IsDone == false),
                TodoFilter.Done => q.Where(t => t.IsDone == true),
                _ => q
            };

            //if (!string.IsNullOrWhiteSpace(query))
            //{
            //    var term = query.Trim().ToLowerInvariant();
            //    // Analyzed fields -> token/substring match via Lucene.Net.Linq's Contains translation.
            //    q = q.Where(t => t.Title.Contains(term) || t.Notes.Contains(term));
            //}

            return q.OrderBy(t => t.IsDone)
                .ThenByDescending(t => t.Created)
                .ToList();
        }
        catch (Exception err)
        {
            Debug.WriteLine(err.Message);
            return Array.Empty<TodoItem>();
        }
    }

    public void Dispose() => _dbTask.Dispose();
}

public enum TodoFilter
{
    All = 0,
    Open = 1,
    Done = 2,
}
