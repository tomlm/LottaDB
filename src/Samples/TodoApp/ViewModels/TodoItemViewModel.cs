using CommunityToolkit.Mvvm.ComponentModel;
using TodoApp.Models;
using TodoApp.Services;

namespace TodoApp.ViewModels;

/// <summary>
/// Wraps a single TodoItem for binding. Edits to Title/Notes are saved on
/// LostFocus (via the view's binding trigger). Toggling IsDone goes through
/// the ETag-aware ChangeAsync path.
/// </summary>
public partial class TodoItemViewModel : ObservableObject
{
    private readonly TodoStore _store;
    private readonly TodoItem _model;

    public TodoItemViewModel(TodoItem model, TodoStore store)
    {
        _model = model;
        _store = store;
    }

    public string Id => _model.Id;
    public DateTimeOffset Created => _model.Created;
    public DateTimeOffset? CompletedAt => _model.CompletedAt;

    public string Title
    {
        get => _model.Title;
        set
        {
            if (_model.Title == value) return;
            _model.Title = value;
            OnPropertyChanged();
            _ = _store.SaveAsync(_model);
        }
    }

    public string Notes
    {
        get => _model.Notes;
        set
        {
            if (_model.Notes == value) return;
            _model.Notes = value;
            OnPropertyChanged();
            _ = _store.SaveAsync(_model);
        }
    }

    public bool IsDone
    {
        get => _model.IsDone;
        set
        {
            if (_model.IsDone == value) return;
            _model.IsDone = value;
            _model.CompletedAt = value ? DateTimeOffset.UtcNow : null;
            OnPropertyChanged();
            OnPropertyChanged(nameof(CompletedAt));
            // ChangeAsync: ETag-aware so a simultaneous title edit won't clobber the toggle.
            _ = _store.ToggleDoneAsync(_model.Id, value);
        }
    }
}
