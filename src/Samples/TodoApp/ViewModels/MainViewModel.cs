using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using System.Collections.ObjectModel;
using TodoApp.Models;
using TodoApp.Services;

namespace TodoApp.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private readonly TodoStore _store;

    public MainViewModel(TodoStore store)
    {
        _store = store;
        Items = new ObservableCollection<TodoItemViewModel>();
        AddCommand = new RelayCommand(AddNewTodo, () => !string.IsNullOrWhiteSpace(NewTodoTitle));
        ClearSearchCommand = new RelayCommand(() => SearchText = string.Empty);
        DeleteCommand = new AsyncRelayCommand<TodoItemViewModel>(DeleteAsync);
        ReloadAsync();
    }

    public ObservableCollection<TodoItemViewModel> Items { get; }

    [ObservableProperty]
    private string _searchText = string.Empty;

    [ObservableProperty]
    private int _filterIndex; // 0=All, 1=Open, 2=Done

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(AddCommand))]
    private string _newTodoTitle = string.Empty;

    [ObservableProperty]
    private string _statusLine = string.Empty;

    public IRelayCommand AddCommand { get; }
    public IRelayCommand ClearSearchCommand { get; }
    public IAsyncRelayCommand<TodoItemViewModel> DeleteCommand { get; }

    partial void OnSearchTextChanged(string value) => ReloadAsync();
    partial void OnFilterIndexChanged(int value) => ReloadAsync();

    private void AddNewTodo()
    {
        var title = NewTodoTitle.Trim();
        if (string.IsNullOrEmpty(title)) return;

        var todo = new TodoItem { Title = title };
        _ = AddAndReloadAsync(todo);

        NewTodoTitle = string.Empty;
    }

    private async Task AddAndReloadAsync(TodoItem todo)
    {
        await _store.SaveAsync(todo);
        await ReloadAsync();
    }

    private async Task DeleteAsync(TodoItemViewModel? vm)
    {
        if (vm is null) return;
        await _store.DeleteAsync(vm.Id);
        await ReloadAsync();
    }

    private async Task ReloadAsync()
    {
        var filter = (TodoFilter)FilterIndex;
        var results = await _store.SearchAsync(SearchText, filter);

        Items.Clear();
        foreach (var t in results)
            Items.Add(new TodoItemViewModel(t, _store));

        var total = results.Count;
        var done = results.Count(t => t.IsDone);
        StatusLine = $"{total} shown · {done} done · {total - done} open";
    }
}
