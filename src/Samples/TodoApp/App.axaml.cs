using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Microsoft.Extensions.DependencyInjection;
using TodoApp.Services;
using TodoApp.ViewModels;
using TodoApp.Views;

namespace TodoApp;

public partial class App : Application
{
    private readonly IServiceProvider _services;

    public App(IServiceProvider sp)
    {
        this._services = sp;
    }

    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            var store = ActivatorUtilities.CreateInstance<TodoStore>(_services);
            desktop.MainWindow = new MainWindow
            {
                DataContext = new MainViewModel(store)
            };
        }

        base.OnFrameworkInitializationCompleted();
    }
}
