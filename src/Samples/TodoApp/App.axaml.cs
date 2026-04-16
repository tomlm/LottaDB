using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Microsoft.Extensions.Configuration;
using TodoApp.Services;
using TodoApp.ViewModels;
using TodoApp.Views;

namespace TodoApp;

public partial class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            var store = new TodoStore(new ConfigurationManager()
            {
                ["ConnectionString"] = "UseDevelopmentStorage=true"
            });
            desktop.MainWindow = new MainWindow
            {
                DataContext = new MainViewModel(store)
            };
        }

        base.OnFrameworkInitializationCompleted();
    }
}
