using Shiny.DocumentDb;

namespace Sample.Maui;

public partial class App : Application
{
    readonly IDocumentStore store;

    public App(IDocumentStore store)
    {
        InitializeComponent();
        this.store = store;
    }

    protected override Window CreateWindow(IActivationState? activationState)
        => new(new NavigationPage(new MainPage(store)));
}
