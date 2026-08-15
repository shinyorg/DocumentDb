namespace Sample.Maui;

public partial class App : Application
{
    readonly IServiceProvider services;

    public App(IServiceProvider services)
    {
        InitializeComponent();
        this.services = services;
    }

    // Resolved per window - MainPage now takes the geofence manager and log alongside the store.
    protected override Window CreateWindow(IActivationState? activationState)
        => new(new NavigationPage(this.services.GetRequiredService<MainPage>()));
}
