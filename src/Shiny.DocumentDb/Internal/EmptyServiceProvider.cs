namespace Shiny.DocumentDb.Internal;

/// <summary>The service provider used by a scope-less session (e.g. <c>store.OpenSession()</c> with no DI). It
/// resolves nothing — scoped interceptors need a real scope, which the factory / scoped registration supplies.</summary>
sealed class EmptyServiceProvider : IServiceProvider
{
    public static readonly EmptyServiceProvider Instance = new();
    EmptyServiceProvider() { }
    public object? GetService(Type serviceType) => null;
}
