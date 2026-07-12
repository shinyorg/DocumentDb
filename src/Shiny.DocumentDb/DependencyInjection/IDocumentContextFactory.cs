namespace Shiny.DocumentDb;

/// <summary>
/// Creates short-lived <see cref="DocumentContext"/> instances on demand — the document-store parallel of
/// EF Core's <c>IDbContextFactory&lt;TContext&gt;</c>. Registered as a singleton by the source-generated
/// <c>Add&lt;Context&gt;Factory</c> extension, so it can be injected anywhere (including into singletons) in
/// apps with no ambient DI scope — MAUI, Blazor, desktop, background services. Call <see cref="Create"/> per
/// unit of work; the returned context is cheap (a stateless facade over the shared, thread-safe
/// <see cref="IDocumentStore"/>) and needs no disposal.
/// </summary>
/// <typeparam name="TContext">The generated context type.</typeparam>
public interface IDocumentContextFactory<out TContext> where TContext : DocumentContext
{
    /// <summary>Creates a new context bound to this factory's store.</summary>
    TContext Create();
}
