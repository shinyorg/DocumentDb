using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.AspNetCore.Internal;

namespace Shiny.DocumentDb.AspNetCore;

/// <summary>
/// Turns a document type into a complete HTTP resource in one line: list, by-id, count, create, replace,
/// merge-patch, delete and a live SSE tail.
/// </summary>
/// <remarks>
/// This is the plain-REST half of the HTTP story. Reach for
/// <c>Shiny.DocumentDb.AspNetCore.OData</c> instead when the client speaks OData (<c>$filter</c>,
/// <c>$metadata</c>, an existing OData toolchain); never map both on the same prefix.
/// </remarks>
public static class DocumentEndpointExtensions
{
    /// <summary>
    /// Maps <typeparamref name="T"/> as a REST resource under <paramref name="prefix"/>. Returns the group, so
    /// authorization, rate limiting, CORS and output caching compose the normal way.
    /// </summary>
    /// <example>
    /// <code>
    /// app.MapDocuments&lt;Order&gt;("/orders", o =>
    /// {
    ///     o.Operations = DocumentEndpoints.All;
    ///     o.AllowFilterOn(x => x.Status, x => x.Total);
    ///     o.TypeInfo = AppJsonContext.Default.Order;
    ///     o.Scope&lt;ITenantContext&gt;((tenant, _) => x => x.TenantId == tenant.TenantId);
    /// })
    /// .RequireAuthorization("orders");
    /// </code>
    /// </example>
    public static RouteGroupBuilder MapDocuments<T>(
        this IEndpointRouteBuilder endpoints,
        string prefix,
        Action<DocumentEndpointOptions<T>>? configure = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);

        var options = new DocumentEndpointOptions<T>();
        configure?.Invoke(options);
        Validate(endpoints, options.Operations, options.RequiredServices, options.StoreName, typeof(T).Name);

        var group = endpoints.MapGroup(prefix).WithTags(typeof(T).Name);
        DocumentEndpointHandlers<T>.Map(group, options);
        return group;
    }

    /// <summary>
    /// Maps a <b>schema-free</b> JSON collection (<c>store.Collection(name)</c>) as a REST resource. Documents
    /// are plain JSON with no CLR type, filtered with the string grammar. Relational providers only.
    /// </summary>
    public static RouteGroupBuilder MapDocumentCollection(
        this IEndpointRouteBuilder endpoints,
        string prefix,
        string collectionName,
        Action<DocumentCollectionEndpointOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);

        var options = new DocumentCollectionEndpointOptions();
        configure?.Invoke(options);
        Validate(endpoints, options.Operations, options.RequiredServices, options.StoreName, collectionName);

        var group = endpoints.MapGroup(prefix).WithTags(collectionName);
        DocumentCollectionEndpointHandlers.Map(group, collectionName, options);
        return group;
    }

    /// <summary>
    /// Startup checks, so the failure is at boot rather than on the first request that needs the thing:
    /// every <c>Scope&lt;TService&gt;</c> service must be registered, and <c>/stream</c> is refused outright on
    /// a provider without change monitoring.
    /// </summary>
    static void Validate(
        IEndpointRouteBuilder endpoints,
        DocumentEndpoints operations,
        IReadOnlyList<Type> requiredServices,
        string? storeName,
        string resource)
    {
        foreach (var serviceType in requiredServices)
        {
            if (endpoints.ServiceProvider.GetService(serviceType) is null)
                throw new InvalidOperationException(
                    $"'{resource}' registers a Scope<{serviceType.Name}>(…) but no '{serviceType}' is registered. " +
                    "A scope that cannot resolve its service refuses every request, so this is a startup error.");
        }

        if (operations.HasFlag(DocumentEndpoints.Stream))
        {
            var store = storeName is null
                ? endpoints.ServiceProvider.GetService<IDocumentStore>()
                : endpoints.ServiceProvider.GetKeyedService<IDocumentStore>(storeName);

            if (store is not null and not IObservableDocumentStore)
                throw new InvalidOperationException(
                    $"'{resource}' maps DocumentEndpoints.Stream, but '{store.GetType().Name}' does not support " +
                    "change monitoring (IObservableDocumentStore). Drop the Stream flag or move to a provider " +
                    "that does — a /stream route that can only ever return 501 is worse than no route.");
        }
    }
}
