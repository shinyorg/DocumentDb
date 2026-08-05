using System.Linq.Expressions;
using System.Security.Claims;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.AspNetCore;

/// <summary>Which HTTP operations a mapped document resource exposes.</summary>
[Flags]
public enum DocumentEndpoints
{
    None = 0,

    /// <summary><c>GET /</c> and <c>GET /{id}</c>.</summary>
    Read = 1,

    /// <summary><c>POST /</c>, <c>PUT /{id}</c>, <c>PATCH /{id}</c>.</summary>
    Write = 2,

    /// <summary><c>DELETE /{id}</c>.</summary>
    Delete = 4,

    /// <summary><c>GET /stream</c> — Server-Sent Events.</summary>
    Stream = 8,

    /// <summary><c>GET /count</c>.</summary>
    Count = 16,

    All = Read | Write | Delete | Stream | Count
}

/// <summary>
/// What a scope callback gets: the request, its DI scope, and the request's cancellation token.
/// </summary>
public readonly struct DocumentScopeContext
{
    internal DocumentScopeContext(HttpContext http)
    {
        this.Http = http;
        this.Services = http.RequestServices;
        this.User = http.User;
        this.CancellationToken = http.RequestAborted;
    }

    public HttpContext Http { get; }

    /// <summary>The request's scoped <see cref="IServiceProvider"/> (<c>Http.RequestServices</c>).</summary>
    public IServiceProvider Services { get; }

    /// <summary><c>Http.User</c> — the common case, one hop shorter.</summary>
    public ClaimsPrincipal User { get; }

    public CancellationToken CancellationToken { get; }

    public TService GetRequiredService<TService>() where TService : notnull
        => this.Services.GetRequiredService<TService>();

    public TService? GetService<TService>() => this.Services.GetService<TService>();
}

/// <summary>Helpers for the scope predicates, so intent reads at the call site.</summary>
public static class DocumentScope
{
    /// <summary>A scope that matches nothing. Return this when the caller may see no rows at all.</summary>
    public static Expression<Func<T, bool>> DenyAll<T>() => _ => false;

    /// <summary>The string-grammar equivalent, for <c>MapDocumentCollection</c>.</summary>
    public const string DenyAllFilter = "1 == 0";
}

/// <summary>Per-resource configuration for <c>MapDocuments&lt;T&gt;</c>.</summary>
public sealed class DocumentEndpointOptions<T> where T : class
{
    internal List<Func<DocumentScopeContext, ValueTask<Expression<Func<T, bool>>>>> Scopes { get; } = new();
    internal List<Type> RequiredServices { get; } = new();
    internal HashSet<string>? AllowedFields { get; private set; }

    /// <summary>Which operations to map. Default: read + count (the safe half).</summary>
    public DocumentEndpoints Operations { get; set; } = DocumentEndpoints.Read | DocumentEndpoints.Count;

    /// <summary>Hard cap on <c>?take=</c>. A larger request is clamped, not refused.</summary>
    public int MaxPageSize { get; set; } = 100;

    /// <summary>Page size when the caller does not ask for one.</summary>
    public int DefaultPageSize { get; set; } = 25;

    /// <summary>Source-generated metadata for <typeparamref name="T"/>. Required for AOT; strongly recommended otherwise.</summary>
    public JsonTypeInfo<T>? TypeInfo { get; set; }

    /// <summary>Resolve the store from a keyed registration (<c>AddDocumentStore(name, …)</c>) instead of the default.</summary>
    public string? StoreName { get; set; }

    /// <summary>When true, a write or delete without <c>If-Match</c> is <c>428 Precondition Required</c>.</summary>
    public bool RequireIfMatch { get; set; }

    /// <summary>Applied when the caller supplies no <c>?orderby=</c>. Cursor paging needs a stable order.</summary>
    public Expression<Func<T, object>>? DefaultOrderBy { get; set; }

    /// <summary>How often <c>/stream</c> emits a keep-alive comment. Proxies kill idle connections; this is not optional.</summary>
    public TimeSpan StreamHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Fields a client may filter, sort or project on. Empty means everything — fine for an internal API,
    /// wrong for a public one, where an unlisted field should be a <c>400</c> rather than a table scan.
    /// </summary>
    public DocumentEndpointOptions<T> AllowFilterOn(params Expression<Func<T, object>>[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        this.AllowedFields ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in fields)
            this.AllowedFields.Add(MemberName(field));

        return this;
    }

    /// <summary>
    /// A server-side scope AND-ed into every read and enforced on every write — the HTTP twin of the AI
    /// tools' non-removable <c>Where</c>. Typically the caller's tenant or owner id. Call it more than once
    /// to AND several scopes together; there is no way for a request to remove one.
    /// </summary>
    /// <remarks>
    /// A document outside the scope is <c>404</c> on read, write and delete — never <c>403</c>, which would
    /// confirm the record exists. Return <see cref="DocumentScope.DenyAll{T}"/> for "no access"; the
    /// endpoints never treat "no scope" as "everything".
    /// </remarks>
    public DocumentEndpointOptions<T> Scope(Func<DocumentScopeContext, Expression<Func<T, bool>>> scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Scopes.Add(ctx => new ValueTask<Expression<Func<T, bool>>>(scope(ctx)));
        return this;
    }

    /// <summary>Async <see cref="Scope(Func{DocumentScopeContext, Expression{Func{T, bool}}})"/> — for a scope the service has to await.</summary>
    public DocumentEndpointOptions<T> Scope(Func<DocumentScopeContext, ValueTask<Expression<Func<T, bool>>>> scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Scopes.Add(scope);
        return this;
    }

    /// <summary>Scope built from a service resolved out of the <b>request</b> scope (<c>GetRequiredService</c>).</summary>
    public DocumentEndpointOptions<T> Scope<TService>(
        Func<TService, DocumentScopeContext, Expression<Func<T, bool>>> scope) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Require<TService>();
        this.Scopes.Add(ctx => new ValueTask<Expression<Func<T, bool>>>(scope(ctx.GetRequiredService<TService>(), ctx)));
        return this;
    }

    /// <summary>Async <see cref="Scope{TService}(Func{TService, DocumentScopeContext, Expression{Func{T, bool}}})"/>.</summary>
    public DocumentEndpointOptions<T> Scope<TService>(
        Func<TService, DocumentScopeContext, ValueTask<Expression<Func<T, bool>>>> scope) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Require<TService>();
        this.Scopes.Add(ctx => scope(ctx.GetRequiredService<TService>(), ctx));
        return this;
    }

    void Require<TService>()
    {
        if (!this.RequiredServices.Contains(typeof(TService)))
            this.RequiredServices.Add(typeof(TService));
    }

    static string MemberName(LambdaExpression expression)
    {
        var body = expression.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
            body = unary.Operand;

        return body is MemberExpression member
            ? member.Member.Name
            : throw new ArgumentException(
                "Field expression must be a simple member access (e.g. x => x.Status).", nameof(expression));
    }
}

/// <summary>Per-resource configuration for <c>MapDocumentCollection</c> (the schema-free lane).</summary>
public sealed class DocumentCollectionEndpointOptions
{
    internal List<Func<DocumentScopeContext, ValueTask<string>>> Scopes { get; } = new();
    internal List<Type> RequiredServices { get; } = new();
    internal HashSet<string>? AllowedFields { get; private set; }

    /// <summary>Which operations to map. Default: read + count.</summary>
    public DocumentEndpoints Operations { get; set; } = DocumentEndpoints.Read | DocumentEndpoints.Count;

    public int MaxPageSize { get; set; } = 100;
    public int DefaultPageSize { get; set; } = 25;

    /// <summary>The JSON property holding the id. Matches <c>store.Collection(name, idProperty)</c>.</summary>
    public string IdProperty { get; set; } = "id";

    public string? StoreName { get; set; }

    /// <summary>Applied when the caller supplies no <c>?orderby=</c>.</summary>
    public string? DefaultOrderBy { get; set; }

    public TimeSpan StreamHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Fields a client may filter, sort or project on. Empty means everything.</summary>
    public DocumentCollectionEndpointOptions AllowFilterOn(params string[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        this.AllowedFields ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in fields)
            this.AllowedFields.Add(field);

        return this;
    }

    /// <summary>A server-side scope clause AND-ed into every read. Same contract as the typed lane.</summary>
    public DocumentCollectionEndpointOptions Scope(Func<DocumentScopeContext, string> scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Scopes.Add(ctx => new ValueTask<string>(scope(ctx)));
        return this;
    }

    /// <summary>Async <see cref="Scope(Func{DocumentScopeContext, string})"/>.</summary>
    public DocumentCollectionEndpointOptions Scope(Func<DocumentScopeContext, ValueTask<string>> scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Scopes.Add(scope);
        return this;
    }

    /// <summary>Scope clause built from a service resolved out of the request scope.</summary>
    public DocumentCollectionEndpointOptions Scope<TService>(
        Func<TService, DocumentScopeContext, string> scope) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Require<TService>();
        this.Scopes.Add(ctx => new ValueTask<string>(scope(ctx.GetRequiredService<TService>(), ctx)));
        return this;
    }

    /// <summary>Async <see cref="Scope{TService}(Func{TService, DocumentScopeContext, string})"/>.</summary>
    public DocumentCollectionEndpointOptions Scope<TService>(
        Func<TService, DocumentScopeContext, ValueTask<string>> scope) where TService : notnull
    {
        ArgumentNullException.ThrowIfNull(scope);
        this.Require<TService>();
        this.Scopes.Add(ctx => scope(ctx.GetRequiredService<TService>(), ctx));
        return this;
    }

    void Require<TService>()
    {
        if (!this.RequiredServices.Contains(typeof(TService)))
            this.RequiredServices.Add(typeof(TService));
    }
}
