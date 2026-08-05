using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.AspNetCore.Internal;

/// <summary>
/// The schema-free lane. There is no CLR type here, so the scope is a string-grammar clause rather than an
/// expression and it is enforced in SQL on every path — including the writes, which resolve their target
/// through the scoped query before touching it.
/// </summary>
static class DocumentCollectionEndpointHandlers
{
    /// <summary>Mapped as <see cref="RequestDelegate"/>s for the same AOT reason as the typed lane.</summary>
    public static void Map(RouteGroupBuilder group, string collectionName, DocumentCollectionEndpointOptions options)
    {
        var ops = options.Operations;

        if (ops.HasFlag(DocumentEndpoints.Read))
        {
            group.MapGet("/", Handler(http => List(http, collectionName, options)))
                .WithName($"List-{collectionName}")
                .WithMetadata(Json(StatusCodes.Status200OK));

            group.MapGet("/{id}", Handler(http => GetById(http, RouteId(http), collectionName, options)))
                .WithName($"Get-{collectionName}")
                .WithMetadata(Json(StatusCodes.Status200OK), Problem(StatusCodes.Status404NotFound));
        }

        if (ops.HasFlag(DocumentEndpoints.Count))
        {
            group.MapGet("/count", Handler(http => Count(http, collectionName, options)))
                .WithName($"Count-{collectionName}")
                .WithMetadata(Json(StatusCodes.Status200OK));
        }

        if (ops.HasFlag(DocumentEndpoints.Write))
        {
            group.MapPost("/", Handler(http => Insert(http, collectionName, options)))
                .WithName($"Create-{collectionName}");

            group.MapPut("/{id}", Handler(http => Write(http, RouteId(http), collectionName, options, patch: false)))
                .WithName($"Replace-{collectionName}")
                .WithMetadata(Problem(StatusCodes.Status404NotFound));

            group.MapPatch("/{id}", Handler(http => Write(http, RouteId(http), collectionName, options, patch: true)))
                .WithName($"Patch-{collectionName}")
                .WithMetadata(Problem(StatusCodes.Status404NotFound));
        }

        if (ops.HasFlag(DocumentEndpoints.Delete))
        {
            group.MapDelete("/{id}", Handler(http => Delete(http, RouteId(http), collectionName, options)))
                .WithName($"Delete-{collectionName}")
                .WithMetadata(Problem(StatusCodes.Status404NotFound));
        }
    }

    static RequestDelegate Handler(Func<HttpContext, Task<IResult>> handler)
        => async http =>
        {
            var result = await Guard(http, () => handler(http)).ConfigureAwait(false);
            await result.ExecuteAsync(http).ConfigureAwait(false);
        };

    static string RouteId(HttpContext http)
        => http.Request.RouteValues["id"]?.ToString()
           ?? throw new BadRequestException("An id is required.");

    static ProducesResponseTypeMetadata Json(int status)
        => new(status, typeof(JsonObject), ["application/json"]);

    static ProducesResponseTypeMetadata Problem(int status)
        => new(status, typeof(Microsoft.AspNetCore.Mvc.ProblemDetails), ["application/problem+json"]);

    static IJsonDocumentCollection Collection(HttpContext http, string name, DocumentCollectionEndpointOptions options)
    {
        var store = options.StoreName is null
            ? http.RequestServices.GetRequiredService<IDocumentStore>()
            : http.RequestServices.GetRequiredKeyedService<IDocumentStore>(options.StoreName);

        return store.Collection(name, options.IdProperty);
    }

    static async Task<IResult> Guard(HttpContext http, Func<Task<IResult>> handler)
    {
        try
        {
            return await handler().ConfigureAwait(false);
        }
        catch (BadRequestException ex)
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
        }
        catch (NotSupportedException ex)
        {
            // JSON collections are relational-only; a document-native provider says so here rather than at boot,
            // because the store may be swapped by configuration.
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status501NotImplemented);
        }
        catch (JsonException ex)
        {
            return Results.Problem($"Malformed JSON body: {ex.Message}", statusCode: StatusCodes.Status400BadRequest);
        }
        catch (ArgumentException ex)
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
        }
    }

    static async ValueTask<IReadOnlyList<string>> Scope(HttpContext http, DocumentCollectionEndpointOptions options)
    {
        if (options.Scopes.Count == 0)
            return Array.Empty<string>();

        var context = new DocumentScopeContext(http);
        var clauses = new List<string>(options.Scopes.Count);
        foreach (var scope in options.Scopes)
        {
            var clause = await scope(context).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(clause))
                throw new InvalidOperationException(
                    $"A scope callback for collection '{context.Http.Request.Path}' returned an empty clause. " +
                    $"Return DocumentScope.DenyAllFilter to deny everything — empty is not a scope.");

            clauses.Add(clause);
        }
        return clauses;
    }

    static async Task<IJsonDocumentQuery> Build(
        HttpContext http,
        string name,
        DocumentCollectionEndpointOptions options,
        ListRequest request)
    {
        var query = Collection(http, name, options).Query();

        foreach (var clause in await Scope(http, options).ConfigureAwait(false))
            query = query.Where(clause);

        if (request.Filter != null)
            query = query.Where(request.Filter);

        var sorted = false;
        foreach (var (path, direction) in request.SortKeys())
        {
            query = query.OrderBy(direction is null ? path : $"{path} {direction}");
            sorted = true;
        }

        if (!sorted && options.DefaultOrderBy != null)
            query = query.OrderBy(options.DefaultOrderBy);

        return query;
    }

    static async Task<IResult> List(HttpContext http, string name, DocumentCollectionEndpointOptions options)
    {
        var request = ListRequest.Parse(http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields);
        var query = await Build(http, name, options, request).ConfigureAwait(false);
        var ct = http.RequestAborted;

        var cursorPaged = http.Request.Query.ContainsKey("cursor");

        if (request.Fields != null)
        {
            if (cursorPaged)
                throw new BadRequestException(
                    "'fields' and 'cursor' cannot be combined. Use skip/take with a sparse fieldset, or drop " +
                    "'fields' to page by cursor.");

            var rows = await query
                .Project(request.Fields)
                .Paginate(request.Skip, request.Take)
                .ToList(ct)
                .ConfigureAwait(false);

            return Json(new JsonArray(rows.Cast<JsonNode?>().ToArray()));
        }

        if (cursorPaged)
        {
            var page = await query.ToCursorPage(request.Cursor, request.Take, ct).ConfigureAwait(false);
            return Json(Envelope(page.Items, page.NextCursor));
        }

        var items = await query.Paginate(request.Skip, request.Take).ToList(ct).ConfigureAwait(false);
        return Json(new JsonArray(items.Cast<JsonNode?>().ToArray()));
    }

    static async Task<IResult> GetById(HttpContext http, string id, string name, DocumentCollectionEndpointOptions options)
    {
        // Resolved through the scoped query, so an out-of-scope id is indistinguishable from a missing one.
        var query = await Build(http, name, options, ListRequest.Parse(
            http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields)).ConfigureAwait(false);

        var match = await query
            .Where(IdClause(options.IdProperty, id))
            .FirstOrDefault(http.RequestAborted)
            .ConfigureAwait(false);

        return match is null
            ? Results.Problem($"No document with id '{id}'.", statusCode: StatusCodes.Status404NotFound)
            : Json(match);
    }

    static async Task<IResult> Count(HttpContext http, string name, DocumentCollectionEndpointOptions options)
    {
        var request = ListRequest.Parse(http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields);
        var query = await Build(http, name, options, request).ConfigureAwait(false);
        var count = await query.Count(http.RequestAborted).ConfigureAwait(false);
        return Json(new JsonObject { ["count"] = count });
    }

    static async Task<IResult> Insert(HttpContext http, string name, DocumentCollectionEndpointOptions options)
    {
        await AssertUnscoped(http, options, "created").ConfigureAwait(false);

        var body = await ReadObject(http).ConfigureAwait(false);
        var id = await Collection(http, name, options).Insert(body, http.RequestAborted).ConfigureAwait(false);
        return Results.Created($"{http.Request.Path}/{id}".Replace("//", "/"), null);
    }

    static async Task<IResult> Write(HttpContext http, string id, string name, DocumentCollectionEndpointOptions options, bool patch)
    {
        await AssertUnscoped(http, options, patch ? "patched" : "replaced").ConfigureAwait(false);

        var body = await ReadObject(http).ConfigureAwait(false);
        body[options.IdProperty] ??= id;

        var affected = await Collection(http, name, options)
            .Update(body, patch, http.RequestAborted)
            .ConfigureAwait(false);

        return affected == 0
            ? Results.Problem($"No document with id '{id}'.", statusCode: StatusCodes.Status404NotFound)
            : Results.NoContent();
    }

    static async Task<IResult> Delete(HttpContext http, string id, string name, DocumentCollectionEndpointOptions options)
    {
        // Deletes resolve through the scoped query first, so this one IS safe alongside a scope.
        var query = await Build(http, name, options, ListRequest.Parse(
            http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields)).ConfigureAwait(false);

        var match = await query
            .Where(IdClause(options.IdProperty, id))
            .FirstOrDefault(http.RequestAborted)
            .ConfigureAwait(false);

        if (match is null)
            return Results.Problem($"No document with id '{id}'.", statusCode: StatusCodes.Status404NotFound);

        await Collection(http, name, options).Remove(id, http.RequestAborted).ConfigureAwait(false);
        return Results.NoContent();
    }

    /// <summary>
    /// A scope and an insert/replace cannot be combined here, for the same reason the AI collection lane
    /// refuses it: a raw JSON body has no evaluator, so the boundary could not be enforced on the way in. The
    /// typed lane (<c>MapDocuments&lt;T&gt;</c>) enforces it on both sides of a write.
    /// </summary>
    static async ValueTask AssertUnscoped(HttpContext http, DocumentCollectionEndpointOptions options, string verb)
    {
        if (options.Scopes.Count > 0)
            throw new NotSupportedException(
                $"This collection endpoint has a Scope(...) and therefore cannot have documents {verb} through it — " +
                "a schema-free body cannot be checked against the scope before it is written. Map the type with " +
                "MapDocuments<T>() for scoped writes.");

        await ValueTask.CompletedTask;
    }

    static async Task<JsonObject> ReadObject(HttpContext http)
        => await JsonSerializer.DeserializeAsync<JsonObject>(http.Request.Body, cancellationToken: http.RequestAborted)
               .ConfigureAwait(false)
           ?? throw new BadRequestException("A request body must be a JSON object.");

    static IResult Json(JsonNode node) => Results.Content(node.ToJsonString(), "application/json");

    static JsonObject Envelope(IReadOnlyList<JsonObject> items, string? nextCursor) => new()
    {
        ["items"] = new JsonArray(items.Cast<JsonNode?>().ToArray()),
        ["nextCursor"] = nextCursor
    };

    /// <summary>
    /// <c>id == 'value'</c> for the string grammar. The value is a quoted literal with its quotes doubled —
    /// the grammar's own escape — so a route id can never break out of the literal.
    /// </summary>
    static string IdClause(string idProperty, string id)
        => $"{idProperty} == '{id.Replace("'", "''")}'";
}
