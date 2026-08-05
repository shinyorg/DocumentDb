using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Metadata;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.AspNetCore.Internal;

/// <summary>
/// The typed lane: one document type as a complete HTTP resource. Reads go through the raw-JSON terminals
/// where they can (a document read only to be written back out never becomes an object); everything that has
/// to enforce the scope in memory materializes, because that is what enforcement costs.
/// </summary>
static class DocumentEndpointHandlers<T> where T : class
{
    /// <summary>
    /// Every route is mapped as a <see cref="RequestDelegate"/> rather than a typed minimal-API handler. That
    /// is deliberate: the <c>Delegate</c> overloads of <c>MapGet</c> and friends reflect over the handler's
    /// parameters (IL2026/IL3050), which would cost this package its AOT-clean promise. The handlers already
    /// have the <see cref="HttpContext"/> in hand, so nothing is lost but the parameter binding.
    /// </summary>
    public static void Map(RouteGroupBuilder group, DocumentEndpointOptions<T> options)
    {
        var ops = options.Operations;
        var name = typeof(T).Name;

        if (ops.HasFlag(DocumentEndpoints.Read))
        {
            group.MapGet("/", Handler(http => List(http, options)))
                .WithName($"List{name}")
                .WithMetadata(Json(StatusCodes.Status200OK));

            group.MapGet("/{id}", Handler(http => GetById(http, RouteId(http), options)))
                .WithName($"Get{name}")
                .WithMetadata(Json(StatusCodes.Status200OK), Problem(StatusCodes.Status404NotFound));
        }

        if (ops.HasFlag(DocumentEndpoints.Count))
        {
            group.MapGet("/count", Handler(http => Count(http, options)))
                .WithName($"Count{name}")
                .WithMetadata(Json(StatusCodes.Status200OK));
        }

        if (ops.HasFlag(DocumentEndpoints.Stream))
        {
            group.MapGet("/stream", new RequestDelegate(http => Stream(http, options)))
                .WithName($"Stream{name}")
                .WithMetadata(new ProducesResponseTypeMetadata(StatusCodes.Status200OK, typeof(void), ["text/event-stream"]));
        }

        if (ops.HasFlag(DocumentEndpoints.Write))
        {
            group.MapPost("/", Handler(http => Insert(http, options)))
                .WithName($"Create{name}")
                .WithMetadata(Problem(StatusCodes.Status400BadRequest), Problem(StatusCodes.Status409Conflict));

            group.MapPut("/{id}", Handler(http => Replace(http, RouteId(http), options)))
                .WithName($"Replace{name}")
                .WithMetadata(Problem(StatusCodes.Status404NotFound), Problem(StatusCodes.Status412PreconditionFailed));

            group.MapPatch("/{id}", Handler(http => Patch(http, RouteId(http), options)))
                .WithName($"Patch{name}")
                .WithMetadata(Problem(StatusCodes.Status404NotFound), Problem(StatusCodes.Status412PreconditionFailed));
        }

        if (ops.HasFlag(DocumentEndpoints.Delete))
        {
            group.MapDelete("/{id}", Handler(http => Delete(http, RouteId(http), options)))
                .WithName($"Delete{name}")
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
        => new(status, typeof(T), ["application/json"]);

    static ProducesResponseTypeMetadata Problem(int status)
        => new(status, typeof(Microsoft.AspNetCore.Mvc.ProblemDetails), ["application/problem+json"]);

    // ── Plumbing ────────────────────────────────────────────────────────

    static IDocumentStore Store(HttpContext http, DocumentEndpointOptions<T> options)
        => options.StoreName is null
            ? http.RequestServices.GetRequiredService<IDocumentStore>()
            : http.RequestServices.GetRequiredKeyedService<IDocumentStore>(options.StoreName);

    /// <summary>
    /// One place that turns an exception into a response, so a malformed filter is a <c>400</c> rather than a
    /// stack trace and a provider gap is a <c>501</c> rather than a <c>500</c>.
    /// </summary>
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
        catch (PreconditionRequiredException ex)
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status428PreconditionRequired);
        }
        catch (ConcurrencyException ex)
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status412PreconditionFailed);
        }
        catch (NotSupportedException ex)
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status501NotImplemented);
        }
        catch (JsonException ex)
        {
            return Results.Problem($"Malformed JSON body: {ex.Message}", statusCode: StatusCodes.Status400BadRequest);
        }
        catch (ArgumentException ex)
        {
            // The grammar parser reports a bad filter this way. The caller's mistake, not the server's.
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
        }
    }

    /// <summary>
    /// Resolves this request's scope: every registered callback, run inside the request's DI scope, AND-ed.
    /// A callback that throws fails the request — never "run without the predicate".
    /// </summary>
    static async ValueTask<ScopeSet> Scope(HttpContext http, DocumentEndpointOptions<T> options)
    {
        if (options.Scopes.Count == 0)
            return ScopeSet.Empty;

        var context = new DocumentScopeContext(http);
        var expressions = new List<Expression<Func<T, bool>>>(options.Scopes.Count);
        var predicates = new List<Func<T, bool>>(options.Scopes.Count);

        foreach (var scope in options.Scopes)
        {
            var expression = await scope(context).ConfigureAwait(false)
                ?? throw new InvalidOperationException(
                    $"A scope callback for '{typeof(T).Name}' returned null. Return DocumentScope.DenyAll<{typeof(T).Name}>() " +
                    "to deny everything — null is not a scope.");

            expressions.Add(expression);
            // The same compile-free interpreter the AI tools use: one implementation, one set of semantics.
            // Rebuilt per request, so it cannot be cached the way a startup-fixed predicate can.
            predicates.Add(ExpressionInterpreter.Interpret(expression));
        }
        return new ScopeSet(expressions, predicates.ToArray());
    }

    readonly struct ScopeSet(IReadOnlyList<Expression<Func<T, bool>>> expressions, Func<T, bool>[] predicates)
    {
        public static ScopeSet Empty { get; } = new(Array.Empty<Expression<Func<T, bool>>>(), Array.Empty<Func<T, bool>>());

        public IDocumentQuery<T> ApplyTo(IDocumentQuery<T> query)
        {
            foreach (var expression in expressions)
                query = query.Where(expression);
            return query;
        }

        public bool Contains(T document)
        {
            foreach (var predicate in predicates)
            {
                if (!predicate(document))
                    return false;
            }
            return true;
        }
    }

    static JsonTypeInfo<T>? TypeInfo(IDocumentStore store, DocumentEndpointOptions<T> options)
        => options.TypeInfo ?? store.Query<T>().QueryTypeInfo;

    static VersionMapping? Version(IDocumentStore store, HttpContext http)
    {
        var storeOptions = (store as IQueryExecutor)?.Options
            ?? http.RequestServices.GetService<DocumentStoreOptions>();

        return storeOptions?.Mappings.ResolveVersionMapping(typeof(T));
    }

    static IDocumentQuery<T> Build(IDocumentStore store, DocumentEndpointOptions<T> options, ScopeSet scope, ListRequest request)
    {
        var typeInfo = TypeInfo(store, options);
        var query = scope.ApplyTo(store.Query(typeInfo));

        if (request.Filter != null)
            query = query.Where(request.Filter, typeInfo);

        var sorted = false;
        foreach (var (path, direction) in request.SortKeys())
        {
            query = query.OrderBy(path, direction, typeInfo);
            sorted = true;
        }

        if (!sorted && options.DefaultOrderBy != null)
            query = query.OrderBy(options.DefaultOrderBy);

        return query;
    }

    static JsonObject Envelope(IReadOnlyList<JsonObject> items, string? nextCursor) => new()
    {
        ["items"] = new JsonArray(items.Cast<JsonNode?>().ToArray()),
        ["nextCursor"] = nextCursor
    };

    /// <summary>Serializes a materialized document without re-encrypting what the store just decrypted.</summary>
    static string Serialize(T document, JsonTypeInfo<T>? typeInfo)
        => typeInfo is null
            ? JsonSerializer.Serialize(document)
            : JsonSerializer.Serialize(document, DocumentEncryption.PlaintextView(typeInfo));

    // ── Reads ───────────────────────────────────────────────────────────

    static async Task<IResult> List(HttpContext http, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);
        var request = ListRequest.Parse(http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields);
        var scope = await Scope(http, options).ConfigureAwait(false);
        var query = Build(store, options, scope, request);
        var ct = http.RequestAborted;

        var cursorPaged = http.Request.Query.ContainsKey("cursor");

        // Sparse fieldset: Project already hands back JsonObjects, so there is nothing to save by taking the
        // raw lane — but paging still applies, on the projected query.
        if (request.Fields != null)
        {
            // Cursor paging is built from the sort/key columns, which a projection may not carry — the engine
            // refuses the combination. Say so plainly rather than surfacing a 501 from underneath.
            if (cursorPaged)
                throw new BadRequestException(
                    "'fields' and 'cursor' cannot be combined. Use skip/take with a sparse fieldset, or drop " +
                    "'fields' to page by cursor.");

            var rows = await query
                .Project(request.Fields, typeInfo)
                .Paginate(request.Skip, request.Take)
                .ToList(ct)
                .ConfigureAwait(false);

            return Results.Content(new JsonArray(rows.Cast<JsonNode?>().ToArray()).ToJsonString(), "application/json");
        }

        if (cursorPaged)
        {
            var page = await query.ToJsonCursorPage(request.Cursor, request.Take, ct).ConfigureAwait(false);
            return Results.Content(Envelope(page.Items, page.NextCursor).ToJsonString(), "application/json");
        }

        query = query.Paginate(request.Skip, request.Take);

        // The raw lane: the stored bodies stream straight to the socket, never buffered, never re-serialized.
        // SupportsRawJson is false for an encrypted type — only the typed path decrypts — so it is probed,
        // not assumed, and the typed fallback keeps the endpoint working rather than turning into a 501.
        if (query.SupportsRawJson)
        {
            http.Response.ContentType = "application/json";
            await query.WriteJsonArrayTo(http.Response.Body, ct).ConfigureAwait(false);
            return Results.Empty;
        }

        var documents = await query.ToList(ct).ConfigureAwait(false);
        var json = new JsonArray(documents.Select(d => JsonNode.Parse(Serialize(d, typeInfo))).ToArray());
        return Results.Content(json.ToJsonString(), "application/json");
    }

    static async Task<IResult> GetById(HttpContext http, string id, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);
        var scope = await Scope(http, options).ConfigureAwait(false);

        // By-id materializes on purpose: the scope is evaluated in memory here, so there is nothing to save
        // by reading the raw body — and the version for the ETag comes off the same instance.
        var document = await store.Get(id, typeInfo, http.RequestAborted).ConfigureAwait(false);
        if (document is null || !scope.Contains(document))
            return Results.Problem($"No {typeof(T).Name} with id '{id}'.", statusCode: StatusCodes.Status404NotFound);

        SetETag(http, store, document);
        return Results.Content(Serialize(document, typeInfo), "application/json");
    }

    static async Task<IResult> Count(HttpContext http, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var request = ListRequest.Parse(http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields);
        var scope = await Scope(http, options).ConfigureAwait(false);
        var query = Build(store, options, scope, request);

        var count = await query.Count(http.RequestAborted).ConfigureAwait(false);
        return Results.Content(new JsonObject { ["count"] = count }.ToJsonString(), "application/json");
    }

    // ── Writes ──────────────────────────────────────────────────────────

    static async Task<IResult> Insert(HttpContext http, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);
        var scope = await Scope(http, options).ConfigureAwait(false);

        var document = await ReadBody(http, typeInfo).ConfigureAwait(false);
        if (!scope.Contains(document))
            return Results.Problem(
                "The document falls outside the scope this endpoint enforces and was not created.",
                statusCode: StatusCodes.Status400BadRequest);

        try
        {
            await store.Insert(document, typeInfo, http.RequestAborted).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("duplicate", StringComparison.OrdinalIgnoreCase))
        {
            return Results.Problem(ex.Message, statusCode: StatusCodes.Status409Conflict);
        }

        SetETag(http, store, document);
        var id = DocumentId(store, options, document);
        return Results.Created(id is null ? null : $"{http.Request.Path.Value?.TrimEnd('/')}/{id}", null);
    }

    static async Task<IResult> Replace(HttpContext http, string id, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);
        var scope = await Scope(http, options).ConfigureAwait(false);

        var ifMatch = IfMatch(http, options);
        var incoming = await ReadBody(http, typeInfo).ConfigureAwait(false);
        if (!scope.Contains(incoming))
            return Results.Problem(
                "The document falls outside the scope this endpoint enforces and was not saved.",
                statusCode: StatusCodes.Status400BadRequest);

        var existing = await store.Get(id, typeInfo, http.RequestAborted).ConfigureAwait(false);
        if (existing is null || !scope.Contains(existing))
            return Results.Problem($"No {typeof(T).Name} with id '{id}'.", statusCode: StatusCodes.Status404NotFound);

        var version = Version(store, http);
        if (ifMatch != null && version != null && version.GetVersion(existing) != ifMatch)
            return Precondition(version.GetVersion(existing));

        // Hand the store a version so its CAS has something to compare: the one the caller says it saw when
        // If-Match was supplied (a race between this read and the write is then the store's to catch), or the
        // current one when it was not — which is the documented last-writer-wins behavior of an optional
        // If-Match, rather than a 412 for every client that does not speak ETags.
        if (version != null)
            version.SetVersion(incoming, ifMatch ?? version.GetVersion(existing));

        await store.Update(incoming, typeInfo, http.RequestAborted).ConfigureAwait(false);

        var saved = await store.Get(id, typeInfo, http.RequestAborted).ConfigureAwait(false);
        if (saved != null)
            SetETag(http, store, saved);

        return Results.NoContent();
    }

    static async Task<IResult> Patch(HttpContext http, string id, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);
        var scope = await Scope(http, options).ConfigureAwait(false);
        var ifMatch = IfMatch(http, options);

        var existing = await store.Get(id, typeInfo, http.RequestAborted).ConfigureAwait(false);
        if (existing is null || !scope.Contains(existing))
            return Results.Problem($"No {typeof(T).Name} with id '{id}'.", statusCode: StatusCodes.Status404NotFound);

        var version = Version(store, http);
        if (ifMatch != null && version != null && version.GetVersion(existing) != ifMatch)
            return Precondition(version.GetVersion(existing));

        var patch = await JsonSerializer.DeserializeAsync<JsonObject>(http.Request.Body, cancellationToken: http.RequestAborted)
            .ConfigureAwait(false)
            ?? throw new BadRequestException("A PATCH body must be a JSON object.");

        // RFC 7396 is applied here, against the document we already read for the scope check, and the result is
        // written as a full replace. The store's own merge deliberately treats a null as "leave alone" — it has
        // to, because a serialized T carries nulls for every unset member — but over HTTP an explicit null is
        // the caller's word, and it means remove. Doing the merge here keeps that promise on every provider.
        // The write still goes through the type-keyed JSON collection, so it rides the full pipeline (tenancy,
        // temporal history, versioning/CAS, sidecars, interceptors).
        var merged = JsonNode.Parse(Serialize(existing, typeInfo))!.AsObject();
        MergePatch.Apply(merged, patch);

        var collection = store.Collection(typeof(T));
        merged[collection.IdProperty] ??= id;
        await collection.Update(merged, patch: false, http.RequestAborted).ConfigureAwait(false);

        var saved = await store.Get(id, typeInfo, http.RequestAborted).ConfigureAwait(false);
        if (saved is null || !scope.Contains(saved))
            return Results.Problem(
                "The patch would move the document outside the scope this endpoint enforces.",
                statusCode: StatusCodes.Status400BadRequest);

        SetETag(http, store, saved);
        return Results.NoContent();
    }

    static async Task<IResult> Delete(HttpContext http, string id, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);
        var scope = await Scope(http, options).ConfigureAwait(false);
        var ifMatch = IfMatch(http, options);

        var existing = await store.Get(id, typeInfo, http.RequestAborted).ConfigureAwait(false);
        if (existing is null || !scope.Contains(existing))
            return Results.Problem($"No {typeof(T).Name} with id '{id}'.", statusCode: StatusCodes.Status404NotFound);

        var version = Version(store, http);
        if (ifMatch != null && version != null && version.GetVersion(existing) != ifMatch)
            return Precondition(version.GetVersion(existing));

        await store.Remove<T>(id, http.RequestAborted).ConfigureAwait(false);
        return Results.NoContent();
    }

    // ── SSE ─────────────────────────────────────────────────────────────

    static async Task Stream(HttpContext http, DocumentEndpointOptions<T> options)
    {
        var store = Store(http, options);
        var typeInfo = TypeInfo(store, options);

        ScopeSet scope;
        Func<T, bool>? filter = null;
        try
        {
            var request = ListRequest.Parse(http.Request.Query, options.DefaultPageSize, options.MaxPageSize, options.AllowedFields);

            // The scope is evaluated ONCE, when the connection opens. A stream can outlive any sane notion of
            // "current permissions", so the app closes the connection when it needs re-authorization; the
            // values are captured here, never the services they came from.
            scope = await Scope(http, options).ConfigureAwait(false);

            if (request.Filter != null && typeInfo != null)
            {
                var predicate = FilterExpressionParser.Parse(request.Filter, typeInfo);
                filter = ExpressionInterpreter.Interpret(predicate);
            }
        }
        catch (Exception ex) when (ex is BadRequestException or ArgumentException)
        {
            http.Response.StatusCode = StatusCodes.Status400BadRequest;
            await http.Response.WriteAsync(ex.Message, http.RequestAborted).ConfigureAwait(false);
            return;
        }

        http.Response.ContentType = "text/event-stream";
        http.Response.Headers.CacheControl = "no-cache";
        http.Response.Headers["X-Accel-Buffering"] = "no";
        await http.Response.Body.FlushAsync(http.RequestAborted).ConfigureAwait(false);

        await using var heartbeat = new Timer(
            _ => _ = WriteHeartbeat(http),
            null,
            options.StreamHeartbeat,
            options.StreamHeartbeat);

        try
        {
            await foreach (var change in store.NotifyOnChange<T>(http.RequestAborted).ConfigureAwait(false))
            {
                var document = change.Document;

                // A delete carries no document, so neither the scope nor the filter can be evaluated against
                // it; those events are dropped rather than leaked past a scope that cannot be checked.
                var visible = document != null && scope.Contains(document) && (filter is null || filter(document));
                if (visible)
                    await WriteEvent(http, change, typeInfo).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The client went away. The enumeration's disposal releases the store subscription.
        }
    }

    static async Task WriteHeartbeat(HttpContext http)
    {
        try
        {
            await http.Response.WriteAsync(": keep-alive\n\n", http.RequestAborted).ConfigureAwait(false);
            await http.Response.Body.FlushAsync(http.RequestAborted).ConfigureAwait(false);
        }
        catch
        {
            // The connection is gone; the enumeration below notices on its own.
        }
    }

    static async Task WriteEvent(HttpContext http, DocumentChange<T> change, JsonTypeInfo<T>? typeInfo)
    {
        var name = change.ChangeType switch
        {
            DocumentChangeType.Inserted => "insert",
            DocumentChangeType.Updated => "update",
            DocumentChangeType.Removed => "delete",
            _ => "clear"
        };

        var payload = new JsonObject
        {
            ["id"] = change.Id,
            ["document"] = change.Document is null ? null : JsonNode.Parse(Serialize(change.Document, typeInfo))
        };

        await http.Response.WriteAsync($"event: {name}\ndata: {payload.ToJsonString()}\n\n", http.RequestAborted).ConfigureAwait(false);
        await http.Response.Body.FlushAsync(http.RequestAborted).ConfigureAwait(false);
    }

    // ── ETag / concurrency ──────────────────────────────────────────────

    static void SetETag(HttpContext http, IDocumentStore store, T document)
    {
        var version = Version(store, http);
        if (version != null)
            http.Response.Headers.ETag = $"\"{version.GetVersion(document)}\"";
    }

    static int? IfMatch(HttpContext http, DocumentEndpointOptions<T> options)
    {
        var header = http.Request.Headers.IfMatch.ToString();
        if (string.IsNullOrWhiteSpace(header))
        {
            if (options.RequireIfMatch)
                throw new PreconditionRequiredException();
            return null;
        }

        var trimmed = header.Trim().Trim('"');
        if (trimmed == "*")
            return null;

        return int.TryParse(trimmed, out var version)
            ? version
            : throw new BadRequestException($"If-Match must be the document's version, e.g. If-Match: \"3\" (got '{header}').");
    }

    static IResult Precondition(int current)
        => Results.Problem(
            $"The document has changed since the version you supplied (current version {current}). Re-read it and retry.",
            statusCode: StatusCodes.Status412PreconditionFailed);

    static async Task<T> ReadBody(HttpContext http, JsonTypeInfo<T>? typeInfo)
    {
        var document = typeInfo is null
            ? await JsonSerializer.DeserializeAsync<T>(http.Request.Body, cancellationToken: http.RequestAborted).ConfigureAwait(false)
            : await JsonSerializer.DeserializeAsync(http.Request.Body, typeInfo, http.RequestAborted).ConfigureAwait(false);

        return document ?? throw new BadRequestException("A request body is required.");
    }

    // The Location header needs whatever id the store just assigned — read back through the same accessor the
    // store uses, so a mapped/renamed id property is honored rather than guessed at.
    static string? DocumentId(IDocumentStore store, DocumentEndpointOptions<T> options, T document)
    {
        try
        {
            return IdAccessor<T>.Create(TypeInfo(store, options)).GetIdAsString(document);
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>Thrown when <c>RequireIfMatch</c> is on and the caller sent no <c>If-Match</c>.</summary>
sealed class PreconditionRequiredException() : Exception(
    "This resource requires an If-Match header carrying the document version you read.");
