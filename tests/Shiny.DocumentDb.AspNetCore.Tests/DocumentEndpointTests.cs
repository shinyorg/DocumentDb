using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Shiny.DocumentDb.AspNetCore.Tests;

/// <summary>
/// The REST surface end to end over a real host: CRUD, filtering and its allowlist, paging, sparse fieldsets,
/// ETag/If-Match concurrency, and the server-side scope — which is the security-critical part and is therefore
/// asserted on every verb, not just on reads.
/// </summary>
public class DocumentEndpointTests
{
    static async Task<JsonNode> Json(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        return JsonNode.Parse(await response.Content.ReadAsStringAsync())!;
    }

    static StringContent Body(object value, JsonSerializerOptions options)
        => new(JsonSerializer.Serialize(value, options), Encoding.UTF8, "application/json");

    // ── CRUD ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListReturnsEveryDocument()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client().GetAsync("/orders"));
        Assert.Equal(3, array.Count);
    }

    [Fact]
    public async Task GetByIdReturnsTheDocument_And404sForAMissingOne()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var order = await Json(await fixture.Client().GetAsync("/orders/o1"));
        Assert.Equal("open", order["status"]!.GetValue<string>());

        var missing = await fixture.Client().GetAsync("/orders/nope");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
        Assert.Equal("application/problem+json", missing.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task CrudRoundTrip()
    {
        await using var fixture = new EndpointFixture();
        var client = fixture.Client();

        var created = await client.PostAsync("/orders",
            Body(new Order { Id = "new-1", Status = "open", CustomerId = "acme", Total = 5m }, fixture.JsonContext.Options));

        Assert.Equal(HttpStatusCode.Created, created.StatusCode);
        Assert.Equal("/orders/new-1", created.Headers.Location?.ToString());

        var replaced = await client.PutAsync("/orders/new-1",
            Body(new Order { Id = "new-1", Status = "closed", CustomerId = "acme", Total = 6m }, fixture.JsonContext.Options));
        Assert.Equal(HttpStatusCode.NoContent, replaced.StatusCode);
        Assert.Equal("closed", (await Json(await client.GetAsync("/orders/new-1")))["status"]!.GetValue<string>());

        var deleted = await client.DeleteAsync("/orders/new-1");
        Assert.Equal(HttpStatusCode.NoContent, deleted.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.DeleteAsync("/orders/new-1")).StatusCode);
    }

    [Fact]
    public async Task PatchIsAMergePatch_PreservingUnspecifiedFieldsAndRemovingExplicitNulls()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();
        var client = fixture.Client();

        var response = await client.PatchAsync("/orders/o1",
            new StringContent("""{"status":"closed"}""", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);

        var patched = await Json(await client.GetAsync("/orders/o1"));
        Assert.Equal("closed", patched["status"]!.GetValue<string>());
        Assert.Equal(100m, patched["total"]!.GetValue<decimal>());     // untouched
        Assert.Equal("first", patched["notes"]!.GetValue<string>());   // untouched

        await client.PatchAsync("/orders/o1", new StringContent("""{"notes":null}""", Encoding.UTF8, "application/json"));
        var cleared = await Json(await client.GetAsync("/orders/o1"));
        Assert.True(cleared["notes"] is null || cleared["notes"]!.GetValueKind() == JsonValueKind.Null);
    }

    // ── Filtering + allowlist ───────────────────────────────────────────

    [Fact]
    public async Task FilterOnAnAllowedFieldWorks()
    {
        await using var fixture = new EndpointFixture(o => o.AllowFilterOn(x => x.Status, x => x.Total));
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client().GetAsync("/orders?filter=status == 'open' and total > 500"));
        Assert.Single(array);
        Assert.Equal("o3", array[0]!["id"]!.GetValue<string>());
    }

    [Fact]
    public async Task FilterOnANonAllowedField_Is400NamingTheField()
    {
        await using var fixture = new EndpointFixture(o => o.AllowFilterOn(x => x.Status));
        await fixture.Seed();

        var response = await fixture.Client().GetAsync("/orders?filter=notes == 'first'");
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("notes", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheIdIsAlwaysAvailable_WhateverTheAllowlistSays()
    {
        await using var fixture = new EndpointFixture(o => o.AllowFilterOn(x => x.Status));
        await fixture.Seed();
        var client = fixture.Client();

        // The id is in the route and in every response body — refusing it would protect nothing and make a
        // sparse fieldset useless.
        var projected = (JsonArray)await Json(await client.GetAsync("/orders?fields=id,status&take=1"));
        Assert.Equal(new[] { "id", "status" }, ((JsonObject)projected[0]!).Select(p => p.Key).Order());

        Assert.Single((JsonArray)await Json(await client.GetAsync("/orders?filter=id == 'o1'")));
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/orders?orderby=id desc")).StatusCode);
    }

    [Fact]
    public async Task AFieldNameInsideAStringLiteralIsNotMistakenForAField()
    {
        await using var fixture = new EndpointFixture(o => o.AllowFilterOn(x => x.Status));
        await fixture.Seed();

        // 'notes' is not allowlisted, but here it is a value, not a field.
        var response = await fixture.Client().GetAsync("/orders?filter=status == 'notes'");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ScalarFunctionsStillWorkUnderAnAllowlist()
    {
        await using var fixture = new EndpointFixture(o => o.AllowFilterOn(x => x.Status));
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client().GetAsync("/orders?filter=lower(status) == 'open'"));
        Assert.Equal(2, array.Count);
    }

    [Fact]
    public async Task AMalformedFilterIs400_Never500()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var response = await fixture.Client().GetAsync("/orders?filter=status ==");
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task DeeplyNestedBooleanInputIsRejectedOrHandled_NeverA500()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var hostile = string.Concat(Enumerable.Repeat("(", 500)) + "status == 'open'" + string.Concat(Enumerable.Repeat(")", 500));
        var response = await fixture.Client().GetAsync("/orders?filter=" + Uri.EscapeDataString(hostile));
        Assert.True(
            response.StatusCode is HttpStatusCode.OK or HttpStatusCode.BadRequest,
            $"Expected 200 or 400 for hostile input, got {(int)response.StatusCode}.");
    }

    // ── Paging, sorting, projection ─────────────────────────────────────

    [Fact]
    public async Task TakeIsClampedToMaxPageSize()
    {
        await using var fixture = new EndpointFixture(o => o.MaxPageSize = 2);
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client().GetAsync("/orders?take=100"));
        Assert.Equal(2, array.Count);
    }

    [Fact]
    public async Task CursorPagingWalksTheWholeSet()
    {
        await using var fixture = new EndpointFixture(o => o.MaxPageSize = 2);
        await fixture.Seed();
        var client = fixture.Client();

        var seen = new List<string>();
        string? cursor = null;
        for (var page = 0; page < 5; page++)
        {
            var url = cursor is null ? "/orders?cursor=&take=2" : $"/orders?cursor={Uri.EscapeDataString(cursor)}&take=2";
            var envelope = await Json(await client.GetAsync(url));
            foreach (var item in (JsonArray)envelope["items"]!)
                seen.Add(item!["id"]!.GetValue<string>());

            cursor = envelope["nextCursor"]?.GetValue<string>();
            if (cursor is null)
                break;
        }

        Assert.Equal(new[] { "o1", "o2", "o3" }, seen.Order());
    }

    [Fact]
    public async Task SortingUsesTheRequestedKey()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client().GetAsync("/orders?orderby=total desc"));
        Assert.Equal("o3", array[0]!["id"]!.GetValue<string>());
    }

    [Fact]
    public async Task SparseFieldsetReturnsOnlyThoseKeys()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client().GetAsync("/orders?fields=id,total"));
        var first = (JsonObject)array[0]!;
        Assert.Equal(new[] { "id", "total" }, first.Select(p => p.Key).Order());
    }

    [Fact]
    public async Task ASparseFieldsetStillPages()
    {
        await using var fixture = new EndpointFixture(o => o.MaxPageSize = 2);
        await fixture.Seed();
        var client = fixture.Client();

        // Projection is not an escape hatch out of the page cap.
        Assert.Single((JsonArray)await Json(await client.GetAsync("/orders?fields=id,total&take=1")));
        Assert.Equal(2, ((JsonArray)await Json(await client.GetAsync("/orders?fields=id,total&take=100"))).Count);

        // Cursor paging is built from the sort keys a projection may drop, so the combination is refused
        // with a message that says what to do instead — not a 501 from the engine.
        var combined = await client.GetAsync("/orders?fields=id,total&cursor=&take=2");
        Assert.Equal(HttpStatusCode.BadRequest, combined.StatusCode);
        Assert.Contains("cannot be combined", await combined.Content.ReadAsStringAsync(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CountRespectsTheFilter()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();

        var body = await Json(await fixture.Client().GetAsync("/orders/count?filter=status == 'open'"));
        Assert.Equal(2, body["count"]!.GetValue<int>());
    }

    // ── Concurrency ─────────────────────────────────────────────────────

    [Fact]
    public async Task ETagChangesAfterAnUpdate_AndAStaleIfMatchIs412()
    {
        await using var fixture = new EndpointFixture();
        await fixture.Seed();
        var client = fixture.Client();

        var first = await client.GetAsync("/orders/o1");
        var etag = first.Headers.ETag!.Tag;

        var replace = new HttpRequestMessage(HttpMethod.Put, "/orders/o1")
        {
            Content = Body(new Order { Id = "o1", Status = "closed", CustomerId = "acme", Total = 100m }, fixture.JsonContext.Options)
        };
        replace.Headers.TryAddWithoutValidation("If-Match", etag);
        Assert.Equal(HttpStatusCode.NoContent, (await client.SendAsync(replace)).StatusCode);

        var second = await client.GetAsync("/orders/o1");
        Assert.NotEqual(etag, second.Headers.ETag!.Tag);

        // The original ETag is now stale.
        var stale = new HttpRequestMessage(HttpMethod.Put, "/orders/o1")
        {
            Content = Body(new Order { Id = "o1", Status = "reopened", CustomerId = "acme", Total = 100m }, fixture.JsonContext.Options)
        };
        stale.Headers.TryAddWithoutValidation("If-Match", etag);
        Assert.Equal(HttpStatusCode.PreconditionFailed, (await client.SendAsync(stale)).StatusCode);
    }

    [Fact]
    public async Task RequireIfMatch_WithoutTheHeaderIs428()
    {
        await using var fixture = new EndpointFixture(o => o.RequireIfMatch = true);
        await fixture.Seed();

        var response = await fixture.Client().DeleteAsync("/orders/o1");
        Assert.Equal(HttpStatusCode.PreconditionRequired, response.StatusCode);
    }

    // ── Scope ───────────────────────────────────────────────────────────

    static EndpointFixture Scoped() => new(o => o
        .Scope<ICustomerContext>((customer, _) => x => x.CustomerId == customer.CustomerId));

    [Fact]
    public async Task ScopeFiltersReadsAndHidesEverythingElse()
    {
        await using var fixture = Scoped();
        await fixture.Seed();

        var acme = (JsonArray)await Json(await fixture.Client("acme").GetAsync("/orders"));
        Assert.Equal(2, acme.Count);

        var globex = (JsonArray)await Json(await fixture.Client("globex").GetAsync("/orders"));
        Assert.Single(globex);
    }

    [Fact]
    public async Task AnOutOfScopeDocumentIs404_OnGetPutPatchAndDelete()
    {
        await using var fixture = Scoped();
        await fixture.Seed();
        var client = fixture.Client("acme");

        // o3 belongs to globex. Every verb reports "not found" — never 403, which would confirm it exists.
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/orders/o3")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.DeleteAsync("/orders/o3")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.PatchAsync("/orders/o3",
            new StringContent("""{"status":"hijacked"}""", Encoding.UTF8, "application/json"))).StatusCode);

        var put = await client.PutAsync("/orders/o3",
            Body(new Order { Id = "o3", Status = "hijacked", CustomerId = "acme", Total = 900m }, fixture.JsonContext.Options));
        Assert.Equal(HttpStatusCode.NotFound, put.StatusCode);

        Assert.Equal("open", (await fixture.Store.Get<Order>("o3", fixture.JsonContext.Order))!.Status);
    }

    [Fact]
    public async Task ADocumentOutsideTheScopeCannotBeCreated()
    {
        await using var fixture = Scoped();

        var response = await fixture.Client("acme").PostAsync("/orders",
            Body(new Order { Id = "sneaky", CustomerId = "globex", Total = 1m }, fixture.JsonContext.Options));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(await fixture.Store.Get<Order>("sneaky", fixture.JsonContext.Order));
    }

    [Fact]
    public async Task TwoScopesAreAndedTogether()
    {
        await using var fixture = new EndpointFixture(o => o
            .Scope<ICustomerContext>((customer, _) => x => x.CustomerId == customer.CustomerId)
            .Scope(_ => x => x.Status == "open"));
        await fixture.Seed();

        var array = (JsonArray)await Json(await fixture.Client("acme").GetAsync("/orders"));
        Assert.Single(array);
        Assert.Equal("o1", array[0]!["id"]!.GetValue<string>());
    }

    [Fact]
    public async Task DenyAllReturnsNothing_AndA404ById()
    {
        await using var fixture = new EndpointFixture(o => o.Scope(_ => DocumentScope.DenyAll<Order>()));
        await fixture.Seed();
        var client = fixture.Client();

        Assert.Empty((JsonArray)await Json(await client.GetAsync("/orders")));
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/orders/o1")).StatusCode);
    }

    [Fact]
    public async Task TheAsyncScopeOverloadIsAwaited()
    {
        await using var fixture = new EndpointFixture(o => o.Scope<ICustomerContext>(async (customer, ctx) =>
        {
            await Task.Delay(5, ctx.CancellationToken);
            var id = customer.CustomerId;
            return x => x.CustomerId == id;
        }));
        await fixture.Seed();

        Assert.Single((JsonArray)await Json(await fixture.Client("globex").GetAsync("/orders")));
    }

    [Fact]
    public async Task TheScopeServiceIsTheRequestScopedInstance()
    {
        var seen = new List<Guid>();
        await using var fixture = new EndpointFixture(o => o.Scope<ICustomerContext>((customer, _) =>
        {
            lock (seen) seen.Add(customer.InstanceId);
            var id = customer.CustomerId;
            return x => x.CustomerId == id;
        }));
        await fixture.Seed();

        await fixture.Client("acme").GetAsync("/orders");
        await fixture.Client("acme").GetAsync("/orders");

        // Scoped, not singleton: two requests, two instances.
        Assert.Equal(2, seen.Count);
        Assert.NotEqual(seen[0], seen[1]);
    }

    [Fact]
    public void AScopeServiceThatIsNotRegistered_FailsAtStartup()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new EndpointFixture(
            o => o.Scope<IMissingService>((svc, _) => x => x.Status == svc.ToString())));

        Assert.Contains(nameof(IMissingService), ex.Message, StringComparison.Ordinal);
    }

    public interface IMissingService;

    // ── Startup guards ──────────────────────────────────────────────────

    [Fact]
    public async Task OperationsNotMappedAreNotRoutes()
    {
        await using var fixture = new EndpointFixture(o => o.Operations = DocumentEndpoints.Read);
        await fixture.Seed();
        var client = fixture.Client();

        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/orders")).StatusCode);

        // /orders/{id} exists for GET, so an unmapped verb on it is a 405 — not a 404, which would imply the
        // resource itself is absent.
        Assert.Equal(HttpStatusCode.MethodNotAllowed, (await client.DeleteAsync("/orders/o1")).StatusCode);

        // /orders/count was never mapped at all.
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/orders/count")).StatusCode);
    }

    [Fact]
    public async Task TheOpenApiMetadataNamesEveryMappedOperation()
    {
        await using var fixture = new EndpointFixture(o => o.Operations = DocumentEndpoints.All & ~DocumentEndpoints.Stream);

        var endpoints = fixture.Services.GetRequiredService<Microsoft.AspNetCore.Routing.EndpointDataSource>().Endpoints;
        var names = endpoints
            .Select(e => e.Metadata.GetMetadata<Microsoft.AspNetCore.Routing.IEndpointNameMetadata>()?.EndpointName)
            .Where(n => n != null)
            .ToList();

        Assert.Contains("ListOrder", names);
        Assert.Contains("GetOrder", names);
        Assert.Contains("CountOrder", names);
        Assert.Contains("CreateOrder", names);
        Assert.Contains("ReplaceOrder", names);
        Assert.Contains("PatchOrder", names);
        Assert.Contains("DeleteOrder", names);

        var byId = endpoints.Single(e =>
            e.Metadata.GetMetadata<Microsoft.AspNetCore.Routing.IEndpointNameMetadata>()?.EndpointName == "GetOrder");
        var responses = byId.Metadata.GetOrderedMetadata<Microsoft.AspNetCore.Http.Metadata.IProducesResponseTypeMetadata>();
        Assert.Contains(responses, r => r.StatusCode == 200 && r.Type == typeof(Order));
        Assert.Contains(responses, r => r.StatusCode == 404);
    }
}
