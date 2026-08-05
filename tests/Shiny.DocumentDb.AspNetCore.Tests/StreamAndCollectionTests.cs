using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.AspNetCore.Tests;

/// <summary>
/// The live tail and the schema-free lane — the two halves of the package that are not plain CRUD.
/// </summary>
public class StreamAndCollectionTests
{
    // ── SSE ─────────────────────────────────────────────────────────────

    static async Task<List<(string Event, JsonNode Data)>> ReadEvents(
        Stream stream,
        int expected,
        CancellationToken cancellationToken)
    {
        var events = new List<(string, JsonNode)>();
        using var reader = new StreamReader(stream);
        string? name = null;

        while (events.Count < expected && !cancellationToken.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (line is null)
                break;

            if (line.StartsWith("event: ", StringComparison.Ordinal))
                name = line["event: ".Length..];
            else if (line.StartsWith("data: ", StringComparison.Ordinal) && name != null)
                events.Add((name, JsonNode.Parse(line["data: ".Length..])!));
        }
        return events;
    }

    [Fact]
    public async Task StreamEmitsInsertAndUpdateEventsMatchingTheFilter()
    {
        await using var fixture = new EndpointFixture(o =>
        {
            o.Operations = DocumentEndpoints.Read | DocumentEndpoints.Stream;
            o.StreamHeartbeat = TimeSpan.FromMilliseconds(100);
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        var client = fixture.Client();
        var response = await client.GetAsync("/orders/stream?filter=status == 'open'", HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        var reader = ReadEvents(stream, expected: 2, cts.Token);

        // Give the subscription a moment to attach before writing.
        await Task.Delay(200, cts.Token);
        await fixture.Store.Insert(new Order { Id = "s1", Status = "open", CustomerId = "acme", Total = 1m }, fixture.JsonContext.Order, cts.Token);
        await fixture.Store.Insert(new Order { Id = "s2", Status = "closed", CustomerId = "acme", Total = 2m }, fixture.JsonContext.Order, cts.Token);
        await fixture.Store.Upsert(new Order { Id = "s1", Status = "open", CustomerId = "acme", Total = 3m }, fixture.JsonContext.Order, cts.Token);

        var events = await reader;

        Assert.Equal(2, events.Count);
        Assert.Equal("insert", events[0].Event);
        Assert.Equal("s1", events[0].Data["id"]!.GetValue<string>());

        // The closed order never arrives — it does not match the client's filter.
        Assert.All(events, e => Assert.NotEqual("s2", e.Data["id"]!.GetValue<string>()));

        cts.Cancel();
    }

    [Fact]
    public async Task StreamHonoursTheScope()
    {
        await using var fixture = new EndpointFixture(o =>
        {
            o.Operations = DocumentEndpoints.Read | DocumentEndpoints.Stream;
            o.Scope<ICustomerContext>((customer, _) => x => x.CustomerId == customer.CustomerId);
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        var response = await fixture.Client("globex")
            .GetAsync("/orders/stream", HttpCompletionOption.ResponseHeadersRead, cts.Token);

        var stream = await response.Content.ReadAsStreamAsync(cts.Token);
        var reader = ReadEvents(stream, expected: 1, cts.Token);

        await Task.Delay(200, cts.Token);
        await fixture.Store.Insert(new Order { Id = "acme-1", Status = "open", CustomerId = "acme", Total = 1m }, fixture.JsonContext.Order, cts.Token);
        await fixture.Store.Insert(new Order { Id = "globex-1", Status = "open", CustomerId = "globex", Total = 2m }, fixture.JsonContext.Order, cts.Token);

        var events = await reader;

        Assert.Single(events);
        Assert.Equal("globex-1", events[0].Data["id"]!.GetValue<string>());

        cts.Cancel();
    }

    [Fact]
    public void StreamOnAProviderWithoutChangeMonitoring_IsRefusedAtStartup()
    {
        // A store with no IObservableDocumentStore is refused at map time rather than 501-ing per request.
        var ex = Assert.Throws<InvalidOperationException>(() => new NonObservableFixture());
        Assert.Contains("IObservableDocumentStore", ex.Message, StringComparison.Ordinal);
    }

    sealed class NonObservableFixture
    {
        public NonObservableFixture()
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IDocumentStore>(new NonObservableStore());

            var app = builder.Build();
            app.MapDocuments<Order>("/orders", o => o.Operations = DocumentEndpoints.Stream);
        }
    }

    // ── Schema-free collections ─────────────────────────────────────────

    static EndpointFixture Collections(Action<DocumentCollectionEndpointOptions>? configure = null)
        => new(
            mapOrders: false,
            mapExtra: app => app.MapDocumentCollection("/intake", "intake_forms", o =>
            {
                o.Operations = DocumentEndpoints.Read | DocumentEndpoints.Write | DocumentEndpoints.Delete | DocumentEndpoints.Count;
                configure?.Invoke(o);
            }));

    [Fact]
    public async Task CollectionCrudRoundTrip()
    {
        await using var fixture = Collections();
        var client = fixture.Client();

        var created = await client.PostAsync("/intake",
            new StringContent("""{"id":"f1","name":"Ada","score":90}""", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.Created, created.StatusCode);

        var fetched = JsonNode.Parse(await (await client.GetAsync("/intake/f1")).Content.ReadAsStringAsync())!;
        Assert.Equal("Ada", fetched["name"]!.GetValue<string>());

        var patched = await client.PatchAsync("/intake/f1",
            new StringContent("""{"score":95}""", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.NoContent, patched.StatusCode);

        var after = JsonNode.Parse(await (await client.GetAsync("/intake/f1")).Content.ReadAsStringAsync())!;
        Assert.Equal(95, after["score"]!.GetValue<int>());
        Assert.Equal("Ada", after["name"]!.GetValue<string>());

        Assert.Equal(HttpStatusCode.NoContent, (await client.DeleteAsync("/intake/f1")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/intake/f1")).StatusCode);
    }

    [Fact]
    public async Task CollectionScopeHidesOtherTenantsDocuments()
    {
        await using var fixture = Collections(o => o
            .Scope<ICustomerContext>((customer, _) => $"tenant == '{customer.CustomerId}'"));

        var collection = fixture.Store.Collection("intake_forms");
        await collection.Insert(JsonNode.Parse("""{"id":"a","tenant":"acme"}""")!.AsObject());
        await collection.Insert(JsonNode.Parse("""{"id":"g","tenant":"globex"}""")!.AsObject());

        var acme = JsonNode.Parse(await (await fixture.Client("acme").GetAsync("/intake")).Content.ReadAsStringAsync())!;
        Assert.Single((JsonArray)acme);

        // Out of scope is "not found", never "forbidden".
        Assert.Equal(HttpStatusCode.NotFound, (await fixture.Client("acme").GetAsync("/intake/g")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await fixture.Client("acme").DeleteAsync("/intake/g")).StatusCode);
    }

    [Fact]
    public async Task AScopedCollectionRefusesWrites()
    {
        await using var fixture = Collections(o => o.Scope(_ => "tenant == 'acme'"));

        var response = await fixture.Client().PostAsync("/intake",
            new StringContent("""{"id":"x","tenant":"globex"}""", Encoding.UTF8, "application/json"));

        // A schema-free body cannot be checked against the scope, so the combination is refused rather than
        // silently weakened — same rule the AI collection lane applies.
        Assert.Equal(HttpStatusCode.NotImplemented, response.StatusCode);
    }

    [Fact]
    public async Task ACollectionSparseFieldsetStillPages()
    {
        await using var fixture = Collections(o => o.MaxPageSize = 2);

        var collection = fixture.Store.Collection("intake_forms");
        for (var i = 0; i < 5; i++)
            await collection.Insert(JsonNode.Parse($$"""{"id":"f{{i}}","name":"n{{i}}","score":{{i}}}""")!.AsObject());

        var page = JsonNode.Parse(await (await fixture.Client().GetAsync("/intake?fields=id&take=100")).Content.ReadAsStringAsync())!;
        Assert.Equal(2, ((JsonArray)page).Count);
    }

    [Fact]
    public async Task CollectionFilteringAndAllowlist()
    {
        await using var fixture = Collections(o => o.AllowFilterOn("score"));

        var collection = fixture.Store.Collection("intake_forms");
        await collection.Insert(JsonNode.Parse("""{"id":"a","name":"Ada","score":90}""")!.AsObject());
        await collection.Insert(JsonNode.Parse("""{"id":"b","name":"Bob","score":40}""")!.AsObject());

        var client = fixture.Client();
        var filtered = JsonNode.Parse(await (await client.GetAsync("/intake?filter=score:int > 50")).Content.ReadAsStringAsync())!;
        Assert.Single((JsonArray)filtered);

        var refused = await client.GetAsync("/intake?filter=name == 'Ada'");
        Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
    }
}

/// <summary>A store with no change monitoring, for the startup-refusal test. Every member is unreachable.</summary>
file sealed class NonObservableStore : IDocumentStore
{
    const string Unused = "This store exists to be rejected at startup.";

    public IDocumentQuery<T> Query<T>(System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null) where T : class => throw new NotSupportedException(Unused);
    public Task Insert<T>(T document, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<int> BatchInsert<T>(IEnumerable<T> documents, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task Update<T>(T document, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task Upsert<T>(T patch, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<bool> SetProperty<T>(object id, System.Linq.Expressions.Expression<Func<T, object>> property, object? value, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<bool> RemoveProperty<T>(object id, System.Linq.Expressions.Expression<Func<T, object>> property, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<T?> Get<T>(object id, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<JsonPatchDocument<T>?> GetDiff<T>(object id, T modified, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<IReadOnlyList<T>> Query<T>(string whereClause, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public IAsyncEnumerable<T> QueryStream<T>(string whereClause, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>? jsonTypeInfo = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<int> Count<T>(string? whereClause = null, object? parameters = null, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<bool> Remove<T>(object id, CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
    public Task<int> Clear<T>(CancellationToken cancellationToken = default) where T : class => throw new NotSupportedException(Unused);
}
