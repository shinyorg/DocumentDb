using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.AI;
using ModelContextProtocol.Server;

namespace Shiny.DocumentDb.Mcp.Internal;

/// <summary>
/// The orientation resources. A model that can read <c>documentdb://types</c> and one schema stops guessing
/// tool arguments, which is worth more than any amount of tool-description prose.
/// </summary>
/// <remarks>
/// Every resource is a hand-written <see cref="AIFunction"/> with a hand-written JSON schema — no reflection
/// over a delegate — and every one that touches data does so <b>through the registered tools</b>, so the
/// non-removable scope, the page caps and the hidden properties apply to a resource read exactly as they do
/// to a tool call. A resource that read the store directly would be a hole in the scope.
/// </remarks>
static class McpResourceFactory
{
    public static IReadOnlyList<McpServerResource> Build(DocumentDbMcpPrimitives primitives, int sampleSize)
    {
        var resources = new List<McpServerResource>
        {
            McpServerResource.Create(
                new TypesResource(primitives),
                new McpServerResourceCreateOptions
                {
                    UriTemplate = "documentdb://types",
                    Name = "documentdb-types",
                    Title = "Document types",
                    Description = "Every type and collection this server exposes, with its capabilities and fields.",
                    MimeType = "application/json"
                }),

            McpServerResource.Create(
                new SchemaResource(primitives),
                new McpServerResourceCreateOptions
                {
                    UriTemplate = "documentdb://types/{name}/schema",
                    Name = "documentdb-type-schema",
                    Title = "Document type schema",
                    Description = "JSON Schema for one exposed type or collection.",
                    MimeType = "application/json"
                }),

            McpServerResource.Create(
                new SampleResource(primitives, sampleSize),
                new McpServerResourceCreateOptions
                {
                    UriTemplate = "documentdb://types/{name}/sample",
                    Name = "documentdb-type-sample",
                    Title = "Document type sample",
                    Description = "A few real documents of one exposed type — the fastest way to learn its shape.",
                    MimeType = "application/json"
                }),

            McpServerResource.Create(
                new StatsResource(primitives),
                new McpServerResourceCreateOptions
                {
                    UriTemplate = "documentdb://stats",
                    Name = "documentdb-stats",
                    Title = "Store statistics",
                    Description = "Provider, capability flags and per-type document counts.",
                    MimeType = "application/json"
                })
        };
        return resources;
    }

    // ── The functions behind them ───────────────────────────────────────

    abstract class ResourceFunction : AIFunction
    {
        static readonly JsonElement NoArguments = Schema("""{"type":"object","properties":{},"required":[]}""");
        static readonly JsonElement NameArgument = Schema(
            """
            {"type":"object","properties":{"name":{"type":"string","description":"The exposed type or collection slug."}},"required":["name"]}
            """);

        protected ResourceFunction(DocumentDbMcpPrimitives primitives, bool takesName)
        {
            this.Primitives = primitives;
            this.JsonSchema = takesName ? NameArgument : NoArguments;
        }

        protected DocumentDbMcpPrimitives Primitives { get; }
        public override JsonElement JsonSchema { get; }

        static JsonElement Schema(string json) => JsonDocument.Parse(json).RootElement.Clone();

        protected static string Slug(AIFunctionArguments arguments)
            => arguments.TryGetValue("name", out var raw) && raw is not null
                ? (raw as string ?? (raw is JsonElement je ? je.GetString() : null))
                  ?? throw new InvalidOperationException("'name' must be a string.")
                : throw new InvalidOperationException("'name' is required.");

        protected static string Json(JsonNode node) => node.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    sealed class TypesResource(DocumentDbMcpPrimitives primitives) : ResourceFunction(primitives, takesName: false)
    {
        public override string Name => "documentdb-types";
        public override string Description => "Every type and collection this server exposes.";

        protected override ValueTask<object?> InvokeCoreAsync(AIFunctionArguments arguments, CancellationToken cancellationToken)
        {
            var array = new JsonArray();
            foreach (var d in this.Primitives.Descriptors)
            {
                var fields = new JsonArray();
                foreach (var f in d.Fields)
                    fields.Add(f);

                array.Add(new JsonObject
                {
                    ["name"] = d.Slug,
                    ["kind"] = d.Kind,
                    ["description"] = d.Description,
                    ["capabilities"] = d.Capabilities.ToString(),
                    ["maxPageSize"] = d.MaxPageSize,
                    // The model is told a scope EXISTS (so "no results" is not a mystery) but never what it is.
                    ["serverScoped"] = d.IsScoped,
                    ["fields"] = fields,
                    ["tools"] = new JsonArray(this.Primitives.Functions
                        .Where(f => f.Name.StartsWith(d.Slug + "_", StringComparison.Ordinal))
                        .Select(f => (JsonNode)f.Name!)
                        .ToArray())
                });
            }
            return ValueTask.FromResult<object?>(Json(new JsonObject { ["types"] = array }));
        }
    }

    sealed class SchemaResource(DocumentDbMcpPrimitives primitives) : ResourceFunction(primitives, takesName: true)
    {
        public override string Name => "documentdb-type-schema";
        public override string Description => "JSON Schema for one exposed type or collection.";

        protected override ValueTask<object?> InvokeCoreAsync(AIFunctionArguments arguments, CancellationToken cancellationToken)
        {
            var slug = Slug(arguments);
            var descriptor = this.Primitives.Descriptor(slug)
                ?? throw new InvalidOperationException($"'{slug}' is not exposed by this server.");

            return ValueTask.FromResult<object?>(Json(new JsonObject
            {
                ["name"] = descriptor.Slug,
                ["kind"] = descriptor.Kind,
                ["schema"] = JsonNode.Parse(descriptor.DocumentSchema.GetRawText())
            }));
        }
    }

    sealed class SampleResource(DocumentDbMcpPrimitives primitives, int sampleSize)
        : ResourceFunction(primitives, takesName: true)
    {
        public override string Name => "documentdb-type-sample";
        public override string Description => "A few real documents of one exposed type.";

        protected override async ValueTask<object?> InvokeCoreAsync(AIFunctionArguments arguments, CancellationToken cancellationToken)
        {
            var slug = Slug(arguments);
            var descriptor = this.Primitives.Descriptor(slug)
                ?? throw new InvalidOperationException($"'{slug}' is not exposed by this server.");

            var query = this.Primitives.Function($"{slug}_query")
                ?? throw new InvalidOperationException(
                    $"'{slug}' is exposed without the Query capability, so it has no documents to sample.");

            // Straight through the query tool: same scope, same caps, same hidden properties. The resource
            // carries the call's Services forward so a request-resolved scope resolves here too.
            var result = await query.InvokeAsync(new AIFunctionArguments(new Dictionary<string, object?>
            {
                ["limit"] = Math.Min(sampleSize, descriptor.MaxPageSize)
            })
            {
                Services = arguments.Services
            }, cancellationToken).ConfigureAwait(false);

            return Json(new JsonObject
            {
                ["name"] = descriptor.Slug,
                ["sample"] = JsonNode.Parse(JsonSerializer.Serialize(result))
            });
        }
    }

    sealed class StatsResource(DocumentDbMcpPrimitives primitives) : ResourceFunction(primitives, takesName: false)
    {
        public override string Name => "documentdb-stats";
        public override string Description => "Provider, capability flags and per-type document counts.";

        protected override async ValueTask<object?> InvokeCoreAsync(AIFunctionArguments arguments, CancellationToken cancellationToken)
        {
            var store = this.Primitives.Store;
            var counts = new JsonObject();

            foreach (var descriptor in this.Primitives.Descriptors)
            {
                var count = this.Primitives.Function($"{descriptor.Slug}_count");
                if (count != null)
                {
                    // Counted through the tool, so a scoped registration reports the caller's slice — not a
                    // total that quietly tells the model how much it is not being shown.
                    var result = await count.InvokeAsync(
                        new AIFunctionArguments { Services = arguments.Services }, cancellationToken).ConfigureAwait(false);
                    counts[descriptor.Slug] = JsonNode.Parse(JsonSerializer.Serialize(result));
                }
            }

            return Json(new JsonObject
            {
                ["provider"] = store.GetType().Name,
                ["capabilities"] = new JsonObject
                {
                    ["transactions"] = store.SupportsTransactions,
                    ["spatial"] = store.SupportsSpatial,
                    ["vector"] = store.SupportsVector,
                    ["fullText"] = store.SupportsFullText
                },
                ["counts"] = counts
            });
        }
    }
}
