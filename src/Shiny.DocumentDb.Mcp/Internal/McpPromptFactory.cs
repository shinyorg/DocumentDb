using System.Text;
using System.Text.Json;
using Microsoft.Extensions.AI;
using ModelContextProtocol.Server;

namespace Shiny.DocumentDb.Mcp.Internal;

/// <summary>
/// The two prompts. Both are pure text built from what the server already exposes — they read no data, so
/// they need no scope.
/// </summary>
static class McpPromptFactory
{
    public static IReadOnlyList<McpServerPrompt> Build(DocumentDbMcpPrimitives primitives) =>
    [
        McpServerPrompt.Create(
            new ExplainCollectionPrompt(primitives),
            new McpServerPromptCreateOptions
            {
                Name = "explain-collection",
                Title = "Explain a collection",
                Description = "Describe what lives in a type and how to filter it."
            }),

        McpServerPrompt.Create(
            new BuildFilterPrompt(primitives),
            new McpServerPromptCreateOptions
            {
                Name = "build-filter",
                Title = "Build a filter",
                Description = "Turn a natural-language ask into a filter for one exposed type."
            })
    ];

    abstract class PromptFunction : AIFunction
    {
        protected PromptFunction(DocumentDbMcpPrimitives primitives, string schema)
        {
            this.Primitives = primitives;
            this.JsonSchema = JsonDocument.Parse(schema).RootElement.Clone();
        }

        protected DocumentDbMcpPrimitives Primitives { get; }
        public override JsonElement JsonSchema { get; }

        protected static string? Arg(AIFunctionArguments arguments, string key)
            => arguments.TryGetValue(key, out var raw)
                ? raw as string ?? (raw is JsonElement je && je.ValueKind == JsonValueKind.String ? je.GetString() : null)
                : null;

        protected string DescribeType(string slug)
        {
            var descriptor = this.Primitives.Descriptor(slug)
                ?? throw new InvalidOperationException($"'{slug}' is not exposed by this server.");

            var sb = new StringBuilder();
            sb.Append("Type: ").AppendLine(descriptor.Slug);
            if (!string.IsNullOrWhiteSpace(descriptor.Description))
                sb.Append("Description: ").AppendLine(descriptor.Description);
            sb.Append("Fields: ").AppendLine(string.Join(", ", descriptor.Fields));
            sb.Append("Tools: ").AppendLine(string.Join(", ", this.Primitives.Functions
                .Where(f => f.Name.StartsWith(descriptor.Slug + "_", StringComparison.Ordinal))
                .Select(f => f.Name)));
            sb.Append("Max page size: ").Append(descriptor.MaxPageSize).AppendLine();
            if (descriptor.IsScoped)
                sb.AppendLine("Note: the server applies its own record-level scope. Some documents may not be visible; that is expected and cannot be overridden.");
            return sb.ToString();
        }
    }

    sealed class ExplainCollectionPrompt(DocumentDbMcpPrimitives primitives) : PromptFunction(
        primitives,
        """
        {"type":"object","properties":{"name":{"type":"string","description":"The exposed type or collection slug."}},"required":["name"]}
        """)
    {
        public override string Name => "explain-collection";
        public override string Description => "Describe what lives in a type and how to filter it.";

        protected override ValueTask<object?> InvokeCoreAsync(AIFunctionArguments arguments, CancellationToken cancellationToken)
        {
            var slug = Arg(arguments, "name") ?? throw new InvalidOperationException("'name' is required.");
            return ValueTask.FromResult<object?>(
                $"""
                 Describe the documents available under this type, using the tools listed below. Read
                 documentdb://types/{slug}/schema for the field types and documentdb://types/{slug}/sample for a
                 few real documents before answering, then summarise what the type holds and which fields are
                 worth filtering on.

                 {this.DescribeType(slug)}
                 """);
        }
    }

    sealed class BuildFilterPrompt(DocumentDbMcpPrimitives primitives) : PromptFunction(
        primitives,
        """
        {"type":"object","properties":{"name":{"type":"string","description":"The exposed type or collection slug."},"ask":{"type":"string","description":"What the user wants to find, in their own words."}},"required":["name","ask"]}
        """)
    {
        public override string Name => "build-filter";
        public override string Description => "Turn a natural-language ask into a filter for one exposed type.";

        protected override ValueTask<object?> InvokeCoreAsync(AIFunctionArguments arguments, CancellationToken cancellationToken)
        {
            var slug = Arg(arguments, "name") ?? throw new InvalidOperationException("'name' is required.");
            var ask = Arg(arguments, "ask") ?? "";

            // $$ so the JSON braces below stay literal and only {{...}} interpolates.
            return ValueTask.FromResult<object?>(
                $$"""
                  Turn this request into a call to the '{{slug}}_query' tool:

                      {{ask}}

                  The tool takes a structured 'filter' argument — a single condition
                  { "field": "...", "op": "eq|ne|gt|gte|lt|lte|contains|startsWith|endsWith|in", "value": ... }
                  or a boolean group { "and": [ ... ] } / { "or": [ ... ] } — plus optional 'orderBy',
                  'orderDirection', 'limit' and 'offset'. Only the fields listed below may be named; anything
                  else is refused. Prefer one precise filter over fetching a page and reasoning over it.

                  {{this.DescribeType(slug)}}
                  """);
        }
    }
}
