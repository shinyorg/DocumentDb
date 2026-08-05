using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Shiny.DocumentDb.Extensions.AI.Internal;

namespace Shiny.DocumentDb.Mcp.Internal;

/// <summary>
/// Everything this server publishes, built once from a live store: the tool set (the Extensions.AI functions,
/// wrapped for audit), the orientation resources, and the prompts.
/// </summary>
sealed class DocumentDbMcpPrimitives
{
    public DocumentDbMcpPrimitives(IDocumentStore store, DocumentDbMcpBuilder builder, ILoggerFactory loggerFactory)
    {
        this.Store = store;
        this.Descriptors = builder.Registrations.Values.Select(r => r.Describe()).ToArray();

        var functions = new List<AIFunction>();
        foreach (var registration in builder.Registrations.Values)
            functions.AddRange(registration.CreateTools(store).OfType<AIFunction>());

        this.Functions = functions;

        var audit = loggerFactory.CreateLogger("Shiny.DocumentDb.Mcp.Tools");
        this.Tools = functions
            .Select(fn => (McpServerTool)new AuditedMcpServerTool(McpServerTool.Create(fn), audit))
            .ToArray();

        this.Resources = builder.ResourcesExposed
            ? McpResourceFactory.Build(this, builder.SampleSize)
            : Array.Empty<McpServerResource>();

        this.Prompts = builder.PromptsExposed
            ? McpPromptFactory.Build(this)
            : Array.Empty<McpServerPrompt>();
    }

    public IDocumentStore Store { get; }
    public IReadOnlyList<DocumentAIDescriptor> Descriptors { get; }
    public IReadOnlyList<AIFunction> Functions { get; }
    public IReadOnlyList<McpServerTool> Tools { get; }
    public IReadOnlyList<McpServerResource> Resources { get; }
    public IReadOnlyList<McpServerPrompt> Prompts { get; }

    public DocumentAIDescriptor? Descriptor(string slug)
        => this.Descriptors.FirstOrDefault(d => string.Equals(d.Slug, slug, StringComparison.OrdinalIgnoreCase));

    /// <summary>The generated function with this exact name, or null when the capability was not granted.</summary>
    public AIFunction? Function(string name)
        => this.Functions.FirstOrDefault(f => string.Equals(f.Name, name, StringComparison.Ordinal));
}
