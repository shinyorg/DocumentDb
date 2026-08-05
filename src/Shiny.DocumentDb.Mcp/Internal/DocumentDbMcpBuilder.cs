using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Extensions.AI;
using Shiny.DocumentDb.Extensions.AI.Internal;

namespace Shiny.DocumentDb.Mcp.Internal;

/// <summary>
/// The MCP builder: a thin shell over the AI tool builder that adds the MCP-only knobs. Registration is
/// delegated verbatim so a fix to the tool surface reaches both lanes.
/// </summary>
sealed class DocumentDbMcpBuilder : IDocumentDbMcpBuilder
{
    readonly DocumentAIToolBuilder inner = new();

    public bool WritesAllowed { get; private set; }
    public bool ResourcesExposed { get; private set; }
    public bool PromptsExposed { get; private set; }
    public int SampleSize { get; private set; } = 3;

    public IReadOnlyDictionary<string, DocumentAIRegistration> Registrations => this.inner.Registrations;

    public IDocumentDbMcpBuilder AllowWrites()
    {
        this.WritesAllowed = true;
        return this;
    }

    public IDocumentDbMcpBuilder ExposeResources(int sampleSize = 3)
    {
        if (sampleSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(sampleSize));

        this.ResourcesExposed = true;
        this.SampleSize = sampleSize;
        return this;
    }

    public IDocumentDbMcpBuilder ExposePrompts()
    {
        this.PromptsExposed = true;
        return this;
    }

    public IDocumentAIToolBuilder AddType<T>(
        JsonTypeInfo<T> jsonTypeInfo,
        string? name = null,
        DocumentAICapabilities capabilities = DocumentAICapabilities.ReadOnly,
        Action<IDocumentAITypeBuilder<T>>? configure = null) where T : class
        => this.inner.AddType(jsonTypeInfo, name, capabilities, configure);

    public IDocumentAIToolBuilder AddCollection(
        string collectionName,
        string? name = null,
        DocumentAICapabilities capabilities = DocumentAICapabilities.ReadOnly,
        Action<IDocumentAICollectionBuilder>? configure = null)
        => this.inner.AddCollection(collectionName, name, capabilities, configure);

    /// <summary>
    /// Startup validation. Two things are checked here rather than at 3am: writes need the second lock, and a
    /// request-resolved filter's service must actually be registered.
    /// </summary>
    public void Validate(IServiceCollection services)
    {
        if (this.Registrations.Count == 0)
            throw new InvalidOperationException(
                "AddDocumentDbMcpServer requires at least one AddType<T>() or AddCollection() call. " +
                "An MCP server with no registrations exposes nothing.");

        if (!this.WritesAllowed)
        {
            foreach (var registration in this.Registrations.Values)
            {
                var writes = registration.Capabilities & DocumentAICapabilities.Write;
                if (writes != DocumentAICapabilities.None)
                    throw new InvalidOperationException(
                        $"'{registration.Slug}' asks for {writes}, but this MCP server has not called AllowWrites(). " +
                        "Writes need both locks — the per-type capability and the server-wide switch — so an " +
                        "agent cannot be handed a destructive tool by a one-word config change.");
            }
        }

        foreach (var registration in this.Registrations.Values)
        {
            foreach (var serviceType in registration.RequiredServiceTypes)
            {
                if (!services.Any(d => d.ServiceType == serviceType))
                    throw new InvalidOperationException(
                        $"'{registration.Slug}' registers a request-resolved filter needing '{serviceType}', " +
                        "but no such service is registered. Register it before AddDocumentDbMcpServer — a filter " +
                        "that cannot resolve its service refuses every tool call.");
            }
        }
    }
}
