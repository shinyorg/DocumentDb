using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace Shiny.DocumentDb.Mcp;

/// <summary>Maps the MCP Streamable-HTTP endpoint for the server registered by <c>AddDocumentDbMcpServer</c>.</summary>
public static class DocumentDbMcpEndpointExtensions
{
    /// <summary>
    /// Maps the MCP endpoint at <paramref name="pattern"/>. Requires <c>.WithHttpTransport()</c> on the builder
    /// <c>AddDocumentDbMcpServer</c> returned. Compose authorization the normal way —
    /// <c>app.MapDocumentDbMcp("/mcp").RequireAuthorization("mcp")</c> — and remember that on this transport the
    /// caller's identity is what a request-resolved scope filter reads.
    /// </summary>
    public static IEndpointConventionBuilder MapDocumentDbMcp(this IEndpointRouteBuilder endpoints, string pattern = "/mcp")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return endpoints.MapMcp(pattern);
    }
}
