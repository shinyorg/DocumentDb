using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using Shiny.DocumentDb.Mcp.Internal;

namespace Shiny.DocumentDb.Mcp;

/// <summary>
/// Turns a registered <see cref="IDocumentStore"/> into an MCP server an agent can explore and query.
/// </summary>
public static class DocumentDbMcpServiceCollectionExtensions
{
    /// <summary>
    /// Registers an MCP server over the default <see cref="IDocumentStore"/>. Returns the SDK's
    /// <see cref="IMcpServerBuilder"/> so the transport is the caller's choice — chain
    /// <c>.WithHttpTransport()</c> for a shared server (then <c>app.MapDocumentDbMcp("/mcp")</c>) or
    /// <c>.WithStdioServerTransport()</c> for a local desktop client.
    /// </summary>
    /// <remarks>
    /// This <b>is</b> the <c>AddMcpServer()</c> call — do not call both. Types are read-only unless
    /// <see cref="IDocumentDbMcpBuilder.AllowWrites"/> is called <i>and</i> the type asks for a write
    /// capability; a type that is never registered is invisible.
    /// </remarks>
    public static IMcpServerBuilder AddDocumentDbMcpServer(
        this IServiceCollection services,
        Action<IDocumentDbMcpBuilder> configure)
        => Add(services, storeName: null, configure);

    /// <summary>
    /// Registers an MCP server over a named (keyed) <see cref="IDocumentStore"/>.
    /// </summary>
    public static IMcpServerBuilder AddDocumentDbMcpServer(
        this IServiceCollection services,
        string storeName,
        Action<IDocumentDbMcpBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        return Add(services, storeName, configure);
    }

    static IMcpServerBuilder Add(IServiceCollection services, string? storeName, Action<IDocumentDbMcpBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DocumentDbMcpBuilder();
        configure(builder);
        builder.Validate(services);

        services.TryAddSingleton(sp => new DocumentDbMcpPrimitives(
            storeName is null
                ? sp.GetRequiredService<IDocumentStore>()
                : sp.GetRequiredKeyedService<IDocumentStore>(storeName),
            builder,
            sp.GetService<ILoggerFactory>() ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance));

        // The primitives need a live store, which only exists once the container is built — so they are added
        // to the server options when those are configured, not at registration time.
        services.AddSingleton<IConfigureOptions<McpServerOptions>, DocumentDbMcpOptionsSetup>();

        return services.AddMcpServer();
    }
}
