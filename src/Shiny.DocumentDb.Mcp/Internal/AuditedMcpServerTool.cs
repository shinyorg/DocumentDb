using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Shiny.DocumentDb.Mcp.Internal;

/// <summary>
/// Wraps a generated tool with the audit line an MCP server needs to be deployable: which tool, by whom,
/// with what arguments (digest only), how it ended, how long it took.
/// </summary>
/// <remarks>
/// Arguments are logged as a SHA-256 digest, never as text: a filter can carry customer data, and an audit log
/// that quietly becomes a second copy of the database is its own incident. The digest still lets you prove two
/// calls were identical, or correlate a complaint with a call.
/// </remarks>
sealed class AuditedMcpServerTool : DelegatingMcpServerTool
{
    readonly ILogger logger;

    public AuditedMcpServerTool(McpServerTool inner, ILogger logger) : base(inner)
        => this.logger = logger;

    public override async ValueTask<CallToolResult> InvokeAsync(
        RequestContext<CallToolRequestParams> request,
        CancellationToken cancellationToken = default)
    {
        var name = this.ProtocolTool.Name;
        var user = request.User?.Identity?.Name;
        var digest = Digest(request.Params?.Arguments);
        var started = Stopwatch.GetTimestamp();

        try
        {
            var result = await base.InvokeAsync(request, cancellationToken).ConfigureAwait(false);
            this.logger.LogInformation(
                "MCP tool {Tool} by {User} args={ArgumentDigest} outcome={Outcome} in {ElapsedMs}ms",
                name, user ?? "(anonymous)", digest,
                result.IsError == true ? "error" : "success",
                Stopwatch.GetElapsedTime(started).TotalMilliseconds);

            return result;
        }
        catch (Exception ex)
        {
            // A refused scope lands here (the filters fail closed by throwing). It is the line you want when
            // someone asks "did the agent see that record" — so it is logged before the exception continues.
            this.logger.LogWarning(ex,
                "MCP tool {Tool} by {User} args={ArgumentDigest} outcome=failed in {ElapsedMs}ms",
                name, user ?? "(anonymous)", digest, Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            throw;
        }
    }

    static string Digest(IDictionary<string, JsonElement>? arguments)
    {
        if (arguments is null || arguments.Count == 0)
            return "none";

        var sb = new StringBuilder();
        foreach (var pair in arguments.OrderBy(p => p.Key, StringComparer.Ordinal))
            sb.Append(pair.Key).Append('=').Append(pair.Value.GetRawText()).Append(';');

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()))).ToLowerInvariant()[..16];
    }
}
