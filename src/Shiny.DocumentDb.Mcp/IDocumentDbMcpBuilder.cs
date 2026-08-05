using Shiny.DocumentDb.Extensions.AI;

namespace Shiny.DocumentDb.Mcp;

/// <summary>
/// Configures what an MCP client may reach in the store. It <b>is</b> the
/// <see cref="IDocumentAIToolBuilder"/> — the tools an MCP client calls are the same
/// <c>Shiny.DocumentDb.Extensions.AI</c> tools an <c>IChatClient</c> calls, with the same scope rules and the
/// same tests — plus the few knobs that only make sense when the caller is an arbitrary agent.
/// </summary>
public interface IDocumentDbMcpBuilder : IDocumentAIToolBuilder
{
    /// <summary>
    /// Unlocks the write capabilities (<c>Insert</c>/<c>Update</c>/<c>Delete</c>) for this server. Writes need
    /// <b>two</b> locks: the per-type capability flag <i>and</i> this. A registration that asks for a write
    /// capability without it is a startup error, not a silently-downgraded server.
    /// </summary>
    IDocumentDbMcpBuilder AllowWrites();

    /// <summary>
    /// Publishes the orientation resources — <c>documentdb://types</c>, <c>documentdb://types/{name}/schema</c>,
    /// <c>documentdb://types/{name}/sample</c> and <c>documentdb://stats</c> — so a model can learn the shape of
    /// the data in one round trip instead of guessing tool arguments.
    /// </summary>
    /// <param name="sampleSize">How many documents <c>.../sample</c> returns. Clamped to the type's page cap.</param>
    /// <remarks>
    /// The sample resource runs through the type's own <b>query tool</b>, so the non-removable scope, the page
    /// cap and the hidden/encrypted properties all apply exactly as they do to a tool call.
    /// </remarks>
    IDocumentDbMcpBuilder ExposeResources(int sampleSize = 3);

    /// <summary>
    /// Publishes the two prompts: <c>explain-collection</c> (describe what lives in a type and how to filter it)
    /// and <c>build-filter</c> (turn a natural-language ask into a filter for a type).
    /// </summary>
    IDocumentDbMcpBuilder ExposePrompts();
}
