using Microsoft.Extensions.AI;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// What a request-resolved scope filter gets when a tool is invoked: the call's service provider, the
/// arguments the model supplied, and the call's cancellation token.
/// </summary>
/// <remarks>
/// <para>
/// The provider comes from <see cref="AIFunctionArguments.Services"/>, which the MCP server sets from the
/// per-request scope and which an <c>IChatClient</c> caller sets themselves
/// (<c>new AIFunctionArguments { Services = scope.ServiceProvider }</c>). Where it is a real request scope,
/// <c>AddScoped</c> services resolve to that request's instances — which is the whole point: "which rows may
/// this caller see" almost never lives in a value fixed at registration.
/// </para>
/// <para>
/// <see cref="Arguments"/> is for correlation and auditing only. Never read a scope value out of it — those
/// are the model's words, and a scope the model can influence is not a scope.
/// </para>
/// </remarks>
public readonly struct DocumentAIFilterContext
{
    internal DocumentAIFilterContext(IServiceProvider services, AIFunctionArguments arguments, CancellationToken cancellationToken)
    {
        this.Services = services;
        this.Arguments = arguments;
        this.CancellationToken = cancellationToken;
    }

    /// <summary>The service provider for this tool call — a request scope on the HTTP transport.</summary>
    public IServiceProvider Services { get; }

    /// <summary>The arguments the model supplied. For correlation/audit only — never a source of scope values.</summary>
    public AIFunctionArguments Arguments { get; }

    /// <summary>Cancels the tool call.</summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>Resolves a required service from the call's scope. A miss fails the tool call (fail closed).</summary>
    public TService GetRequiredService<TService>() where TService : notnull
        => (TService)(this.Services.GetService(typeof(TService))
            ?? throw new InvalidOperationException(
                $"No service of type '{typeof(TService)}' is registered. A request-resolved AI tool filter " +
                "cannot be evaluated without it, and the call is refused rather than run unscoped."));

    /// <summary>Resolves an optional service from the call's scope.</summary>
    public TService? GetService<TService>()
        => (TService?)this.Services.GetService(typeof(TService));
}
