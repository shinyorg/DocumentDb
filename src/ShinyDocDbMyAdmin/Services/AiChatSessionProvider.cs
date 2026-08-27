using Microsoft.Extensions.AI;
using Shiny.Blazor.Controls.Chat;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Hands <c>ChatView</c> a conversation for a connection. Registered per circuit, so one browser
/// tab's transcript is not another's.
/// </summary>
/// <remarks>
/// Session ids carry the scope: <c>{profileId}</c> for a connection-wide chat, or
/// <c>{profileId}|{table}|{type}</c> for one opened from a document type. The scope only shapes the
/// opening system prompt - the tool surface reaches every connection either way, which is what the
/// warning in the UI says.
/// </remarks>
public sealed class AiChatSessionProvider(
    ProfileStore profiles,
    AiClientFactory clients,
    AiToolSurface toolSurface,
    AiAvailability availability,
    ILogger<AiChatSessionProvider> logger
) : IChatSessionProvider
{
    readonly Dictionary<string, IChatSession> sessions = new(StringComparer.Ordinal);

    /// <summary>Builds the id <c>ChatView</c> should be given for a scope.</summary>
    public static string SessionIdFor(string profileId, string? table = null, string? typeName = null)
        => string.IsNullOrWhiteSpace(table) || string.IsNullOrWhiteSpace(typeName)
            ? profileId
            : $"{profileId}|{table}|{typeName}";

    public Task<IChatSession> CreateSessionAsync(string[] userIds, CancellationToken cancellationToken = default)
        => throw new ChatSessionException("Assistant conversations are opened against a connection, not created.");

    public async Task<IChatSession> GetSessionAsync(string sessionId, CancellationToken cancellationToken = default)
    {
        // Belt and braces with the route guard: if the feature is off, no conversation exists even
        // for a caller that got this far.
        if (!availability.IsEnabled)
            throw new ChatSessionException("The assistant is disabled on this deployment.");

        if (this.sessions.TryGetValue(sessionId, out var existing))
            return existing;

        var parts = sessionId.Split('|');
        var profileId = parts[0];
        var table = parts.Length > 1 ? parts[1] : null;
        var typeName = parts.Length > 2 ? parts[2] : null;

        var profile = await profiles.Get(profileId, cancellationToken)
            ?? throw new ChatSessionException($"Connection '{profileId}' was not found.");

        var settings = await profiles.GetAiSettings(profileId, cancellationToken);
        if (settings is null || !settings.IsUsable())
            throw new ChatSessionException($"No assistant is configured for '{profile.Name}'.");

        IChatClient client;
        try
        {
            client = clients.Create(settings);
        }
        catch (InvalidOperationException ex)
        {
            // The factory's messages name the missing field; passing them through is more use than
            // "could not start a session".
            throw new ChatSessionException(ex.Message, ex);
        }

        var session = new AiChatSession(
            sessionId,
            typeName is null ? profile.Name : $"{profile.Name} · {typeName}",
            client,
            toolSurface.Build(settings),
            AiPrompt.Build(AiPrompt.Surface.Web, profile.Name, table, typeName, settings),
            logger);

        this.sessions[sessionId] = session;
        return session;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var session in this.sessions.Values)
            await session.DisposeAsync();

        this.sessions.Clear();
    }
}
