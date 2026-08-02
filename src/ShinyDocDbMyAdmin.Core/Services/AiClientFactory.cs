using System.ClientModel;
using Microsoft.Extensions.AI;
using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Turns a connection's saved <see cref="AiConnectionSettings"/> into an <see cref="IChatClient"/>.
/// </summary>
/// <remarks>
/// The one place that knows how each backend is constructed, so everything downstream works against
/// <c>IChatClient</c> alone. Clients are built per conversation and disposed with it, matching how
/// <c>AdminConnection</c> and the filter console open and close around their work rather than
/// holding a handle for the life of the process.
/// </remarks>
public sealed class AiClientFactory(ProfileStore profiles, ILogger<AiClientFactory> logger)
{
    /// <summary>
    /// Builds a ready-to-use client with the function-invocation middleware attached, so the tool
    /// calls in <see cref="AiToolSurface"/> are executed rather than handed back as raw requests.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The settings are incomplete. The message names the missing field: a null-ref at first send
    /// tells the user nothing about which box they left empty.
    /// </exception>
    public IChatClient Create(AiConnectionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var descriptor = AiProviderCatalog.Get(settings.Provider);
        var apiKey = profiles.RevealApiKey(settings);

        if (descriptor.RequiresApiKey && string.IsNullOrWhiteSpace(apiKey))
            throw new InvalidOperationException($"{descriptor.DisplayName} needs an API key; none is saved for this connection.");

        if (string.IsNullOrWhiteSpace(settings.Model))
            throw new InvalidOperationException($"No {descriptor.ModelLabel.ToLowerInvariant()} is set for this connection.");

        if (descriptor.RequiresEndpoint && string.IsNullOrWhiteSpace(settings.Endpoint))
            throw new InvalidOperationException($"{descriptor.DisplayName} needs an endpoint; none is saved for this connection.");

        var inner = this.CreateInner(settings, descriptor, apiKey);

        logger.LogInformation(
            "Built a {Provider} chat client for connection {ProfileId}.",
            descriptor.DisplayName,
            settings.ProfileId);

        return inner
            .AsBuilder()
            .UseFunctionInvocation()
            .Build();
    }

    IChatClient CreateInner(AiConnectionSettings settings, AiProviderDescriptor descriptor, string? apiKey)
        => settings.Provider switch
        {
            AiProviderKind.OpenAI =>
                new OpenAI.OpenAIClient(new ApiKeyCredential(apiKey!))
                    .GetChatClient(settings.Model)
                    .AsIChatClient(),

            AiProviderKind.AzureOpenAI =>
                new Azure.AI.OpenAI.AzureOpenAIClient(new Uri(settings.Endpoint!), new ApiKeyCredential(apiKey!))
                    // On Azure this is a deployment name, not a model id - see AiProviderCatalog.
                    .GetChatClient(settings.Model)
                    .AsIChatClient(),

            AiProviderKind.Anthropic =>
                // Anthropic's own official library, which ships an AsIChatClient adapter of its own -
                // the same shape as the OpenAI path above. Not the similarly named community
                // Anthropic.SDK package.
                new Anthropic.AnthropicClient(new Anthropic.Core.ClientOptions { ApiKey = apiKey! })
                    .AsIChatClient(settings.Model),

            AiProviderKind.OpenAiCompatible =>
                new OpenAI.OpenAIClient(
                        // A local runtime usually ignores the key but the client still wants a
                        // non-empty credential, so a placeholder keeps "no key" a working setup.
                        new ApiKeyCredential(string.IsNullOrWhiteSpace(apiKey) ? "not-required" : apiKey),
                        new OpenAI.OpenAIClientOptions { Endpoint = new Uri(settings.Endpoint!) })
                    .GetChatClient(settings.Model)
                    .AsIChatClient(),

            _ => throw new InvalidOperationException($"'{settings.Provider}' is not a chat backend this tool can build.")
        };
}
