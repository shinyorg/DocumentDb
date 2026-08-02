namespace ShinyDocDbMyAdmin.Providers;

/// <summary>
/// The only place that knows about specific chat backends - the counterpart to
/// <see cref="ProviderCatalog"/>. Model suggestions are a convenience, not a whitelist: the boxes
/// are free text so a model released after this build still works.
/// </summary>
public static class AiProviderCatalog
{
    static readonly Dictionary<AiProviderKind, AiProviderDescriptor> Descriptors = new()
    {
        [AiProviderKind.OpenAI] = new AiProviderDescriptor
        {
            Kind = AiProviderKind.OpenAI,
            DisplayName = "OpenAI",
            Summary = "api.openai.com, using your OpenAI API key.",
            KeyHelpUrl = "https://platform.openai.com/api-keys",
            SuggestedModels = ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini", "o4-mini"]
        },

        [AiProviderKind.AzureOpenAI] = new AiProviderDescriptor
        {
            Kind = AiProviderKind.AzureOpenAI,
            DisplayName = "Azure OpenAI",
            Summary = "Your own Azure OpenAI resource, so the traffic stays inside your tenant.",
            RequiresEndpoint = true,
            EndpointPlaceholder = "https://my-resource.openai.azure.com",
            // Azure addresses a deployment you named, not a published model id. Getting this wrong
            // is a 404 with nothing in it to suggest the name was the problem.
            ModelLabel = "Deployment name",
            SuggestedModels = []
        },

        [AiProviderKind.Anthropic] = new AiProviderDescriptor
        {
            Kind = AiProviderKind.Anthropic,
            DisplayName = "Anthropic",
            Summary = "api.anthropic.com, using your Anthropic API key.",
            KeyHelpUrl = "https://console.anthropic.com/settings/keys",
            SuggestedModels =
            [
                "claude-sonnet-4-5-20250929",
                "claude-opus-4-1-20250805",
                "claude-haiku-4-5-20251001"
            ]
        },

        [AiProviderKind.OpenAiCompatible] = new AiProviderDescriptor
        {
            Kind = AiProviderKind.OpenAiCompatible,
            DisplayName = "OpenAI-compatible (custom endpoint)",
            Summary =
                "Anything speaking the OpenAI wire format - OpenRouter, Groq, Gemini's compatibility " +
                "endpoint, LM Studio, vLLM, Ollama. Point it at a local model and nothing leaves this machine.",
            RequiresEndpoint = true,
            EndpointPlaceholder = "http://localhost:11434/v1",
            // A local runtime typically ignores the key entirely, so demanding one would block the
            // configuration that sends no data anywhere - the safest option on offer.
            RequiresApiKey = false,
            SuggestedModels = []
        }
    };

    public static AiProviderDescriptor Get(AiProviderKind kind) => Descriptors[kind];

    public static IReadOnlyList<AiProviderDescriptor> All { get; } =
        [.. Descriptors.Values.OrderBy(x => x.DisplayName, StringComparer.OrdinalIgnoreCase)];
}
