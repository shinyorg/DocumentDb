namespace ShinyDocDbMyAdmin.Providers;

/// <summary>
/// The chat backends the assistant can talk to. Each one resolves to a
/// <c>Microsoft.Extensions.AI.IChatClient</c>, so the rest of the tool never switches on this - the
/// same way <see cref="ProviderKind"/> is only ever consulted by <see cref="ProviderCatalog"/>.
/// </summary>
public enum AiProviderKind
{
    OpenAI,
    AzureOpenAI,
    Anthropic,

    /// <summary>
    /// Any service that speaks the OpenAI wire format at a base URL of your choosing - OpenRouter,
    /// Groq, Gemini's compatibility endpoint, LM Studio, vLLM, llama.cpp, Ollama. It exists so an
    /// unlisted provider is never a blocker, and so a local model remains reachable for anyone who
    /// would rather no data left the host at all.
    /// </summary>
    OpenAiCompatible
}
