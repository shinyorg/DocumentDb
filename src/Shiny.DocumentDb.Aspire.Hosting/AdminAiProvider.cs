namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// A chat backend the admin tool's assistant can be pointed at from the AppHost.
/// </summary>
/// <remarks>
/// The names match ShinyDocDbMyAdmin's own <c>AiProviderKind</c> exactly - the value is written out as
/// its name and parsed back by name, the same host↔tool contract <see cref="DocumentProviderKind"/>
/// uses for the database side.
/// </remarks>
public enum AdminAiProvider
{
    /// <summary>api.openai.com, using an OpenAI API key.</summary>
    OpenAI,

    /// <summary>An Azure OpenAI resource. Needs an endpoint, and the "model" is a deployment name.</summary>
    AzureOpenAI,

    /// <summary>api.anthropic.com, using an Anthropic API key.</summary>
    Anthropic,

    /// <summary>
    /// Anything speaking the OpenAI wire format at a base URL you choose - OpenRouter, Groq, LM Studio,
    /// vLLM, Ollama. Needs an endpoint; a local runtime usually needs no key at all.
    /// </summary>
    OpenAiCompatible
}
