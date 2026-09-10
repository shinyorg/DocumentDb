using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Shiny.DocumentDb.Aspire.Hosting.Internal;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// Configures the admin tool's AI assistant from the AppHost, so a developer opening the tool gets a
/// working assistant without pasting their own API key into a database browser.
/// </summary>
/// <remarks>
/// <para>
/// Both front ends - the container (<see cref="DocumentDbAdminResourceBuilderExtensions.AddDocumentDbAdmin"/>)
/// and the terminal tool (<see cref="DocumentDbAdminTerminalResourceBuilderExtensions.AddDocumentDbAdminTerminal"/>)
/// - are the same Core over the same <c>IConfiguration</c>, so one set of extensions covers both.
/// </para>
/// <para>
/// <b>Configuring it here makes it read-only there.</b> The tool treats a host-supplied assistant the
/// same way it treats a host-supplied connection: the settings screen shows the values and refuses to
/// change them. The AppHost stays the single source of truth, and nobody can quietly repoint a shared
/// admin instance at a different key.
/// </para>
/// </remarks>
public static class AdminAiResourceBuilderExtensions
{
    /// <summary>
    /// Points the assistant at a chat backend for every connection in the tool.
    /// </summary>
    /// <param name="builder">The admin resource - container or terminal.</param>
    /// <param name="provider">The chat backend.</param>
    /// <param name="model">Model id. On <see cref="AdminAiProvider.AzureOpenAI"/> this is the deployment name.</param>
    /// <param name="apiKey">
    /// The key, as a parameter so it lives in user secrets or the deployment's configuration rather
    /// than in AppHost source.
    /// </param>
    /// <param name="endpoint">Base URL. Required for Azure and OpenAI-compatible backends.</param>
    /// <param name="enabled">
    /// False stages the configuration without switching the assistant on - useful when the key is in
    /// place but you are not ready for the tool to start talking to a provider.
    /// </param>
    /// <example>
    /// <code>
    /// builder.AddDocumentDbAdmin()
    ///        .WithReference(store)
    ///        .WithAi(AdminAiProvider.Anthropic, "claude-sonnet-4-5-20250929",
    ///                builder.AddParameter("anthropic-key", secret: true));
    /// </code>
    /// </example>
    public static IResourceBuilder<T> WithAi<T>(
        this IResourceBuilder<T> builder,
        AdminAiProvider provider,
        string model,
        IResourceBuilder<ParameterResource> apiKey,
        string? endpoint = null,
        bool enabled = true)
        where T : IResourceWithEnvironment
    {
        ArgumentNullException.ThrowIfNull(apiKey);
        return builder.ApplyAi(null, provider, model, apiKey, apiKeyLiteral: null, endpoint, enabled);
    }

    /// <summary>
    /// The same, with the key as a literal - or with no key at all, which is the normal shape for a
    /// local OpenAI-compatible runtime. Prefer the parameter overload for a key that matters.
    /// </summary>
    /// <remarks>
    /// The key is optional here and required on the parameter overload on purpose: it means nobody
    /// ever has to write <c>apiKey: null</c>, which would be ambiguous between the two.
    /// </remarks>
    public static IResourceBuilder<T> WithAi<T>(
        this IResourceBuilder<T> builder,
        AdminAiProvider provider,
        string model,
        string? apiKey = null,
        string? endpoint = null,
        bool enabled = true)
        where T : IResourceWithEnvironment
        => builder.ApplyAi(null, provider, model, apiKeyParameter: null, apiKey, endpoint, enabled);

    /// <summary>
    /// Overrides the assistant for one store, layered over whatever <see cref="WithAi{T}(IResourceBuilder{T}, AdminAiProvider, string, IResourceBuilder{ParameterResource}?, string?, bool)"/>
    /// set. Only the values you pass are overridden, so pointing one database at a bigger model does
    /// not mean restating the API key.
    /// </summary>
    /// <param name="store">The store whose assistant this configures.</param>
    public static IResourceBuilder<T> WithAiFor<T>(
        this IResourceBuilder<T> builder,
        IResourceBuilder<DocumentStoreResource> store,
        AdminAiProvider provider,
        string model,
        IResourceBuilder<ParameterResource>? apiKey = null,
        string? endpoint = null,
        bool enabled = true)
        where T : IResourceWithEnvironment
    {
        ArgumentNullException.ThrowIfNull(store);
        // No literal-key twin here: an override exists to change the model, and restating the key is
        // exactly what layering over the instance default saves you from.
        return builder.ApplyAi(store.Resource.Name, provider, model, apiKey, apiKeyLiteral: null, endpoint, enabled);
    }

    /// <summary>
    /// Lets the assistant write. Off unless you call this: the read surface is the safe default, and a
    /// write tool the AppHost granted silently is exactly the thing nobody wants to discover later.
    /// The tool still refuses every write on a connection marked read-only, whatever is granted here.
    /// </summary>
    /// <param name="store">
    /// Null grants on every connection; a store grants only on that one - the same instance-default /
    /// per-store split the rest of this surface uses.
    /// </param>
    public static IResourceBuilder<T> WithAiWrites<T>(
        this IResourceBuilder<T> builder,
        bool insert = false,
        bool update = false,
        bool delete = false,
        IResourceBuilder<DocumentStoreResource>? store = null)
        where T : IResourceWithEnvironment
    {
        ArgumentNullException.ThrowIfNull(builder);

        var name = store?.Resource.Name;
        return builder
            .WithEnvironment(AdminConstants.AiEnvVar(name, "AllowInsert"), insert ? "true" : "false")
            .WithEnvironment(AdminConstants.AiEnvVar(name, "AllowUpdate"), update ? "true" : "false")
            .WithEnvironment(AdminConstants.AiEnvVar(name, "AllowDelete"), delete ? "true" : "false");
    }

    static IResourceBuilder<T> ApplyAi<T>(
        this IResourceBuilder<T> builder,
        string? storeName,
        AdminAiProvider provider,
        string model,
        IResourceBuilder<ParameterResource>? apiKeyParameter,
        string? apiKeyLiteral,
        string? endpoint,
        bool enabled)
        where T : IResourceWithEnvironment
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(model);

        // Caught here rather than at the tool, which can only log a warning and carry on with no
        // assistant - by which point the AppHost that made the mistake is long past.
        if (endpoint is null && provider is AdminAiProvider.AzureOpenAI or AdminAiProvider.OpenAiCompatible)
            throw new ArgumentException(
                $"{provider} needs an endpoint - pass endpoint: to WithAi.",
                nameof(endpoint));

        builder = builder
            .WithEnvironment(AdminConstants.AiEnvVar(storeName, "Provider"), provider.ToString())
            .WithEnvironment(AdminConstants.AiEnvVar(storeName, "Model"), model)
            .WithEnvironment(AdminConstants.AiEnvVar(storeName, "Enabled"), enabled ? "true" : "false");

        if (endpoint is not null)
            builder = builder.WithEnvironment(AdminConstants.AiEnvVar(storeName, "Endpoint"), endpoint);

        if (apiKeyParameter is not null)
            builder = builder.WithEnvironment(AdminConstants.AiEnvVar(storeName, "ApiKey"), apiKeyParameter);
        else if (!string.IsNullOrWhiteSpace(apiKeyLiteral))
            builder = builder.WithEnvironment(AdminConstants.AiEnvVar(storeName, "ApiKey"), apiKeyLiteral);

        return builder;
    }
}
