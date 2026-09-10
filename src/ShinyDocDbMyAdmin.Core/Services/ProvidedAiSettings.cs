using ShinyDocDbMyAdmin.Models;
using ShinyDocDbMyAdmin.Providers;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Assistant configuration handed to the tool by whatever is hosting it - an Aspire AppHost, the
/// terminal tool's command line, or plain environment variables - rather than typed into the UI.
/// </summary>
/// <remarks>
/// Two shapes, and the narrower one wins:
/// <list type="bullet">
/// <item><c>ShinyDocDbMyAdmin:Ai:*</c> - the instance default, applying to every connection.</item>
/// <item><c>Shiny:DocumentDb:{name}:Ai:*</c> - an override for one host-provided store, layered over
/// the default key by key, so an AppHost can point one database at a different model without
/// restating the API key.</item>
/// </list>
/// <para>
/// Unlike <see cref="ProvidedConnections"/> the instance default deliberately covers <b>saved</b>
/// connections too. Configuring the assistant is an instance-level policy - it is what makes
/// <c>shinydocdb --ai-provider … --ai-model …</c> mean anything when the profile you are opening is
/// one you saved yourself. Once the host has configured it, the host owns it: nothing here can be
/// edited from either front end.
/// </para>
/// <para>
/// The API key stays plaintext in this type and is encrypted by <see cref="ProfileStore"/> on the way
/// out, mirroring <see cref="ProvidedConnection"/>. Anything else would need a second,
/// plaintext-carrying route into <see cref="AiClientFactory"/>.
/// </para>
/// </remarks>
public sealed class ProvidedAiSettings
{
    /// <summary>The instance-wide default section.</summary>
    public const string ConfigurationSection = "ShinyDocDbMyAdmin:Ai";

    /// <summary>The per-store subsection, under <c>Shiny:DocumentDb:{name}</c>.</summary>
    public const string PerStoreSubsection = "Ai";

    readonly ProvidedAiConfiguration? fallback;
    readonly Dictionary<string, ProvidedAiConfiguration> byProfileId = new(StringComparer.OrdinalIgnoreCase);

    public ProvidedAiSettings(IConfiguration configuration, ILogger<ProvidedAiSettings> logger)
    {
        // A host that removed the assistant outright has nothing to configure, and honouring the keys
        // anyway would hand an API key to a front end that refuses to render a chat. Demo mode forces
        // this key on, so this is also what keeps a demo instance from carrying a host's key.
        if (configuration.GetValue(AiAvailability.DisableKey, false))
        {
            this.Any = false;
            return;
        }

        this.fallback = Read(configuration.GetSection(ConfigurationSection), null, logger, ConfigurationSection);

        foreach (var store in configuration.GetSection(ProvidedConnections.ConfigurationSection).GetChildren())
        {
            var section = store.GetSection(PerStoreSubsection);
            var path = $"{ProvidedConnections.ConfigurationSection}:{store.Key}:{PerStoreSubsection}";
            var settings = section.Exists()
                ? Read(section, this.fallback, logger, path)
                : this.fallback;

            if (settings is not null)
                this.byProfileId[ProvidedConnections.IdPrefix + store.Key] = settings;
        }

        this.Any = this.fallback is not null || this.byProfileId.Count > 0;

        if (this.Any)
            logger.LogInformation(
                "The assistant is configured by the host environment{Scope}; it cannot be changed from the UI.",
                this.byProfileId.Count > 0 ? $" ({this.byProfileId.Count} store override(s))" : null);
    }

    /// <summary>True when the host configured the assistant at all.</summary>
    public bool Any { get; private init; } = true;

    /// <summary>True when this connection's assistant configuration comes from the host.</summary>
    public bool IsProvided(string profileId) => this.Resolve(profileId) is not null;

    /// <summary>
    /// This connection's host-supplied configuration, or null when the host supplied none. Returns a
    /// fresh instance per call - callers hand these to UI screens that mutate them.
    /// </summary>
    public ProvidedAiConfiguration? Find(string profileId)
    {
        var resolved = this.Resolve(profileId);
        return resolved is null ? null : resolved with { Settings = Clone(resolved.Settings, profileId) };
    }

    // A per-store override is only ever reachable by its provided-{name} id; everything else - saved
    // connections included - falls back to the instance default.
    ProvidedAiConfiguration? Resolve(string profileId)
        => this.byProfileId.GetValueOrDefault(profileId) ?? this.fallback;

    static ProvidedAiConfiguration? Read(
        IConfigurationSection section,
        ProvidedAiConfiguration? fallback,
        ILogger logger,
        string path)
    {
        var providerName = section["Provider"];
        var provider = fallback?.Settings.Provider;

        if (!string.IsNullOrWhiteSpace(providerName))
        {
            if (TryParseProvider(providerName, out var parsed))
            {
                provider = parsed;
            }
            else
            {
                logger.LogWarning(
                    "Ignoring host assistant configuration at '{Path}': '{Provider}' is not a chat backend this tool knows.",
                    path,
                    providerName);

                return fallback;
            }
        }

        var model = section["Model"] ?? fallback?.Settings.Model;
        var apiKey = section["ApiKey"] ?? fallback?.ApiKey;

        // Provider + model are the irreducible pair: without both there is nothing to build a client
        // from, and a half-filled section is far more likely a typo than an intention.
        if (provider is null || string.IsNullOrWhiteSpace(model))
        {
            if (providerName is not null || section["Model"] is not null || section["ApiKey"] is not null)
                logger.LogWarning(
                    "Ignoring host assistant configuration at '{Path}': it needs both Provider and Model.",
                    path);

            return fallback;
        }

        var settings = new AiConnectionSettings
        {
            Id = "provided-ai",
            Provider = provider.Value,
            Model = model,
            Endpoint = section["Endpoint"] ?? fallback?.Settings.Endpoint,
            // Configuring it at all is the decision to turn it on; Enabled=false is how you stage a
            // key without switching the assistant on yet.
            Enabled = section.GetValue("Enabled", fallback?.Settings.Enabled ?? true),
            AllowInsert = section.GetValue("AllowInsert", fallback?.Settings.AllowInsert ?? false),
            AllowUpdate = section.GetValue("AllowUpdate", fallback?.Settings.AllowUpdate ?? false),
            AllowDelete = section.GetValue("AllowDelete", fallback?.Settings.AllowDelete ?? false)
        };

        return new ProvidedAiConfiguration(settings, apiKey);
    }

    static AiConnectionSettings Clone(AiConnectionSettings source, string profileId) => new()
    {
        Id = source.Id,
        ProfileId = profileId,
        Provider = source.Provider,
        Model = source.Model,
        Endpoint = source.Endpoint,
        Enabled = source.Enabled,
        AllowInsert = source.AllowInsert,
        AllowUpdate = source.AllowUpdate,
        AllowDelete = source.AllowDelete
    };

    /// <summary>
    /// Maps a provider name off the wire onto an <see cref="AiProviderKind"/>. The aliases exist
    /// because nobody typing a command line writes "OpenAiCompatible".
    /// </summary>
    static bool TryParseProvider(string value, out AiProviderKind kind)
    {
        switch (value.Trim().Replace("-", "").Replace("_", ""))
        {
            case var s when s.Equals("azure", StringComparison.OrdinalIgnoreCase):
                kind = AiProviderKind.AzureOpenAI;
                return true;

            case var s when s.Equals("compatible", StringComparison.OrdinalIgnoreCase)
                            || s.Equals("openaicompatible", StringComparison.OrdinalIgnoreCase)
                            || s.Equals("ollama", StringComparison.OrdinalIgnoreCase):
                kind = AiProviderKind.OpenAiCompatible;
                return true;

            case var s:
                return Enum.TryParse(s, ignoreCase: true, out kind) && Enum.IsDefined(kind);
        }
    }
}

/// <summary>Host-supplied assistant configuration and its plaintext key, which never reaches the profile store.</summary>
public sealed record ProvidedAiConfiguration(AiConnectionSettings Settings, string? ApiKey);
