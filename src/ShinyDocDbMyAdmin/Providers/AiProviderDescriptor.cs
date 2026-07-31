namespace ShinyDocDbMyAdmin.Providers;

/// <summary>
/// Everything the UI needs about one chat backend without switching on <see cref="AiProviderKind"/>.
/// The mirror of <see cref="ProviderDescriptor"/>, for the same reason: the settings form renders
/// from this, so adding a provider is one catalog entry rather than a sweep through the markup.
/// </summary>
public sealed record AiProviderDescriptor
{
    public required AiProviderKind Kind { get; init; }
    public required string DisplayName { get; init; }

    /// <summary>Where a key is obtained, shown under the key field. Null for backends that need none.</summary>
    public string? KeyHelpUrl { get; init; }

    /// <summary>False only for a local endpoint that authenticates some other way, or not at all.</summary>
    public bool RequiresApiKey { get; init; } = true;

    /// <summary>
    /// True when the user must supply the endpoint - the resource address for Azure, the base URL
    /// for an OpenAI-compatible service. False when the SDK's own default is correct.
    /// </summary>
    public bool RequiresEndpoint { get; init; }

    /// <summary>Prefilled into the endpoint box as a shape to edit rather than a blank to guess at.</summary>
    public string? EndpointPlaceholder { get; init; }

    /// <summary>
    /// What the model box is called on this backend. Azure addresses a <i>deployment</i> you named,
    /// not a published model id, and calling it "Model" is how people put the model id in and get a
    /// 404 they cannot explain.
    /// </summary>
    public string ModelLabel { get; init; } = "Model";

    /// <summary>Offered as datalist suggestions. Free text either way - this is not a closed set.</summary>
    public IReadOnlyList<string> SuggestedModels { get; init; } = [];

    /// <summary>One line under the provider picker saying what picking it means.</summary>
    public required string Summary { get; init; }
}
