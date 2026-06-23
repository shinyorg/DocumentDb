using Microsoft.Extensions.AI;
using Shiny.DocumentDb.Extensions.AI.Internal;

namespace Shiny.DocumentDb.Extensions.AI;

/// <summary>
/// Build <see cref="DocumentStoreAITools"/> directly from a live <see cref="IDocumentStore"/> — no DI
/// container required. Use this when you construct the store yourself (<c>new DocumentStore(options)</c>,
/// MAUI, scripts, tests); the DI registration (<c>AddDocumentStoreAITools</c>) builds the same result from
/// a container-resolved store.
/// </summary>
public static class DocumentStoreAIToolsExtensions
{
    /// <summary>
    /// Wraps the opted-in document types as <see cref="AITool"/>s over <paramref name="store"/>. Pass the
    /// result's <see cref="DocumentStoreAITools.Tools"/> to your <c>IChatClient</c> / <c>ChatOptions.Tools</c>.
    /// </summary>
    public static DocumentStoreAITools CreateAITools(this IDocumentStore store, Action<IDocumentAIToolBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(configure);
        return Build(store, BuildOrThrow(configure));
    }

    // Runs the builder callback and validates at least one type was registered. Shared with the DI path so
    // the empty-registration check stays eager (at AddDocumentStoreAITools call time, not first resolve).
    internal static DocumentAIToolBuilder BuildOrThrow(Action<IDocumentAIToolBuilder> configure)
    {
        var builder = new DocumentAIToolBuilder();
        configure(builder);

        if (builder.Registrations.Count == 0)
            throw new InvalidOperationException(
                "AddDocumentStoreAITools / CreateAITools requires at least one AddType<T>() call. " +
                "An empty registration would expose no tools to the LLM.");

        return builder;
    }

    internal static DocumentStoreAITools Build(IDocumentStore store, DocumentAIToolBuilder builder)
    {
        var tools = new List<AITool>();
        foreach (var registration in builder.Registrations.Values)
            tools.AddRange(registration.CreateTools(store));
        return new DocumentStoreAITools(tools);
    }
}
