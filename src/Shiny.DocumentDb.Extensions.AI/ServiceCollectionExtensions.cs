using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Shiny.DocumentDb.Extensions.AI.Internal;

namespace Shiny.DocumentDb.Extensions.AI;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a set of <see cref="AITool"/> instances that wrap <see cref="IDocumentStore"/>
    /// for the document types you opt-in to. Types and operations not listed here are
    /// invisible to the LLM.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Builder callback used to opt-in document types and capabilities.</param>
    public static IServiceCollection AddDocumentStoreAITools(
        this IServiceCollection services,
        Action<IDocumentAIToolBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DocumentAIToolBuilder();
        configure(builder);

        if (builder.Registrations.Count == 0)
            throw new InvalidOperationException(
                "AddDocumentStoreAITools requires at least one AddType<T>() call. " +
                "An empty registration would expose no tools to the LLM.");

        services.AddSingleton(sp =>
        {
            var store = sp.GetRequiredService<IDocumentStore>();
            var tools = new List<AITool>();
            foreach (var registration in builder.Registrations.Values)
                tools.AddRange(registration.CreateTools(store));
            return new DocumentStoreAITools(tools);
        });

        return services;
    }

    /// <summary>
    /// Registers a set of <see cref="AITool"/> instances that wrap a named <see cref="IDocumentStore"/>
    /// for the document types you opt-in to.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="storeName">The keyed service name of the document store to target.</param>
    /// <param name="configure">Builder callback used to opt-in document types and capabilities.</param>
    public static IServiceCollection AddDocumentStoreAITools(
        this IServiceCollection services,
        string storeName,
        Action<IDocumentAIToolBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DocumentAIToolBuilder();
        configure(builder);

        if (builder.Registrations.Count == 0)
            throw new InvalidOperationException(
                "AddDocumentStoreAITools requires at least one AddType<T>() call. " +
                "An empty registration would expose no tools to the LLM.");

        services.AddSingleton(sp =>
        {
            var store = sp.GetRequiredKeyedService<IDocumentStore>(storeName);
            var tools = new List<AITool>();
            foreach (var registration in builder.Registrations.Values)
                tools.AddRange(registration.CreateTools(store));
            return new DocumentStoreAITools(tools);
        });

        return services;
    }
}
