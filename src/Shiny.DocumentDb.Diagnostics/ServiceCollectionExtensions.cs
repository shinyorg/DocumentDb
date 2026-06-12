using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.Diagnostics;

namespace Shiny.DocumentDb;

public static class DiagnosticsServiceCollectionExtensions
{
    /// <summary>
    /// Wraps the registered <see cref="IDocumentStore"/> in an <see cref="InstrumentedDocumentStore"/>
    /// that emits OpenTelemetry-native metrics and trace spans (meter / source name
    /// <see cref="DocumentStoreMetrics.MeterName"/>). Call this <b>after</b> registering a store
    /// (e.g. <c>AddDocumentStore(...)</c> or a provider store). Subscribe from your OTel pipeline with
    /// <c>.AddMeter("Shiny.DocumentDb")</c> and <c>.AddSource("Shiny.DocumentDb")</c>.
    /// <para>
    /// Decorates the non-keyed singleton/scoped <see cref="IDocumentStore"/> registration and re-points
    /// <see cref="ITemporalDocumentStore"/> at the same decorated instance. Keyed registrations (the
    /// named <c>AddDocumentStore(name, ...)</c> overload) are not auto-decorated.
    /// </para>
    /// </summary>
    public static IServiceCollection AddDocumentStoreInstrumentation(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddMetrics();
        services.TryAddSingleton<DocumentStoreMetrics>();

        var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(IDocumentStore) && !d.IsKeyedService)
            ?? throw new InvalidOperationException(
                "No IDocumentStore is registered. Register a store (e.g. AddDocumentStore(...) or a provider " +
                "store) before calling AddDocumentStoreInstrumentation().");

        services.Remove(descriptor);

        // Rebuild a factory for the original (undecorated) store, preserving its registration shape.
        Func<IServiceProvider, IDocumentStore> innerFactory;
        if (descriptor.ImplementationFactory != null)
        {
            innerFactory = sp => (IDocumentStore)descriptor.ImplementationFactory(sp);
        }
        else if (descriptor.ImplementationInstance != null)
        {
            var instance = (IDocumentStore)descriptor.ImplementationInstance;
            innerFactory = _ => instance;
        }
        else
        {
            // Let the container activate the concrete type (no reflection on our side).
            var implType = descriptor.ImplementationType!;
            services.Add(new ServiceDescriptor(implType, implType, descriptor.Lifetime));
            innerFactory = sp => (IDocumentStore)sp.GetRequiredService(implType);
        }

        services.Add(new ServiceDescriptor(
            typeof(IDocumentStore),
            sp => new InstrumentedDocumentStore(innerFactory(sp), sp.GetRequiredService<DocumentStoreMetrics>()),
            descriptor.Lifetime));

        // So consumers that resolve the temporal capability from DI get the decorated instance too.
        services.Add(new ServiceDescriptor(
            typeof(ITemporalDocumentStore),
            sp => (ITemporalDocumentStore)sp.GetRequiredService<IDocumentStore>(),
            descriptor.Lifetime));

        return services;
    }
}
