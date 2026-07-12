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
    /// <see cref="ITemporalDocumentStore"/> at the same decorated instance. To instrument a keyed/named
    /// registration, use <see cref="AddDocumentStoreInstrumentation(IServiceCollection, string)"/>.
    /// </para>
    /// </summary>
    public static IServiceCollection AddDocumentStoreInstrumentation(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        RegisterMetrics(services);

        var descriptor = services.LastOrDefault(d => d.ServiceType == typeof(IDocumentStore) && !d.IsKeyedService)
            ?? throw new InvalidOperationException(
                "No IDocumentStore is registered. Register a store (e.g. AddDocumentStore(...) or a provider " +
                "store) before calling AddDocumentStoreInstrumentation().");

        services.Remove(descriptor);

        var innerFactory = BuildInnerFactory(services, descriptor);

        services.Add(new ServiceDescriptor(
            typeof(IDocumentStore),
            sp => Wrap(innerFactory(sp), sp, storeName: null),
            descriptor.Lifetime));

        // So consumers that resolve the temporal capability from DI get the decorated instance too.
        services.Add(new ServiceDescriptor(
            typeof(ITemporalDocumentStore),
            sp => (ITemporalDocumentStore)sp.GetRequiredService<IDocumentStore>(),
            descriptor.Lifetime));

        return services;
    }

    /// <summary>
    /// Wraps the keyed/named <see cref="IDocumentStore"/> registered under <paramref name="name"/> (the
    /// <c>AddDocumentStore(name, ...)</c> overload) in an <see cref="InstrumentedDocumentStore"/>. Every
    /// metric measurement and span from the decorated store carries a <c>db.namespace</c> tag equal to
    /// <paramref name="name"/> so signals from multiple stores can be told apart.
    /// <para>
    /// The decorated instance faithfully surfaces <see cref="ITemporalDocumentStore"/>,
    /// <see cref="IObservableDocumentStore"/>, and <see cref="IChangeFeedDocumentStore"/>, so a cast on the
    /// keyed <see cref="IDocumentStore"/> keeps working — there is no separate keyed capability descriptor to
    /// re-point. Idempotent: an already-instrumented store is returned unchanged.
    /// </para>
    /// </summary>
    public static IServiceCollection AddDocumentStoreInstrumentation(this IServiceCollection services, string name)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        RegisterMetrics(services);

        var descriptor = services.LastOrDefault(d =>
                d.ServiceType == typeof(IDocumentStore) && d.IsKeyedService && Equals(d.ServiceKey, name))
            ?? throw new InvalidOperationException(
                $"No keyed IDocumentStore is registered under key '{name}'. Register it (e.g. " +
                $"AddDocumentStore(\"{name}\", ...)) before calling AddDocumentStoreInstrumentation(\"{name}\").");

        services.Remove(descriptor);

        var innerFactory = BuildKeyedInnerFactory(services, descriptor);

        services.Add(new ServiceDescriptor(
            typeof(IDocumentStore),
            name,
            (sp, key) => Wrap(innerFactory(sp, key), sp, storeName: name),
            descriptor.Lifetime));

        return services;
    }

    static void RegisterMetrics(IServiceCollection services)
    {
        services.AddMetrics();
        services.TryAddSingleton<DocumentStoreMetrics>();
    }

    // Wrap once: if the store is already instrumented (flag + explicit call, or a double call), leave it be.
    static IDocumentStore Wrap(IDocumentStore inner, IServiceProvider sp, string? storeName)
        => inner as InstrumentedDocumentStore
           ?? new InstrumentedDocumentStore(inner, sp.GetRequiredService<DocumentStoreMetrics>(), storeName);

    // Rebuild a factory for the original (undecorated) non-keyed store, preserving its registration shape.
    static Func<IServiceProvider, IDocumentStore> BuildInnerFactory(IServiceCollection services, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationFactory != null)
            return sp => (IDocumentStore)descriptor.ImplementationFactory(sp);

        if (descriptor.ImplementationInstance != null)
        {
            var instance = (IDocumentStore)descriptor.ImplementationInstance;
            return _ => instance;
        }

        // Let the container activate the concrete type (no reflection on our side).
        var implType = descriptor.ImplementationType!;
        services.Add(new ServiceDescriptor(implType, implType, descriptor.Lifetime));
        return sp => (IDocumentStore)sp.GetRequiredService(implType);
    }

    // Same, for a keyed descriptor's three shapes.
    static Func<IServiceProvider, object?, IDocumentStore> BuildKeyedInnerFactory(IServiceCollection services, ServiceDescriptor descriptor)
    {
        if (descriptor.KeyedImplementationFactory != null)
            return (sp, key) => (IDocumentStore)descriptor.KeyedImplementationFactory(sp, key);

        if (descriptor.KeyedImplementationInstance != null)
        {
            var instance = (IDocumentStore)descriptor.KeyedImplementationInstance;
            return (_, _) => instance;
        }

        var implType = descriptor.KeyedImplementationType!;
        services.Add(new ServiceDescriptor(implType, descriptor.ServiceKey, implType, descriptor.Lifetime));
        return (sp, key) => (IDocumentStore)sp.GetRequiredKeyedService(implType, key);
    }
}
