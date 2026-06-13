using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;
using Shiny.DocumentDb.Orleans;

namespace Orleans.Hosting;

public static class DocumentDbGrainStorageSiloBuilderExtensions
{
    /// <summary>Adds a Shiny.DocumentDb-backed grain storage provider with the given name.</summary>
    public static ISiloBuilder AddDocumentDbGrainStorage(
        this ISiloBuilder builder,
        string name,
        Action<DocumentDbGrainStorageOptions> configure
    ) => builder.ConfigureServices(services => services.AddDocumentDbGrainStorage(name, configure));

    /// <summary>Adds a Shiny.DocumentDb-backed grain storage provider as the default provider.</summary>
    public static ISiloBuilder AddDocumentDbGrainStorageAsDefault(
        this ISiloBuilder builder,
        Action<DocumentDbGrainStorageOptions> configure
    ) => builder.AddDocumentDbGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configure);

    public static IServiceCollection AddDocumentDbGrainStorage(
        this IServiceCollection services,
        string name,
        Action<DocumentDbGrainStorageOptions> configure
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddOptions<DocumentDbGrainStorageOptions>(name).Configure(configure);

        services.AddTransient<IConfigurationValidator>(sp =>
            new DocumentDbGrainStorageOptionsValidator(
                sp.GetRequiredService<IOptionsMonitor<DocumentDbGrainStorageOptions>>().Get(name),
                name));

        // Default-name provider is resolvable as the unkeyed IGrainStorage.
        services.TryAddSingleton(sp =>
            sp.GetKeyedService<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME)!);

        services.AddKeyedSingleton<IGrainStorage>(name, (sp, key) =>
            DocumentDbGrainStorage.Create(sp, (string)key!));

        services.AddKeyedSingleton<ILifecycleParticipant<ISiloLifecycle>>(name, (sp, key) =>
            (ILifecycleParticipant<ISiloLifecycle>)sp.GetRequiredKeyedService<IGrainStorage>(key!));

        return services;
    }
}
