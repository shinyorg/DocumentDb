using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.DocumentDb.DocumentDb;

namespace Shiny.DocumentDb;

public static class DocumentDbServiceCollectionExtensions
{
    /// <summary>
    /// Registers an Amazon DocumentDB <see cref="IDocumentStore"/> as a singleton, along with
    /// <see cref="IDocumentMaintenance"/> pointing at the same instance. Amazon DocumentDB is MongoDB
    /// wire-compatible; TLS is on by default and <c>retryWrites</c> is off by default — see
    /// <see cref="DocumentDbDocumentStoreOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configures the store options (connection string, database, TLS / CA bundle, mappings).</param>
    public static IServiceCollection AddDocumentDbDocumentStore(
        this IServiceCollection services,
        Action<DocumentDbDocumentStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // ConnectionString/DatabaseName are `required` on the base options; the placeholders are overwritten
        // by configure (which supplies the real values).
        var options = new DocumentDbDocumentStoreOptions
        {
            ConnectionString = null!,
            DatabaseName = null!
        };
        configure(options);

        // Factory (not an eager instance) so the SP-taking ctor can wire DI-registered interceptors.
        services.AddSingleton<IDocumentStore>(sp => new DocumentDbDocumentStore(options, sp));
        services.TryAddSingleton<IDocumentMaintenance>(sp => (IDocumentMaintenance)sp.GetRequiredService<IDocumentStore>());
        return services;
    }
}
