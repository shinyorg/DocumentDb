using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb;

public static class JsonSchemaServiceCollectionExtensions
{
    /// <summary>
    /// Registers JSON Schema validation as a DI-resolved <see cref="IDocumentInterceptor"/>. The store's
    /// interceptor pipeline picks it up automatically — no core change required. Map schemas to document
    /// types inside <paramref name="configure"/>.
    /// </summary>
    public static IServiceCollection AddDocumentJsonSchema(this IServiceCollection services, Action<JsonSchemaOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new JsonSchemaOptions();
        configure(options);
        services.AddSingleton(options);
        services.AddSingleton<IDocumentInterceptor>(sp => new JsonSchemaInterceptor(sp.GetRequiredService<JsonSchemaOptions>()));
        return services;
    }

    /// <summary>
    /// Options-based registration, for use alongside <c>AddInterceptor</c> inside <c>AddDocumentStore(...)</c>.
    /// Adds the schema-validation interceptor directly to the store options.
    /// </summary>
    public static DocumentStoreOptions AddJsonSchemaValidation(this DocumentStoreOptions options, Action<JsonSchemaOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var schemaOptions = new JsonSchemaOptions();
        configure(schemaOptions);
        options.AddInterceptor(new JsonSchemaInterceptor(schemaOptions));
        return options;
    }
}
