using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Shiny.DocumentDb.Aspire.Client.Internal;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Diagnostics;

namespace Shiny.DocumentDb.Aspire.Client;

/// <summary>
/// Consuming-service extensions that wire a DocumentDb store from Aspire-injected configuration:
/// the connection string (<c>ConnectionStrings:&lt;name&gt;</c>) plus the provider discriminator
/// (<c>Shiny:DocumentDb:&lt;name&gt;:Provider</c>). Adds a health check and OpenTelemetry by default.
/// </summary>
public static class DocumentStoreClientExtensions
{
    /// <summary>
    /// Registers a keyed DocumentDb store named <paramref name="name"/>, selecting the provider from
    /// the host-injected discriminator (or <see cref="DocumentStoreSettings.Provider"/> override) and
    /// the connection string from <c>ConnectionStrings:&lt;name&gt;</c> (or
    /// <see cref="DocumentStoreSettings.ConnectionString"/>). Health check + telemetry are wired unless
    /// disabled in settings.
    /// <para>
    /// Use <paramref name="configureOptions"/> for plain option setup (JSON contexts, type/table maps,
    /// query filters, interceptor instances, …). Use <paramref name="configureServiceOptions"/> when the
    /// configuration needs the resolved <see cref="IServiceProvider"/> — for example resolving an
    /// interceptor or custom tenant accessor from the container. Set
    /// <see cref="DocumentStoreSettings.MultiTenant"/> for the common shared-table tenancy case, which
    /// wires <c>TenantIdAccessor</c> from a registered <see cref="ITenantResolver"/> automatically.
    /// </para>
    /// </summary>
    public static IServiceCollection AddDocumentStore(
        this IHostApplicationBuilder builder,
        string name,
        Action<DocumentStoreSettings>? configureSettings = null,
        Action<DocumentStoreOptions>? configureOptions = null,
        Action<IServiceProvider, DocumentStoreOptions>? configureServiceOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var settings = new DocumentStoreSettings();
        configureSettings?.Invoke(settings);

        var connectionString = settings.ConnectionString
            ?? builder.Configuration.GetConnectionString(name)
            ?? throw new InvalidOperationException(
                $"No connection string found for DocumentDb store '{name}'. Expected 'ConnectionStrings:{name}' " +
                "(injected by WithReference on the AppHost) or DocumentStoreSettings.ConnectionString.");

        var kind = settings.Provider ?? ResolveProvider(builder.Configuration, name);

        builder.Services.AddDocumentStore(name, (sp, o) =>
        {
            o.DatabaseProvider = DatabaseProviderFactory.Create(kind, connectionString);
            if (settings.MultiTenant)
                o.TenantIdAccessor = () => sp.GetRequiredService<ITenantResolver>().GetCurrentTenant();
            configureOptions?.Invoke(o);
            configureServiceOptions?.Invoke(sp, o);
        });

        if (!settings.DisableHealthChecks)
        {
            builder.Services
                .AddHealthChecks()
                .AddCheck(
                    $"DocumentDb.{name}",
                    new DocumentStoreHealthCheck(kind, connectionString));
        }

        if (!settings.DisableMetrics || !settings.DisableTracing)
        {
            // Instrumentation is embedded and always-on in the store (tagged db.namespace = name for a keyed
            // store), so there's nothing to decorate — just subscribe the OTel pipeline to its meter/source.
            var otel = builder.Services.AddOpenTelemetry();
            if (!settings.DisableMetrics)
                otel.WithMetrics(m => m.AddMeter(DocumentStoreClientConstants.TelemetryName));
            if (!settings.DisableTracing)
                otel.WithTracing(t => t.AddSource(DocumentStoreClientConstants.TelemetryName));
        }

        return builder.Services;
    }

    static DocumentProviderKind ResolveProvider(IConfiguration configuration, string name)
    {
        var raw = configuration[DocumentStoreClientConstants.ProviderConfigKey(name)];
        if (string.IsNullOrWhiteSpace(raw))
            throw new InvalidOperationException(
                $"No DocumentDb provider discriminator found for store '{name}'. Expected config key " +
                $"'{DocumentStoreClientConstants.ProviderConfigKey(name)}' (injected by WithReference on the AppHost) " +
                "or DocumentStoreSettings.Provider.");

        if (!Enum.TryParse<DocumentProviderKind>(raw, ignoreCase: true, out var kind))
            throw new InvalidOperationException(
                $"DocumentDb provider discriminator '{raw}' for store '{name}' is not a recognized DocumentProviderKind " +
                "(Sqlite, Postgres, CockroachDb, SqlServer, MySql, MariaDb).");

        return kind;
    }
}
