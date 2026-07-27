using System.Reflection;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// Adds the DocumentDb Admin UI to an AppHost - a browser front end over the stores the AppHost
/// already models, in the same spirit as running pgAdmin next to Postgres.
/// </summary>
/// <example>
/// <code>
/// var store = builder.AddPostgresDocumentStore("orders");
///
/// builder.AddDocumentDbAdmin(port: 8085)
///        .WithReference(store)
///        .WaitFor(store);
/// </code>
/// </example>
public static class DocumentDbAdminResourceBuilderExtensions
{
    /// <summary>The registry the admin image is published to.</summary>
    public const string ContainerRegistry = "ghcr.io";

    /// <summary>The admin image, within <see cref="ContainerRegistry"/>.</summary>
    public const string ContainerImage = "shinyorg/shiny-docdb-myadmin";

    /// <summary>The port the image listens on. Matches ASPNETCORE_HTTP_PORTS in its Dockerfile.</summary>
    public const int ContainerPort = 8080;

    /// <summary>Where the image keeps its state, and so what a data volume or bind mount maps onto.</summary>
    public const string DataPath = "/data";

    const string EndpointName = "http";
    const string SecretKeyEnvVar = "ShinyDocDbMyAdmin__SecretKey";
    const string ReadOnlyEnvVar = "ShinyDocDbMyAdmin__ReadOnly";

    /// <summary>AssemblyMetadata key the csproj stamps $(PackageVersion) into. Change one, change the other.</summary>
    const string NuGetPackageVersionKey = "NuGetPackageVersion";

    /// <summary>
    /// Adds the DocumentDb Admin UI. Reference stores into it with <c>WithReference</c> and they show
    /// up already connected - the tool reads the same
    /// <c>ConnectionStrings:{name}</c> + <c>Shiny:DocumentDb:{name}:Provider</c> pair that the client
    /// integration does.
    /// </summary>
    /// <param name="builder">The AppHost builder.</param>
    /// <param name="name">Resource name, and the name shown in the dashboard.</param>
    /// <param name="port">
    /// The host port to serve the UI on. Left null, Aspire picks a free one - fine for poking around,
    /// worth pinning when you want a bookmarkable URL.
    /// </param>
    /// <remarks>
    /// This is a container, so it reaches the databases the AppHost models over the container network -
    /// which Aspire wires up for you. What it cannot reach is a <em>file</em> on your machine: a SQLite,
    /// SQLCipher or DuckDB store living at a host path is not visible inside the container unless you
    /// mount it, so pair those with <see cref="WithHostPath"/>.
    /// </remarks>
    public static IResourceBuilder<DocumentDbAdminResource> AddDocumentDbAdmin(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name = "documentdb-admin",
        int? port = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return builder
            .AddResource(new DocumentDbAdminResource(name))
            // WithImage first: WithImageRegistry throws unless the image annotation already exists.
            .WithImage(ContainerImage, PackageVersion ?? "latest")
            .WithImageRegistry(ContainerRegistry)
            .WithHttpEndpoint(port: port, targetPort: ContainerPort, name: EndpointName)
            .WithHttpHealthCheck("/", endpointName: EndpointName);
    }

    /// <summary>
    /// Sets the host port the UI is served on - the one you type into a browser. Null hands the choice
    /// back to Aspire.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminResource> WithHostPort(
        this IResourceBuilder<DocumentDbAdminResource> builder,
        int? port)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithEndpoint(EndpointName, endpoint => endpoint.Port = port);
    }

    /// <summary>
    /// Keeps the tool's own state - saved connections, saved queries and uploaded database files - in a
    /// named volume, so it survives the container being recreated. Without this, anything saved in the
    /// UI is gone at the end of the run.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminResource> WithDataVolume(
        this IResourceBuilder<DocumentDbAdminResource> builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithVolume(name ?? $"{builder.Resource.Name}-data", DataPath);
    }

    /// <summary>
    /// Mounts a directory from your machine into the container, which is how a file-backed store
    /// (SQLite, SQLCipher, DuckDB) becomes reachable at all.
    /// </summary>
    /// <param name="builder">The admin resource.</param>
    /// <param name="hostPath">The directory on your machine holding the database files.</param>
    /// <param name="containerPath">
    /// Where it appears inside the container. The connection string you reference in must use
    /// <em>this</em> path, not the host one.
    /// </param>
    /// <param name="isReadOnly">Mount read-only. Worth doing for anything you are only inspecting.</param>
    /// <example>
    /// <code>
    /// var store = builder.AddSqliteDocumentStore("orders", "/data/orders.db");
    ///
    /// builder.AddDocumentDbAdmin()
    ///        .WithHostPath("/Users/me/databases", "/data")
    ///        .WithReference(store);
    /// </code>
    /// </example>
    public static IResourceBuilder<DocumentDbAdminResource> WithHostPath(
        this IResourceBuilder<DocumentDbAdminResource> builder,
        string hostPath,
        string containerPath,
        bool isReadOnly = false)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(hostPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(containerPath);

        return builder.WithBindMount(hostPath, containerPath, isReadOnly);
    }

    /// <summary>
    /// The key that encrypts the secret-bearing parts of a saved connection (connection strings and
    /// SQLCipher keys). Without one the tool generates a key next to the database it protects - fine
    /// until the data volume outlives the run, at which point pin it.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminResource> WithSecretKey(
        this IResourceBuilder<DocumentDbAdminResource> builder,
        string key)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        return builder.WithEnvironment(SecretKeyEnvVar, key);
    }

    /// <summary>
    /// The same, from a parameter - the form to use for anything real, so the key lives in user secrets
    /// or the deployment's configuration rather than in the AppHost source.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminResource> WithSecretKey(
        this IResourceBuilder<DocumentDbAdminResource> builder,
        IResourceBuilder<ParameterResource> key)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(key);

        return builder.WithEnvironment(SecretKeyEnvVar, key);
    }

    /// <summary>
    /// Blocks every write path in the UI for every store the AppHost hands over - inserts, edits,
    /// deletes, index changes and non-SELECT statements in the SQL console. The setting you want when
    /// the AppHost points at anything you did not create five minutes ago.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminResource> WithReadOnly(
        this IResourceBuilder<DocumentDbAdminResource> builder,
        bool readOnly = true)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithEnvironment(ReadOnlyEnvVar, readOnly ? "true" : "false");
    }

    /// <summary>
    /// This package's own NuGet version, used as the default image tag - the admin UI and this
    /// integration are built and released together, so the tags line up.
    /// </summary>
    static string? PackageVersion
    {
        get
        {
            // Baked in by the csproj from MSBuild's $(PackageVersion), so this is literally the
            // version this package shipped as - and the admin image is pushed under the same one.
            //
            // Deriving it from AssemblyInformationalVersion instead does not work: Nerdbank stamps
            // that from the four-part build version, so a package released as "12.2.1" carries
            // "12.2.1.1+d1e4e9e2c3" and yields a "12.2.1.1" tag that was never published.
            var version = typeof(DocumentDbAdminResourceBuilderExtensions).Assembly
                .GetCustomAttributes<AssemblyMetadataAttribute>()
                .FirstOrDefault(x => x.Key == NuGetPackageVersionKey)
                ?.Value;

            return string.IsNullOrWhiteSpace(version) ? null : version;
        }
    }
}
