using System.Diagnostics.CodeAnalysis;
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Shiny.DocumentDb.Aspire.Hosting.Internal;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// Adds the DocumentDb terminal admin tool to an AppHost as a resource you attach a terminal to from
/// the dashboard - the same admin layer as <see cref="DocumentDbAdminResourceBuilderExtensions"/>,
/// without a browser or a container.
/// </summary>
/// <remarks>
/// <para>
/// This rides on Aspire's experimental terminal support, so the whole surface carries Aspire's own
/// <c>ASPIRETERMINAL001</c> diagnostic: one suppression covers both this and any direct
/// <c>WithTerminal</c> call in the same AppHost. It needs Aspire 13.5 or later, and the
/// <c>aspire terminal</c> CLI command is itself behind a feature flag
/// (<c>aspire config set features.terminalCommandsEnabled true</c>) - the dashboard's terminal view
/// needs no flag.
/// </para>
/// <para>
/// The tool has to be installed. Either globally (<c>dotnet tool install -g ShinyDocDbMyAdmin.Tui</c>)
/// or in the repository's local manifest - see <see cref="DocumentDbAdminTerminalTool"/>.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// #pragma warning disable ASPIRETERMINAL001
/// var pg    = builder.AddPostgres("pg").AddDatabase("ordersdb");
/// var store = pg.AsDocumentStore("orders");
///
/// builder.AddDocumentDbAdminTerminal()
///        .WithReference(store)
///        .WithStartupProfile(store)
///        .WaitFor(pg);
/// </code>
/// </example>
[Experimental("ASPIRETERMINAL001")]
public static class DocumentDbAdminTerminalResourceBuilderExtensions
{
    /// <summary>The command the <c>ShinyDocDbMyAdmin.Tui</c> package installs.</summary>
    public const string ToolCommand = "shinydocdb";

    /// <summary>The package the command comes from, for the install line in an error message.</summary>
    public const string ToolPackageId = "ShinyDocDbMyAdmin.Tui";

    /// <summary>
    /// Terminal width the tool is given by default. The stock 120 columns cuts the explorer pane off
    /// the document grid; this fits both.
    /// </summary>
    public const int DefaultColumns = 160;

    /// <summary>Terminal height the tool is given by default, up from the stock 30.</summary>
    public const int DefaultRows = 45;

    /// <summary>
    /// Adds the terminal admin tool. Reference stores into it with <c>WithReference</c> and they show
    /// up already connected - the tool reads the same
    /// <c>ConnectionStrings:{name}</c> + <c>Shiny:DocumentDb:{name}:Provider</c> pair the container
    /// front end and the client integration read.
    /// </summary>
    /// <param name="builder">The AppHost builder.</param>
    /// <param name="name">Resource name, and the name shown in the dashboard.</param>
    /// <param name="tool">Where the <c>shinydocdb</c> command comes from. Defaults to the global tool.</param>
    /// <param name="workingDirectory">
    /// Left null, the AppHost's own directory - which is what
    /// <see cref="DocumentDbAdminTerminalTool.Local"/> needs, since <c>dotnet tool run</c> resolves the
    /// manifest from the current directory upwards.
    /// </param>
    /// <param name="configureTerminal">
    /// Sizes the pseudo-terminal the tool is given. This is the place to change it: the underlying
    /// annotation is set once, here.
    /// </param>
    /// <remarks>
    /// <para>
    /// The resource is excluded from the manifest, because there is nothing to deploy. Aspire's
    /// terminal sessions are a dev-loop feature - the dashboard and CLI attach to a live pseudo-terminal
    /// on the machine running the AppHost - and a <c>dotnet tool</c> on a developer's box is not a
    /// deployment target either. For an admin UI that <em>does</em> publish, use
    /// <see cref="DocumentDbAdminResourceBuilderExtensions.AddDocumentDbAdmin"/>: it is a container, and
    /// it goes wherever the AppHost goes.
    /// </para>
    /// <para>
    /// Unlike the container, this shares the developer's own <c>~/.shinydocdbmyadmin</c> - the same
    /// saved connections they see when they run <c>shinydocdb</c> themselves, alongside the ones the
    /// AppHost hands over. Point it somewhere else with <see cref="WithDataDirectory"/> when that
    /// mixing is not what you want.
    /// </para>
    /// </remarks>
    public static IResourceBuilder<DocumentDbAdminTerminalResource> AddDocumentDbAdminTerminal(
        this IDistributedApplicationBuilder builder,
        [ResourceName] string name = "documentdb-terminal",
        DocumentDbAdminTerminalTool tool = DocumentDbAdminTerminalTool.Global,
        string? workingDirectory = null,
        Action<TerminalOptions>? configureTerminal = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        // A local tool is not on PATH: it is reached through the SDK, with the manifest resolved from
        // the working directory. The launcher arguments go in front of everything the With* methods
        // add, which is why they live on the resource rather than in a WithArgs call whose position
        // depends on the order the AppHost happened to write.
        var (command, launcherArgs) = tool switch
        {
            DocumentDbAdminTerminalTool.Global => (ToolCommand, (IReadOnlyList<string>)[]),
            DocumentDbAdminTerminalTool.Local => ("dotnet", ["tool", "run", ToolCommand]),
            _ => throw new ArgumentOutOfRangeException(nameof(tool), tool, "Unknown tool installation.")
        };

        var resource = new DocumentDbAdminTerminalResource(
            name,
            command,
            workingDirectory ?? builder.AppHostDirectory,
            launcherArgs
        );

        return builder
            .AddResource(resource)
            .WithTerminal(options =>
            {
                options.Columns = DefaultColumns;
                options.Rows = DefaultRows;
                configureTerminal?.Invoke(options);
            })
            .WithArgs(context =>
            {
                foreach (var arg in resource.LauncherArgs)
                    context.Args.Add(arg);

                if (!string.IsNullOrWhiteSpace(resource.StartupProfile))
                {
                    context.Args.Add("--profile");
                    context.Args.Add(resource.StartupProfile);
                }
            })
            .ExcludeFromManifest();
    }

    /// <summary>
    /// Opens straight into a connection instead of the connection list - the difference between a
    /// dashboard tab that is already showing your data and one that is asking which database you meant.
    /// </summary>
    /// <param name="builder">The terminal resource.</param>
    /// <param name="profileName">
    /// A connection name or id. A store referenced in with <c>WithReference</c> is named after the
    /// resource, so that is the name to use; a connection the developer saved themselves works too.
    /// </param>
    public static IResourceBuilder<DocumentDbAdminTerminalResource> WithStartupProfile(
        this IResourceBuilder<DocumentDbAdminTerminalResource> builder,
        string profileName
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(profileName);

        builder.Resource.StartupProfile = profileName;
        return builder;
    }

    /// <summary>
    /// The same, naming a store the AppHost models. This does not reference the store in - pair it with
    /// <c>WithReference(store)</c>, which is what makes the connection exist at all.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminTerminalResource> WithStartupProfile(
        this IResourceBuilder<DocumentDbAdminTerminalResource> builder,
        IResourceBuilder<DocumentStoreResource> store
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(store);

        return builder.WithStartupProfile(store.Resource.Name);
    }

    /// <summary>
    /// Points the tool at a different directory for its own state - saved connections, saved queries,
    /// the log file and the key that protects stored credentials.
    /// </summary>
    /// <remarks>
    /// Worth doing when the AppHost should not see, or write to, the connections the developer keeps in
    /// <c>~/.shinydocdbmyadmin</c>. A path under the AppHost's <c>obj</c> gives the run its own
    /// scratch store; connections handed over with <c>WithReference</c> are unaffected either way,
    /// since those are never written to the profile store.
    /// </remarks>
    public static IResourceBuilder<DocumentDbAdminTerminalResource> WithDataDirectory(
        this IResourceBuilder<DocumentDbAdminTerminalResource> builder,
        string path
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        return builder.WithEnvironment(AdminConstants.DataDirectoryEnvVar, Path.GetFullPath(path));
    }

    /// <summary>
    /// Blocks every write path in the UI for every store the AppHost hands over - inserts, edits,
    /// deletes, index changes and non-SELECT statements in the SQL console. The setting you want when
    /// the AppHost points at anything you did not create five minutes ago.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminTerminalResource> WithReadOnly(
        this IResourceBuilder<DocumentDbAdminTerminalResource> builder,
        bool readOnly = true
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithEnvironment(AdminConstants.ReadOnlyEnvVar, readOnly ? "true" : "false");
    }

    /// <summary>
    /// Removes the AI assistant from the tool entirely - no tab, and nothing behind it registered.
    /// </summary>
    public static IResourceBuilder<DocumentDbAdminTerminalResource> WithoutAi(
        this IResourceBuilder<DocumentDbAdminTerminalResource> builder,
        bool disabled = true
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithEnvironment(AdminConstants.DisableAiEnvVar, disabled ? "true" : "false");
    }
}
