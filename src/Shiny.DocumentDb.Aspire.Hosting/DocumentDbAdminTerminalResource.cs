using Aspire.Hosting.ApplicationModel;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// The DocumentDb terminal admin tool as an Aspire resource - the <c>shinydocdb</c> dotnet tool run as
/// a process alongside the stores the AppHost models, attached to from the dashboard's terminal view.
/// </summary>
/// <remarks>
/// <para>
/// A process rather than a container, which is the whole point of using this one: the terminal front
/// end runs on your machine, so a file-backed store (SQLite, SQLCipher, DuckDB) at a host path is
/// simply reachable. The containerised web front end needs a bind mount and a rewritten connection
/// string for the same thing.
/// </para>
/// <para>
/// It is a development-loop resource and nothing else - see
/// <see cref="DocumentDbAdminTerminalResourceBuilderExtensions.AddDocumentDbAdminTerminal"/> for why
/// it is excluded from the manifest.
/// </para>
/// </remarks>
public sealed class DocumentDbAdminTerminalResource : ExecutableResource
{
    internal DocumentDbAdminTerminalResource(
        string name,
        string command,
        string workingDirectory,
        IReadOnlyList<string> launcherArgs
    )
        : base(name, command, workingDirectory)
        => this.LauncherArgs = launcherArgs;

    /// <summary>
    /// The arguments that select the tool itself rather than configure it - empty for the global tool,
    /// <c>tool run shinydocdb</c> for a local one. Always emitted first, whatever order the
    /// <c>With*</c> calls came in.
    /// </summary>
    internal IReadOnlyList<string> LauncherArgs { get; }

    /// <summary>The connection to open straight into, if any. Last call wins - it is one argument.</summary>
    internal string? StartupProfile { get; set; }
}
