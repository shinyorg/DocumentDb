namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// How the AppHost finds the <c>shinydocdb</c> tool. Both forms run the same tool - the difference is
/// who installed it and whether a fresh clone gets it.
/// </summary>
public enum DocumentDbAdminTerminalTool
{
    /// <summary>
    /// The globally installed tool (<c>dotnet tool install -g ShinyDocDbMyAdmin.Tui</c>) - the
    /// <c>shinydocdb</c> command on PATH.
    /// </summary>
    Global,

    /// <summary>
    /// The tool from the repository's local manifest (<c>dotnet tool install ShinyDocDbMyAdmin.Tui</c>),
    /// run through <c>dotnet tool run</c>. The form to use for a shared repository: a fresh clone gets
    /// the tool - at the pinned version - from <c>dotnet tool restore</c> rather than from a README
    /// step somebody skipped.
    /// </summary>
    Local
}
