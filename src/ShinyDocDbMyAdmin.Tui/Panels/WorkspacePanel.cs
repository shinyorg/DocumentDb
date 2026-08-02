using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Shell;
using XenoAtom.Terminal.UI;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>
/// What every workspace panel is pointed at: one type, in one table, on one connection.
/// </summary>
/// <remarks>
/// <see cref="ReadOnly"/> is state rather than a value because a profile can be edited while its
/// workspace is open, and every write button in every panel is bound to it.
/// </remarks>
public sealed class WorkspaceContext(AdminShell shell, string profileId, string table, string typeName)
{
    public AdminShell Shell { get; } = shell;
    public string ProfileId { get; } = profileId;
    public string Table { get; } = table;
    public string TypeName { get; } = typeName;

    public DocumentAdminService Admin => this.Shell.Admin;

    public State<bool> ReadOnly { get; } = new(true);

    /// <summary>True when a write path may be offered. Read-only is enforced below this too.</summary>
    public bool CanWrite => !this.ReadOnly.Value;

    public void Run(Func<CancellationToken, Task> work, string? failureContext = null)
        => this.Shell.RunAsync(work, failureContext);

    public void Post(Action action) => this.Shell.Post(action);
}

/// <summary>One tab of the type workspace.</summary>
public abstract class WorkspacePanel(WorkspaceContext context)
{
    Visual? view;

    protected WorkspaceContext Context { get; } = context;

    protected AdminShell Shell => this.Context.Shell;

    public Visual View => this.view ??= this.Build();

    protected abstract Visual Build();

    /// <summary>Called the first time the tab is opened, and again on an explicit reload.</summary>
    public abstract Task Load(CancellationToken ct);
}
