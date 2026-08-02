using XenoAtom.Terminal.UI;

namespace ShinyDocDbMyAdmin.Tui.Shell;

/// <summary>
/// One full-window view - the terminal equivalent of a routed page in the web front end.
/// </summary>
/// <remarks>
/// The visual is built once and kept: screens hold their own <see cref="State{T}"/> and let the
/// binding layer re-render the parts that changed. Rebuilding the tree on every data change would
/// throw away focus and scroll position, which in a keyboard-driven app is the whole interface.
/// </remarks>
public abstract class Screen(AdminShell shell)
{
    Visual? view;

    protected AdminShell Shell { get; } = shell;

    /// <summary>Shown in the status bar's breadcrumb.</summary>
    public abstract string Title { get; }

    /// <summary>The visual, built on first request and reused after that.</summary>
    public Visual View => this.view ??= this.Build();

    protected abstract Visual Build();

    /// <summary>
    /// Loads whatever the screen shows. Called after the screen is pushed, and again on refresh.
    /// Runs off the UI thread; assign to state through <see cref="AdminShell.Post"/>.
    /// </summary>
    public virtual Task Load(CancellationToken ct) => Task.CompletedTask;

    /// <summary>Screen-scoped commands, merged into the command palette while this screen is on top.</summary>
    public virtual IEnumerable<XenoAtom.Terminal.UI.Commands.Command> Commands() => [];
}
