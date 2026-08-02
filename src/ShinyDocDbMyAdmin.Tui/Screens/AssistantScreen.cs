using ShinyDocDbMyAdmin.Tui.Panels;
using ShinyDocDbMyAdmin.Tui.Shell;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Screens;

/// <summary>
/// The assistant opened from a connection rather than from a type.
/// </summary>
/// <remarks>
/// The same panel the workspace tab uses, with no type in scope - so the opening instruction does not
/// claim the user is looking at one. Reached from the database overview, which is where you are when
/// the question is about the database rather than about a particular type.
/// </remarks>
public sealed class AssistantScreen : Screen
{
    readonly AssistantPanel panel;

    public AssistantScreen(AdminShell shell, string profileId) : base(shell)
    {
        this.ProfileId = profileId;

        // No table and no type: WorkspaceContext carries them because most panels need them, and the
        // assistant is the one that is just as useful without.
        this.panel = new AssistantPanel(new WorkspaceContext(shell, profileId, "", ""));
    }

    public string ProfileId { get; }

    public override string Title => "Assistant";

    protected override Visual Build()
        => new Padder(new VStack(Ui.Title("Assistant"), this.panel.View.Stretch()).Spacing(1))
            .Padding(new Thickness(1));

    public override Task Load(CancellationToken ct) => this.panel.Load(ct);
}
