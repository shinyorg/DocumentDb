using Microsoft.Extensions.DependencyInjection;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Screens;
using ShinyDocDbMyAdmin.Tui.Widgets;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;

namespace ShinyDocDbMyAdmin.Tui.Panels;

/// <summary>
/// A read-only chat over the data, configured per connection.
/// </summary>
/// <remarks>
/// The assistant's reach is deliberately every connection, not only the one it was opened from -
/// "is this order id in staging too?" is exactly the question worth asking. It can only read: the
/// write paths are never registered as tools, so there is nothing to call however it is asked. That
/// reach is what the warning at the top of the panel is about.
/// </remarks>
public sealed class AssistantPanel(WorkspaceContext context) : WorkspacePanel(context)
{
    readonly State<string> composed = new("");
    readonly State<string> statusLine = new("");
    readonly State<int> revision = new(0);

    TuiChatSession? session;
    CancellationTokenSource? inFlight;

    protected override Visual Build()
    {
        var prompt = Ui.Area(this.composed).MinHeight(3);
        prompt.AddKeyBinding(new KeyGesture(TerminalKey.Enter, TerminalModifiers.Ctrl), this.Send);

        var toolbar = Ui.Toolbar(
            Ui.Primary("Send", this.Send),
            Ui.Action("Stop", () => this.inFlight?.Cancel()),
            Ui.Action("New conversation", this.Reset),
            Ui.Action("Settings", () => this.Shell.Push(new AiSettingsScreen(this.Shell, this.Context.ProfileId)))
        );

        return new VStack(
            Ui.Warning(
                "The assistant can read every connection configured here, not only this one, and whatever it reads " +
                "is sent to the model provider you configured. It cannot write - the write paths are not registered " +
                "as tools, so there is nothing for it to call."),
            new ScrollViewer(new ComputedVisual(() =>
            {
                _ = this.revision.Value;
                return this.BuildTranscript();
            })).Stretch(),
            new Markup(() => this.statusLine.Value),
            new VStack(Ui.Label("Ask (Ctrl+Enter sends)"), prompt).Spacing(0),
            toolbar
        ).Spacing(1);
    }

    Visual BuildTranscript()
    {
        if (this.session is not { Transcript.Count: > 0 } live)
            return Ui.Empty("Ask something about this data.");

        var stack = new VStack().Spacing(1);
        foreach (var turn in live.Transcript)
        {
            var (label, colour) = turn.Speaker switch
            {
                ChatSpeaker.User => ("you", "cyan"),
                ChatSpeaker.Assistant => ("assistant", "green"),
                ChatSpeaker.Tools => ("read", "dim"),
                _ => ("error", "red")
            };

            var body = turn.Text.Length == 0 ? "…" : turn.Text;
            stack.Add(new VStack(
                new Markup($"[{colour}][bold]{label}[/][/]"),
                new Markup(Ui.Escape(body)).Wrap(true)
            ).Spacing(0));
        }

        return stack;
    }

    public override async Task Load(CancellationToken ct)
    {
        if (this.session is not null)
            return;

        this.Shell.AiAvailability.AssertEnabled();

        var profile = await this.Shell.Profiles.Get(this.Context.ProfileId, ct)
                      ?? throw new InvalidOperationException("That connection no longer exists.");

        var settings = await this.Shell.Profiles.GetAiSettings(this.Context.ProfileId, ct);
        if (settings is null || !settings.IsUsable())
        {
            this.Context.Post(() => this.statusLine.Value = "[yellow]No assistant is configured for this connection. Press Settings.[/]");
            return;
        }

        var factory = this.Shell.Services.GetRequiredService<AiClientFactory>();
        var surface = this.Shell.Services.GetRequiredService<AiToolSurface>();

        var client = factory.Create(settings);
        var built = new TuiChatSession(
            client,
            surface.Build(),
            AiPrompt.Build(AiPrompt.Surface.Terminal, profile.Name, this.Context.Table, this.Context.TypeName)
        );

        this.Context.Post(() =>
        {
            this.session = built;
            this.statusLine.Value = $"[dim]{settings.Provider} · {Ui.Escape(settings.Model)}[/]";
            this.revision.Value++;
        });
    }

    void Send()
    {
        if (this.session is not { } live)
        {
            this.Shell.Info("Configure the assistant for this connection first.");
            return;
        }

        var text = this.composed.Value;
        if (string.IsNullOrWhiteSpace(text))
            return;

        this.composed.Value = "";

        this.inFlight?.Cancel();
        var cts = CancellationTokenSource.CreateLinkedTokenSource(this.Shell.Lifetime);
        this.inFlight = cts;

        this.Context.Run(async _ =>
        {
            // Repaint on every chunk, marshalled back onto the render thread - the whole point of
            // streaming is that the answer appears as it is written.
            await live.Send(text, () => this.Context.Post(() => this.revision.Value++), cts.Token);
        }, "The assistant call failed");
    }

    void Reset()
    {
        this.inFlight?.Cancel();

        // Detached here, on the render thread, before any of it goes async: Load returns immediately
        // when a session already exists, so clearing the field from a posted callback would race the
        // reload that is meant to replace it.
        var old = this.session;
        this.session = null;
        this.revision.Value++;

        this.Context.Run(async ct =>
        {
            if (old is not null)
                await old.DisposeAsync();

            await this.Load(ct);
        }, "Could not start a new conversation");
    }
}
