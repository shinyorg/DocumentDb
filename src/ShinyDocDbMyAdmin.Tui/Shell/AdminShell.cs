using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Cli;
using XenoAtom.Terminal;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Commands;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Input;
using XenoAtom.Terminal.UI.Styling;

namespace ShinyDocDbMyAdmin.Tui.Shell;

/// <summary>
/// The application frame: menu, explorer, screen stack, status line, toasts and dialogs.
/// </summary>
/// <remarks>
/// <para>
/// The web front end navigates by URL, which gives it a back button and deep links for free. A
/// terminal has neither, so the equivalent is an explicit screen stack with Escape as the back
/// button and a command palette as the address bar. Everything reachable by clicking is also
/// reachable by name from the palette, because in a terminal that is the only way to reach something
/// you cannot currently see.
/// </para>
/// <para>
/// All state mutation happens on the render thread. Work that touches a database runs on the pool and
/// comes back through <see cref="Post"/>, which is the framework's own dispatcher - assigning to a
/// <see cref="State{T}"/> from a worker thread would race the renderer.
/// </para>
/// </remarks>
public sealed class AdminShell
{
    readonly List<Screen> stack = [];
    readonly State<int> navigation = new(0);
    readonly State<Theme> theme;
    readonly ILogger<AdminShell> log;
    readonly CancellationTokenSource lifetime = new();

    ToastHost toasts = null!;
    WindowLayer windows = null!;
    ExplorerPane explorer = null!;
    Visual root = null!;
    TerminalApp? app;

    public AdminShell(IServiceProvider services, CommandLineOptions options)
    {
        this.Services = services;
        this.Options = options;
        this.Profiles = services.GetRequiredService<ProfileStore>();
        this.Admin = services.GetRequiredService<DocumentAdminService>();
        this.Transfer = services.GetRequiredService<ConnectionTransferService>();
        this.ImportExport = services.GetRequiredService<ImportExportService>();
        this.AiAvailability = services.GetRequiredService<AiAvailability>();
        this.log = services.GetRequiredService<ILogger<AdminShell>>();
        this.Settings = new UiSettings(services.GetRequiredService<AppPaths>());

        var preference = options.Theme ?? this.Settings.Theme;
        this.theme = new State<Theme>(preference == "light" ? Theme.DefaultLight : Theme.Default);
    }

    public IServiceProvider Services { get; }
    public CommandLineOptions Options { get; }
    public ProfileStore Profiles { get; }
    public DocumentAdminService Admin { get; }
    public ConnectionTransferService Transfer { get; }
    public ImportExportService ImportExport { get; }
    public AiAvailability AiAvailability { get; }
    public UiSettings Settings { get; }

    /// <summary>Cancelled when the app is shutting down; every background load hangs off it.</summary>
    public CancellationToken Lifetime => this.lifetime.Token;

    /// <summary>The left-hand message in the status bar. Screens set it to say what they are showing.</summary>
    public State<string> Status { get; } = new("");

    /// <summary>
    /// Drives the busy mark in the status bar. A count of in-flight loads rather than a flag, so two
    /// overlapping loads do not clear each other's mark.
    /// </summary>
    /// <remarks>
    /// The count is kept in a plain field and only assigned into the state: the binding layer refuses a
    /// read-then-write of the same value inside one tracking context, which is exactly what <c>++</c> is.
    /// </remarks>
    public State<int> Pending { get; } = new(0);

    int pending;

    public State<bool> ExitRequested { get; } = new(false);

    public Screen Current => this.stack[^1];

    // ── Frame ───────────────────────────────────────────────────────────

    public Visual Build(Screen initial)
    {
        this.stack.Add(initial);
        this.explorer = new ExplorerPane(this);

        // Rebuilt only when the stack changes. Screens keep their own visual, so a push and a pop
        // return to exactly the view that was left - scroll offset, selection and all.
        var content = new ComputedVisual(() =>
        {
            _ = this.navigation.Value;
            return this.Current.View;
        });

        var body = new HSplitter()
            .First(this.explorer.View)
            .Second(content)
            .Ratio(0.24)
            .MinFirst(22)
            .MinSecond(40);

        var status = new StatusBar(
            new Markup(() => this.StatusMarkup()),
            new Markup(() => this.HintMarkup())
        );

        // Two rows top and bottom, mirroring the web front end's brand row above its toolbar and its
        // credit bar under the whole shell. DockLayout has one slot each way, so each is a VStack.
        var header = new VStack(this.BuildBrand(), this.BuildMenu()).Spacing(0);
        var footer = new VStack(status, BuildFooter()).Spacing(0);

        this.toasts = new ToastHost(new DockLayout(header, body, footer)).Position(ToastPosition.BottomRight);
        this.windows = new WindowLayer(this.toasts);
        this.root = this.windows.Style(this.theme);

        this.RegisterGlobalCommands();
        return this.root;
    }

    /// <summary>
    /// Kicks off the first load. Separate from <see cref="Build"/> so composing the tree and putting
    /// work on the thread pool are two decisions: the render tests and the screenshot harness build a
    /// shell and then drive loading themselves, and a background load racing them would be a source of
    /// intermittent failures rather than of coverage.
    /// </summary>
    public void Start()
    {
        this.Start(this.Current);
        this.explorer.Reload();
    }

    string StatusMarkup()
    {
        var breadcrumb = string.Join(" [dim]›[/] ", this.stack.Select(s => s.Title));
        var status = this.Status.Value;
        var busy = this.Pending.Value > 0 ? "[yellow]● [/]" : "";
        return status.Length == 0 ? $"{busy}{breadcrumb}" : $"{busy}{breadcrumb}  [dim]—[/]  {status}";
    }

    string HintMarkup()
        => this.stack.Count > 1
            ? "[dim]Esc back · Ctrl+P commands · F1 help[/]"
            : "[dim]Ctrl+P commands · F1 help[/]";

    /// <summary>The mark and the wordmark, in the top-left corner - the terminal twin of the web brand.</summary>
    Visual BuildBrand()
        => new HStack(
            new Markup(Splash.MarkRow()),
            new Markup("[bold]ShinyDocDbMyAdmin[/]")
        ).Spacing(1);

    /// <summary>
    /// The credit bar: the site in the middle of the <i>bar</i> and the version hard right, exactly as the
    /// web footer reads.
    /// </summary>
    /// <remarks>
    /// Three tracks — <c>1* auto 1*</c> — which is the same shape as the web footer's CSS grid and for the
    /// same reason: the site is centred against the <i>bar</i>, not against whatever the version left over,
    /// so it does not drift left by half the version's width. An <c>HStack</c> cannot express this; it lays
    /// its children out at their natural width and packs them.
    /// <para>
    /// The site is text rather than a link: a terminal hyperlink is an escape sequence half of them swallow,
    /// and a URL you can select and paste works everywhere.
    /// </para>
    /// </remarks>
    static Visual BuildFooter()
        => new Grid()
            .Columns(
                new ColumnDefinition().Width(GridLength.Star(1)),
                new ColumnDefinition().Width(GridLength.Auto),
                new ColumnDefinition().Width(GridLength.Star(1)))
            .Cells(
                new GridCell(new Markup($"[dim]Built with Shiny .NET · [/][#935CFA]{AppInfo.Site}[/]")).Column(1),
                new GridCell(new Markup($"[dim]v{AppInfo.Version}[/]").HorizontalAlignment(Align.End)).Column(2));

    MenuBar BuildMenu()
    {
        var file = new MenuItem("File").Items(
        [
            new MenuItem("Connections", () => this.GoHome()),
            new MenuItem("Import / export connections", () => this.Push(new Screens.ConnectionTransferScreen(this))),
            MenuItem.CreateSeparator(),
            new MenuItem("Quit", () => this.ExitRequested.Value = true)
        ]);

        var view = new MenuItem("View").Items(
        [
            new MenuItem("Refresh", () => this.Reload()),
            new MenuItem("Toggle theme", () => this.ToggleTheme()),
            new MenuItem("Command palette", () => this.ShowPalette())
        ]);

        var help = new MenuItem("Help").Items(
        [
            new MenuItem("Keyboard shortcuts", () => this.ShowHelp()),
            new MenuItem("About", () => this.ShowAbout())
        ]);

        return new MenuBar().Items([file, view, help]);
    }

    void RegisterGlobalCommands()
    {
        this.root.AddCommand(new Command
        {
            Id = "shell.palette",
            Name = "Command palette",
            LabelMarkup = "Command palette",
            Gesture = new KeyGesture(TerminalChar.CtrlP, TerminalModifiers.Ctrl),
            Execute = _ => this.ShowPalette()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.connections",
            Name = "Connections",
            LabelMarkup = "Go to connections",
            SearchText = "connections home profiles databases",
            Execute = _ => this.GoHome()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.transfer",
            Name = "Import / export connections",
            LabelMarkup = "Import / export connections",
            SearchText = "bundle export import transfer settings move",
            Execute = _ => this.Push(new Screens.ConnectionTransferScreen(this))
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.explorer",
            Name = "Focus the explorer",
            LabelMarkup = "Focus the explorer",
            SearchText = "tree connections tables types navigate",
            Gesture = new KeyGesture(TerminalKey.F2),
            Execute = _ => this.FocusExplorer()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.refresh",
            Name = "Refresh",
            LabelMarkup = "Refresh",
            Gesture = new KeyGesture(TerminalKey.F5),
            Execute = _ => this.Reload()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.back",
            Name = "Back",
            LabelMarkup = "Back",
            Gesture = new KeyGesture(TerminalKey.Escape),
            CanExecute = _ => this.stack.Count > 1,

            // Escape has to keep reaching text editors and popups that are using it; only claim it
            // when there is somewhere to go back to.
            ConsumesGestureWhenUnavailable = false,
            Execute = _ => this.Pop()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.theme",
            Name = "Toggle theme",
            LabelMarkup = "Toggle light / dark theme",
            Execute = _ => this.ToggleTheme()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.help",
            Name = "Keyboard shortcuts",
            LabelMarkup = "Keyboard shortcuts",
            Gesture = new KeyGesture(TerminalKey.F1),
            Execute = _ => this.ShowHelp()
        });

        this.root.AddCommand(new Command
        {
            Id = "shell.quit",
            Name = "Quit",
            LabelMarkup = "Quit",
            Gesture = new KeyGesture(TerminalChar.CtrlQ, TerminalModifiers.Ctrl),
            Execute = _ => this.ExitRequested.Value = true
        });
    }

    /// <summary>
    /// The palette's contents: the shell's own commands plus whatever the screen on top contributes.
    /// Gathered at open time rather than registered once, so a screen's commands appear while it is
    /// the screen you are looking at and not before.
    /// </summary>
    void ShowPalette()
        => Widgets.CommandPaletteDialog.Open(this, [.. this.root.Commands, .. this.Current.Commands()]);

    // ── Navigation ──────────────────────────────────────────────────────

    public void Push(Screen screen)
    {
        this.stack.Add(screen);
        this.navigation.Value++;
        this.Status.Value = "";
        this.FocusInto(screen);
        this.Start(screen);
    }

    public void Pop()
    {
        if (this.stack.Count <= 1)
            return;

        this.stack.RemoveAt(this.stack.Count - 1);
        this.navigation.Value++;
        this.Status.Value = "";
    }

    /// <summary>Replaces everything above the connection list - "go home", but keeping the home instance.</summary>
    public void GoHome()
    {
        if (this.stack.Count == 1)
        {
            this.Reload();
        }
        else
        {
            this.stack.RemoveRange(1, this.stack.Count - 1);
            this.navigation.Value++;
            this.Status.Value = "";
        }
    }

    /// <summary>Swaps the top of the stack, so a workspace tab change does not deepen the breadcrumb.</summary>
    public void Replace(Screen screen)
    {
        this.stack[^1] = screen;
        this.navigation.Value++;
        this.Status.Value = "";
        this.FocusInto(screen);
        this.Start(screen);
    }

    public void Reload() => this.Start();

    /// <summary>Refreshes the tree alone, after a change that altered the connection list.</summary>
    public void ReloadExplorer() => this.explorer.Reload();

    /// <summary>Opens every connection in the tree - what the documentation images want to show.</summary>
    public void ExpandExplorer() => this.explorer.ExpandAll();

    /// <summary>
    /// Puts the cursor in the explorer.
    /// </summary>
    /// <remarks>
    /// The tree is the primary way around this app, and tabbing to it means counting controls in a
    /// toolbar that changes per screen. One key that always lands there is the difference between a
    /// keyboard-first interface and one you have to feel your way around.
    /// </remarks>
    public void FocusExplorer() => this.app?.Focus(this.explorer.Tree);

    /// <summary>
    /// Moves the cursor into a screen that has just been opened.
    /// </summary>
    /// <remarks>
    /// Not a nicety: a command's gesture is routed up from the focused element, so a screen's own
    /// shortcuts are unreachable while the focus is still in the tree that opened it. Pressing Enter on
    /// a type and then finding that the workspace does not answer its own keys is the bug this avoids.
    /// </remarks>
    void FocusInto(Screen screen)
    {
        if (this.app is not { } live)
            return;

        var target = screen.View
            .EnumerateVisualsDepthFirst()
            .FirstOrDefault(v => v is { Focusable: true, IsTabStop: true, IsVisible: true });

        if (target is not null)
            live.Focus(target);
    }

    void Start(Screen screen) => this.RunAsync(screen.Load, $"Could not open {screen.Title}");

    // ── Threading ───────────────────────────────────────────────────────

    /// <summary>Marshals onto the render thread. Every assignment to a <see cref="State{T}"/> goes through this.</summary>
    public void Post(Action action)
    {
        // Before the loop starts there is no dispatcher and no renderer to race, so run it here -
        // that is the path the render tests and the screenshot harness take.
        if (this.app is null)
            action();
        else
            this.app.Post(action);
    }

    /// <summary>
    /// Runs database work off the render thread and reports failures as a toast rather than a crash.
    /// </summary>
    /// <remarks>
    /// An admin tool spends its life talking to databases that are down, mistyped or read-only, so a
    /// failed operation is an ordinary event and has to look like one. The exception text goes to the
    /// toast and the full exception to the log file.
    /// </remarks>
    public void RunAsync(Func<CancellationToken, Task> work, string? failureContext = null)
    {
        this.Post(() => this.Pending.Value = ++this.pending);

        _ = Task.Run(async () =>
        {
            try
            {
                await work(this.Lifetime);
            }
            catch (OperationCanceledException) when (this.Lifetime.IsCancellationRequested)
            {
                // Shutting down. Nothing to report to a UI that is going away.
            }
            catch (Exception ex)
            {
                // Both, not either: the toast is what the person sees and it scrolls away, the log is
                // what is left to read afterwards - and a stack trace has nowhere to go on screen.
                this.log.LogError(ex, "{Context}", failureContext ?? "Background work failed");

                var message = failureContext is null ? ex.Message : $"{failureContext}: {ex.Message}";
                this.Post(() => this.Error(message));
            }
            finally
            {
                this.Post(() => this.Pending.Value = --this.pending);
            }
        });
    }

    internal void Attach(TerminalApp terminalApp) => this.app = terminalApp;

    /// <summary>
    /// Waits until nothing is loading. For the render tests and the documentation harness, which have
    /// no input loop to wait on and need the data to have arrived before they look at the screen.
    /// </summary>
    /// <remarks>
    /// Polling rather than a completion signal because the thing being waited for is "all of it",
    /// including work a finishing load started - a panel's load posts the next one, and a single
    /// awaitable would return between the two.
    /// </remarks>
    public async Task WaitForIdle(TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (Volatile.Read(ref this.pending) > 0)
        {
            if (DateTimeOffset.UtcNow > deadline)
                throw new TimeoutException($"{this.pending} load(s) were still running after {timeout}.");

            await Task.Delay(15);
        }
    }

    public void Shutdown() => this.lifetime.Cancel();

    // ── Notifications and dialogs ───────────────────────────────────────

    public void Info(string message) => this.toasts.Show(new Markup(Escape(message)), ToastSeverity.Info);
    public void Success(string message) => this.toasts.Show(new Markup(Escape(message)), ToastSeverity.Success);
    public void Warn(string message) => this.toasts.Show(new Markup(Escape(message)), ToastSeverity.Warning);
    public void Error(string message) => this.toasts.Show(new Markup(Escape(message)), ToastSeverity.Error);

    /// <summary>Database errors carry SQL, and SQL is full of brackets the markup parser would eat.</summary>
    static string Escape(string text) => text.Replace("[", "[[").Replace("]", "]]");

    public void OpenDialog(Visual dialog) => this.windows.AddWindow(dialog);

    public void CloseDialog(Visual dialog) => this.windows.RemoveWindow(dialog);

    void ToggleTheme()
    {
        var light = !ReferenceEquals(this.theme.Value, Theme.DefaultLight);
        this.theme.Value = light ? Theme.DefaultLight : Theme.Default;
        this.Settings.Theme = light ? "light" : "dark";
        this.Settings.Save();
    }

    void ShowHelp() => this.OpenDialog(Widgets.HelpDialog.Create(this));

    void ShowAbout() => this.OpenDialog(Widgets.AboutDialog.Create(this));
}
