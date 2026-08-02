using System.Globalization;
using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Geometry;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// The small vocabulary every panel is written in - headings, muted notes, empty states, toolbars,
/// and the formatters that make numbers and byte counts read the same everywhere.
/// </summary>
/// <remarks>
/// The equivalent of the web front end's <c>app.css</c>: not a component library, just the handful of
/// shapes that would otherwise be spelled out slightly differently in each of the eleven panels.
/// </remarks>
public static class Ui
{
    /// <summary>
    /// Escapes text for a <see cref="Markup"/>. Anything read out of a database goes through this -
    /// document ids, error text and SQL are all full of brackets the markup parser would swallow.
    /// </summary>
    public static string Escape(string? text)
        => (text ?? "").Replace("[", "[[").Replace("]", "]]");

    public static Visual Title(string text) => new Markup($"[bold]{Escape(text)}[/]");

    /// <summary>
    /// An explanatory line. Wraps: these are sentences, and a terminal narrow enough to cut one in
    /// half is exactly the terminal where the explanation matters.
    /// </summary>
    public static Visual Muted(string text) => new Markup($"[dim]{Escape(text)}[/]").Wrap(true);

    public static Visual Label(string text) => new Markup($"[dim]{Escape(text)}[/]");

    /// <summary>A bordered section with a heading, the terminal answer to the web front end's cards.</summary>
    public static Group Panel(string title, Visual content)
        => new Group(new Markup($"[bold]{Escape(title)}[/]"), content).Padding(new Thickness(1, 0, 1, 0));

    /// <summary>
    /// Nothing at all, and no row spent on it. An empty <c>TextBlock</c> still measures one line, which
    /// a panel with three conditional messages would pay for three times over whether or not any of
    /// them had something to say.
    /// </summary>
    public static Visual Nothing() => new VStack();

    /// <summary>The "nothing here" state, which in an admin tool is a normal state and not an error.</summary>
    public static Visual Empty(string message)
        => new Center(new Markup($"[dim]{Escape(message)}[/]"));

    public static Visual Warning(string message)
        => new Markup($"[yellow]! [/][dim]{Escape(message)}[/]").Wrap(true);

    public static Visual Failure(string message)
        => new Markup($"[red]✗ [/]{Escape(message)}").Wrap(true);

    /// <summary>
    /// A row of buttons and inputs across the top of a panel.
    /// </summary>
    /// <remarks>
    /// Wraps rather than clips. A plain row squeezes its children when the terminal is narrow, and a
    /// button squeezed to "Refres" is worse than a toolbar two lines tall - especially since the width
    /// here is whatever window someone happened to open.
    /// </remarks>
    public static WrapHStack Toolbar(params Visual[] items) => new WrapHStack().Children(items).Spacing(1);

    /// <summary>
    /// A text box over a state this app only ever puts a string in.
    /// </summary>
    /// <remarks>
    /// The controls bind <c>Binding&lt;string?&gt;</c> because their own <c>Text</c> is nullable. Nothing
    /// here ever assigns null - an emptied box reads as <c>""</c> - so the conversion is a signature
    /// mismatch rather than a value one, and it is done once here instead of at forty call sites.
    /// Read the other way with <see cref="Typed"/>, which is where the nullable half is honoured.
    /// </remarks>
    public static TextBox Input(State<string> value) => new(value.Bind.Value!);

    public static TextArea Area(State<string> value) => new(value.Bind.Value!);

    public static CodeEditor Code(State<string> value) => new(value.Bind.Value!);

    /// <summary>What is in a text box, never null.</summary>
    public static string Typed(this TextBox box) => box.Text ?? "";

    public static Button Action(string text, Action onClick)
        => new Button(new TextBlock(text)).Click(onClick);

    public static Button Primary(string text, Action onClick)
        => new Button(new TextBlock(text)).Tone(XenoAtom.Terminal.UI.Styling.ControlTone.Primary).Click(onClick);

    public static Button Danger(string text, Action onClick)
        => new Button(new TextBlock(text)).Tone(XenoAtom.Terminal.UI.Styling.ControlTone.Error).Click(onClick);

    /// <summary>A two-column label/value block, used for every statistics and detail panel.</summary>
    public static Visual Facts(params (string Label, string Value)[] rows)
    {
        var stack = new VStack();
        foreach (var (label, value) in rows)
        {
            stack.Add(new HStack(
                new Markup($"[dim]{Escape(label)}[/]").MinWidth(22),
                new Markup(Escape(value))
            ).Spacing(1));
        }
        return stack;
    }

    // ── Formatting ──────────────────────────────────────────────────────

    public static string Number(long value) => value.ToString("N0", CultureInfo.CurrentCulture);

    public static string Bytes(long? value)
    {
        if (value is not { } bytes)
            return "—";

        string[] units = ["B", "KB", "MB", "GB", "TB"];
        double size = bytes;
        var unit = 0;
        while (size >= 1024 && unit < units.Length - 1)
        {
            size /= 1024;
            unit++;
        }

        return unit == 0
            ? $"{bytes} B"
            : string.Create(CultureInfo.CurrentCulture, $"{size:0.#} {units[unit]}");
    }

    public static string Timestamp(DateTimeOffset? value)
        => value is { } when ? when.LocalDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.CurrentCulture) : "—";

    public static string Duration(TimeSpan value)
        => value.TotalMilliseconds < 1000
            ? string.Create(CultureInfo.CurrentCulture, $"{value.TotalMilliseconds:0} ms")
            : string.Create(CultureInfo.CurrentCulture, $"{value.TotalSeconds:0.00} s");

    /// <summary>
    /// One line of a value, cut to fit a grid cell. Newlines become spaces first: a JSON body wrapped
    /// across three lines turns a fixed-height row into a broken one.
    /// </summary>
    public static string Cell(string? text, int max = 120)
    {
        if (string.IsNullOrEmpty(text))
            return "";

        var flat = text.ReplaceLineEndings(" ").Trim();
        return flat.Length <= max ? flat : flat[..(max - 1)] + "…";
    }
}
