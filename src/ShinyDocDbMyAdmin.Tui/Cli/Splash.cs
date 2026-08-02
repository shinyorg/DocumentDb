using System.Diagnostics;
using System.Text;
using XenoAtom.Terminal;

namespace ShinyDocDbMyAdmin.Tui.Cli;

/// <summary>
/// The Shiny mark, drawn once on startup before the UI takes the screen.
/// </summary>
/// <remarks>
/// <para>
/// Written straight to the terminal rather than built as a <c>Visual</c>: this lands on the normal screen
/// buffer, before <see cref="Terminal.RunAsync(XenoAtom.Terminal.UI.Visual,System.Func{XenoAtom.Terminal.UI.TerminalLoopContext,XenoAtom.Terminal.UI.TerminalLoopResult},XenoAtom.Terminal.UI.TerminalRunOptions,System.Threading.CancellationToken)"/>
/// switches to the alternate one, so it costs the app no frame and no key - it scrolls back into the
/// session's history when the tool exits, the way a CLI banner does.
/// </para>
/// <para>
/// The logo is the 64px mark quantised to twelve colours and half a character cell per pixel row, which is
/// the most a terminal can say about a circle. Everything about capability - truecolor, 256, 16, none, and
/// enabling VT on Windows - is <see cref="Terminal"/>'s problem: markup written to a 16-colour terminal is
/// degraded rather than dumped as escape codes, and a redirected stdout is not interactive, so it is skipped
/// entirely along with <c>--no-splash</c> and <c>NO_COLOR</c>.
/// </para>
/// </remarks>
public static class Splash
{
    /// <summary>
    /// How long the mark is guaranteed to stay up. Building the container and opening the profile store
    /// happen underneath it, so most runs wait for a fraction of this and some for none of it.
    /// </summary>
    static readonly TimeSpan OnScreen = TimeSpan.FromMilliseconds(650);

    /// <summary>Indexed by the letters used in <see cref="Pixels"/>, <c>a</c> first.</summary>
    static readonly string[] Palette =
    [
        "#FFEE6A", // a
        "#FFB646", // b
        "#CFCF81", // c
        "#6BEE6C", // d
        "#F78991", // e
        "#BA85C4", // f
        "#B15FE1", // g
        "#935CFA", // h
        "#AF56E7", // i
        "#B941ED", // j
        "#8957F5", // k
        "#7F3FF2"  // l
    ];

    /// <summary>One character per pixel, two rows per line of output. A space is transparent.</summary>
    static readonly string[] Pixels =
    [
        "          hh            ",
        "       hhkkkkhhh  bbbb  ",
        "     hh        hkebbbbb ",
        "    hh  eeeegg   ebbbbbb",
        "   h  eeeeeegggj bbbbbbb",
        "  hh eeeeeefggjjj bbbbb ",
        "  h eeeeeeefggfffjbbbb  ",
        " h  eeeeeeffgjfcfih g   ",
        " h eeeeeecdddggffii kh  ",
        "kk eeeeeddddddgiiih  h  ",
        "h  beeecdddddddiihhh k  ",
        "h  bbeecdddddddgihhh h  ",
        "h  bbbecdddddddghhhh h  ",
        "h  bbbecdddddddihhhh k  ",
        "kk bbbecddddddfiihh  h  ",
        " h aaaaccddddfiiihh hh  ",
        " h aaaaaecccfggiiih h   ",
        "  faaaaaceffggiiih hh   ",
        "  faaaaaeeffggiii  h    ",
        "   aaaaaeeffggii  h     ",
        "    aaa eeffgg   hh     ",
        "     kl        hh       ",
        "      hhhhhhhhhh        ",
        "         hhhh           "
    ];

    /// <summary>The line the caption starts on, which centres four lines against twelve of logo.</summary>
    const int CaptionTop = 4;

    const int Margin = 2;

    /// <summary>
    /// Draws the mark, or does nothing when the terminal is not one a logo means anything on.
    /// </summary>
    /// <returns>
    /// A timestamp to hand to <see cref="Settle"/>, or null when nothing was drawn.
    /// </returns>
    public static long? Show(CommandLineOptions options)
    {
        // NO_COLOR is checked here rather than left to the terminal's own handling of it: without colour
        // the mark is a block of grey squares, so the answer is not a monochrome logo but no logo.
        if (options.NoSplash
            || !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("NO_COLOR"))
            || !Terminal.IsInteractive
            || Terminal.Capabilities.ColorLevel == TerminalColorLevel.None)
            return null;

        // The caption only earns its place when the window is wide enough to keep it beside the mark.
        // Under it, the banner would be taller than the connection list it is covering for.
        Terminal.WriteLine("");
        foreach (var line in Render(Terminal.WindowWidth >= Margin + Pixels[0].Length + 2 + 42))
            Terminal.WriteMarkupLine(line);
        Terminal.WriteLine("");
        Terminal.Flush();

        return Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// The banner as markup, one string per line of output - separated from <see cref="Show"/> so it can
    /// be built without a terminal to write it to.
    /// </summary>
    public static IReadOnlyList<string> Render(bool withCaption)
    {
        var caption = withCaption ? Caption() : [];
        var width = Pixels[0].Length;
        var lines = new List<string>(Pixels.Length / 2);

        for (var line = 0; line < Pixels.Length / 2; line++)
        {
            var top = Pixels[line * 2];
            var bottom = Pixels[line * 2 + 1];
            var markup = new StringBuilder().Append(' ', Margin);

            for (var x = 0; x < width; x++)
                markup.Append(Cell(top[x], bottom[x]));

            var text = line - CaptionTop;
            if (text >= 0 && text < caption.Length)
                markup.Append("  ").Append(caption[text]);

            lines.Add(markup.ToString());
        }

        return lines;
    }

    /// <summary>
    /// Holds the mark on screen for whatever is left of <see cref="OnScreen"/> after startup, so a fast
    /// machine still sees it and a slow one is not made slower.
    /// </summary>
    public static async Task Settle(long? shownAt)
    {
        if (shownAt is not { } start)
            return;

        var remaining = OnScreen - Stopwatch.GetElapsedTime(start);
        if (remaining > TimeSpan.Zero)
            await Task.Delay(remaining);
    }

    static string[] Caption() =>
    [
        "[bold]ShinyDocDbMyAdmin[/]",
        "[dim]a terminal front end for Shiny.DocumentDb[/]",
        $"[dim]{AppInfo.Version}[/]",
        $"[#935CFA]{AppInfo.Site}[/]"
    ];

    /// <summary>
    /// Two pixels in one cell: the upper half block painted in the top pixel's colour over the bottom
    /// pixel's as background, which is how a terminal draws a square pixel.
    /// </summary>
    static string Cell(char top, char bottom) => (top, bottom) switch
    {
        (' ', ' ') => " ",
        (' ', _) => $"[{Palette[bottom - 'a']}]▄[/]",
        (_, ' ') => $"[{Palette[top - 'a']}]▀[/]",
        _ => $"[{Palette[top - 'a']} on {Palette[bottom - 'a']}]▀[/]"
    };
}
