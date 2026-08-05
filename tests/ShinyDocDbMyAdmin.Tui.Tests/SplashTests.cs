using System.Diagnostics;
using System.Text.RegularExpressions;
using ShinyDocDbMyAdmin.Services;
using ShinyDocDbMyAdmin.Tui.Cli;

namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// The startup banner. It is markup written straight at the terminal rather than a <c>Visual</c>, so
/// nothing else checks that it parses - a stray bracket in the pixel data would print as a bracket, and
/// a colour letter with no palette entry would take the tool down before it drew a thing.
/// </summary>
public sealed class SplashTests
{
    [Fact]
    public void The_mark_is_half_a_cell_per_pixel_row()
    {
        var lines = Splash.Render(false);

        // Twelve lines for twenty-four pixel rows. A banner that grows past a small terminal's height
        // would scroll the prompt away, which is not what a splash is for.
        Assert.Equal(12, lines.Count);
        Assert.All(lines, line => Assert.NotEqual("", line.Trim()));
    }

    [Fact]
    public void Every_line_is_markup_that_closes_what_it_opens()
    {
        foreach (var line in Splash.Render(true))
        {
            var opens = Regex.Matches(line, @"\[(?!/)[^\]]+\]").Count;
            var closes = Regex.Matches(line, @"\[/\]").Count;

            Assert.Equal(opens, closes);

            // Nothing outside a tag: the whole line is generated, so an unescaped bracket could only be
            // a mistake in the pixel data or the caption.
            Assert.DoesNotContain("[", Regex.Replace(line, @"\[[^\]]*\]", ""));
        }
    }

    [Fact]
    public void The_caption_names_the_tool_and_links_the_site()
    {
        var text = string.Join("\n", Splash.Render(true));

        Assert.Contains("ShinyDocDbMyAdmin", text);
        Assert.Contains(AppInfo.Site, text);
        Assert.Contains(AppInfo.Version, text);
    }

    [Fact]
    public void A_narrow_terminal_gets_the_mark_alone()
    {
        var text = string.Join("\n", Splash.Render(false));

        Assert.DoesNotContain("ShinyDocDbMyAdmin", text);
        Assert.DoesNotContain(AppInfo.Site, text);
    }

    [Fact]
    public async Task Nothing_drawn_means_nothing_waited_for()
    {
        var watch = Stopwatch.StartNew();
        await Splash.Settle(null);

        Assert.True(watch.ElapsedMilliseconds < 100);
    }

    [Fact]
    public void The_version_is_the_informational_one_without_its_build_metadata()
    {
        Assert.NotEqual("unknown", AppInfo.Version);
        Assert.DoesNotContain("+", AppInfo.Version);
    }
}
