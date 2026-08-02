using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Styling;
using XenoAtom.Terminal.UI.Text;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// JSON colouring for the document editor and the query consoles.
/// </summary>
/// <remarks>
/// <para>
/// A hand-written line tokeniser rather than a grammar: the tool ships no TextMate grammars, JSON's
/// lexical structure is four token kinds, and this way the editor has no extra dependency and no
/// per-keystroke grammar pass. It is the same reasoning as the web front end's <c>JsonHtml</c>, which
/// also renders its own rather than shipping a highlighter library.
/// </para>
/// <para>
/// Lines are coloured independently. That is exact for JSON as this tool ever writes it - one value
/// per line, strings never spanning lines - and a string that somehow did span lines would lose its
/// colour rather than corrupt anything after it.
/// </para>
/// </remarks>
public sealed class JsonSyntaxHighlighter : CodeEditorSyntaxHighlighter
{
    public static readonly JsonSyntaxHighlighter Instance = new();

    /// <summary>
    /// There is no state to carry between lines, so the state object only records which snapshot it
    /// was built for. Everything this highlighter knows, it works out from the line in front of it.
    /// </summary>
    sealed class State(int snapshotVersion, long stamp) : CodeEditorSyntaxState
    {
        public override int SnapshotVersion { get; } = snapshotVersion;
        public override long CompatibilityStamp { get; } = stamp;
        public override bool IsComplete => true;
    }

    public override CodeEditorSyntaxState Build(in CodeEditorSyntaxBuildContext context)
        => new State(context.Snapshot.Version, this.GetCompatibilityStamp(context.Theme));

    public override CodeEditorSyntaxState Update(CodeEditorSyntaxState previousState, in CodeEditorSyntaxUpdateContext context)
        => new State(context.Snapshot.Version, this.GetCompatibilityStamp(context.Theme));

    // The scheme decides every colour this highlighter picks, so a theme change has to
    // invalidate the cached runs. A theme with none is a theme whose colours cannot change.
    public override long GetCompatibilityStamp(Theme theme) => theme.Scheme?.GetHashCode() ?? 0;

    public override void GetLineRuns(CodeEditorSyntaxState state, in CodeEditorLineSyntaxRequest request, List<StyledRun> runs)
    {
        var length = request.LineLength;
        if (length == 0)
            return;

        var buffer = length <= 512 ? stackalloc char[length] : new char[length];
        request.Snapshot.CopyTo(request.LineStart, buffer);

        var theme = request.Theme;
        var stringStyle = Style.None.WithForeground(theme.Success ?? Colors.Green);
        var keyStyle = Style.None.WithForeground(theme.Primary ?? Colors.Cyan);
        var numberStyle = Style.None.WithForeground(theme.Warning ?? Colors.Yellow);
        var literalStyle = Style.None.WithForeground(theme.Accent ?? Colors.Magenta);
        var punctuationStyle = Style.None.WithForeground(theme.Muted ?? Colors.Gray);

        var index = 0;
        while (index < length)
        {
            var c = buffer[index];

            if (c == '"')
            {
                var start = index;
                index++;
                while (index < length)
                {
                    if (buffer[index] == '\\')
                        index += 2;
                    else if (buffer[index] == '"')
                    {
                        index++;
                        break;
                    }
                    else
                        index++;
                }

                index = Math.Min(index, length);

                // A string followed by a colon is a property name, which is what you scan a document
                // for - so it gets its own colour rather than reading as another value.
                var after = index;
                while (after < length && char.IsWhiteSpace(buffer[after]))
                    after++;

                var isKey = after < length && buffer[after] == ':';
                runs.Add(new StyledRun(start, index - start, isKey ? keyStyle : stringStyle));
            }
            else if (char.IsAsciiDigit(c) || (c == '-' && index + 1 < length && char.IsAsciiDigit(buffer[index + 1])))
            {
                var start = index;
                index++;
                while (index < length && (char.IsAsciiDigit(buffer[index]) || buffer[index] is '.' or 'e' or 'E' or '+' or '-'))
                    index++;

                runs.Add(new StyledRun(start, index - start, numberStyle));
            }
            else if (char.IsAsciiLetter(c))
            {
                var start = index;
                while (index < length && char.IsAsciiLetter(buffer[index]))
                    index++;

                runs.Add(new StyledRun(start, index - start, literalStyle));
            }
            else if (c is '{' or '}' or '[' or ']' or ':' or ',')
            {
                runs.Add(new StyledRun(index, 1, punctuationStyle));
                index++;
            }
            else
            {
                index++;
            }
        }
    }
}
