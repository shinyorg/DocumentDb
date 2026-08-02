using XenoAtom.Terminal.UI;
using XenoAtom.Terminal.UI.Controls;
using XenoAtom.Terminal.UI.Styling;
using XenoAtom.Terminal.UI.Text;

namespace ShinyDocDbMyAdmin.Tui.Widgets;

/// <summary>
/// SQL colouring for the SQL console.
/// </summary>
/// <remarks>
/// One keyword list for nine dialects. That is deliberate: the console runs whatever you type against
/// whichever backend the connection points at, and a per-dialect keyword set would colour the same
/// statement differently depending on where it was aimed. The list is the common core plus the words
/// DocumentDb's own generated SQL uses, so the statements this tool produces read correctly
/// everywhere; an unrecognised keyword loses its colour and nothing else.
/// </remarks>
public sealed class SqlSyntaxHighlighter : CodeEditorSyntaxHighlighter
{
    public static readonly SqlSyntaxHighlighter Instance = new();

    static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "select", "from", "where", "and", "or", "not", "in", "is", "null", "as", "on",
        "join", "inner", "left", "right", "full", "outer", "cross", "lateral",
        "group", "by", "having", "order", "asc", "desc", "limit", "offset", "fetch", "next", "rows", "only",
        "insert", "into", "values", "update", "set", "delete", "returning",
        "create", "table", "index", "unique", "drop", "alter", "add", "column", "constraint", "primary", "key",
        "distinct", "union", "all", "except", "intersect", "case", "when", "then", "else", "end",
        "exists", "between", "like", "ilike", "escape", "cast", "with", "recursive",
        "count", "sum", "avg", "min", "max", "coalesce", "nullif", "explain", "analyze", "plan", "for",
        "begin", "commit", "rollback", "transaction", "true", "false"
    };

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
        var keywordStyle = Style.None.WithForeground(theme.Primary ?? Colors.Cyan).WithTextStyle(TextStyle.Bold);
        var stringStyle = Style.None.WithForeground(theme.Success ?? Colors.Green);
        var numberStyle = Style.None.WithForeground(theme.Warning ?? Colors.Yellow);
        var parameterStyle = Style.None.WithForeground(theme.Accent ?? Colors.Magenta);
        var commentStyle = Style.None.WithForeground(theme.Muted ?? Colors.Gray);

        var index = 0;
        while (index < length)
        {
            var c = buffer[index];

            // A line comment runs to the end of the line, so there is nothing after it to colour.
            if (c == '-' && index + 1 < length && buffer[index + 1] == '-')
            {
                runs.Add(new StyledRun(index, length - index, commentStyle));
                return;
            }

            if (c == '\'')
            {
                var start = index;
                index++;
                while (index < length)
                {
                    // '' is an escaped quote, not the end of the literal.
                    if (buffer[index] == '\'' && index + 1 < length && buffer[index + 1] == '\'')
                        index += 2;
                    else if (buffer[index] == '\'')
                    {
                        index++;
                        break;
                    }
                    else
                        index++;
                }

                index = Math.Min(index, length);
                runs.Add(new StyledRun(start, index - start, stringStyle));
            }
            else if (c is '@' or ':' && index + 1 < length && (char.IsAsciiLetter(buffer[index + 1]) || buffer[index + 1] == '_'))
            {
                var start = index;
                index++;
                while (index < length && (char.IsAsciiLetterOrDigit(buffer[index]) || buffer[index] == '_'))
                    index++;

                runs.Add(new StyledRun(start, index - start, parameterStyle));
            }
            else if (char.IsAsciiDigit(c))
            {
                var start = index;
                while (index < length && (char.IsAsciiDigit(buffer[index]) || buffer[index] == '.'))
                    index++;

                runs.Add(new StyledRun(start, index - start, numberStyle));
            }
            else if (char.IsAsciiLetter(c) || c == '_')
            {
                var start = index;
                while (index < length && (char.IsAsciiLetterOrDigit(buffer[index]) || buffer[index] == '_'))
                    index++;

                var word = new string(buffer[start..index]);
                if (Keywords.Contains(word))
                    runs.Add(new StyledRun(start, index - start, keywordStyle));
            }
            else
            {
                index++;
            }
        }
    }
}
