using System.Globalization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A compact recursive-descent parser for the grouped string grammar (projection field lists and HAVING
/// predicates). Deliberately self-contained — it recognizes the aggregate functions
/// (<c>count/sum/avg/min/max</c>), dotted field paths, string/number literals, comparison operators, and
/// <c>and</c>/<c>or</c>/<c>not</c> — so it never risks the main <see cref="FilterExpressionParser"/> grammar.
/// </summary>
sealed class GroupGrammarParser
{
    public readonly record struct ProjectionItem(string? Function, string? Argument, string? FieldPath, string? Alias);

    public enum OperandKind { Aggregate, Field, Literal }
    public readonly record struct Operand(OperandKind Kind, string? Function, string? Argument, string? Text, object? Literal);

    static readonly HashSet<string> AggFns = new(StringComparer.OrdinalIgnoreCase) { "count", "sum", "avg", "min", "max" };

    readonly List<Token> tokens;
    int pos;

    public GroupGrammarParser(string input)
    {
        this.tokens = Tokenize(input);
    }

    Token Cur => this.tokens[this.pos];

    // ── projection: item (',' item)* ──
    public List<ProjectionItem> ParseProjection()
    {
        var items = new List<ProjectionItem> { this.ParseProjectionItem() };
        while (this.Cur.Kind == TokKind.Comma)
        {
            this.pos++;
            items.Add(this.ParseProjectionItem());
        }
        this.Expect(TokKind.End);
        return items;
    }

    ProjectionItem ParseProjectionItem()
    {
        if (this.Cur.Kind != TokKind.Ident)
            throw Err($"Expected a field or aggregate, found '{this.Cur.Text}'");

        // aggregate function?
        if (this.Peek().Kind == TokKind.LParen && AggFns.Contains(this.Cur.Text))
        {
            var (fn, arg) = this.ParseAggregate();
            return new ProjectionItem(fn, arg, null, this.ParseAlias());
        }

        var field = this.ParseFieldPath();
        return new ProjectionItem(null, null, field, this.ParseAlias());
    }

    (string Func, string? Arg) ParseAggregate()
    {
        var fn = this.Cur.Text;
        this.pos++;
        this.Expect(TokKind.LParen);
        string? arg = null;
        if (this.Cur.Kind == TokKind.Ident)
            arg = this.ParseFieldPath();
        this.Expect(TokKind.RParen);
        if (fn.Equals("count", StringComparison.OrdinalIgnoreCase) && arg != null)
            throw Err("count() takes no argument.");
        if (!fn.Equals("count", StringComparison.OrdinalIgnoreCase) && arg == null)
            throw Err($"{fn}() requires a field argument.");
        return (fn, arg);
    }

    string ParseFieldPath()
    {
        var sb = new System.Text.StringBuilder(this.Cur.Text);
        this.pos++;
        while (this.Cur.Kind == TokKind.Dot)
        {
            this.pos++;
            if (this.Cur.Kind != TokKind.Ident)
                throw Err("Expected an identifier after '.'");
            sb.Append('.').Append(this.Cur.Text);
            this.pos++;
        }
        return sb.ToString();
    }

    string? ParseAlias()
    {
        if (this.Cur.Kind == TokKind.Ident && this.Cur.Text.Equals("as", StringComparison.OrdinalIgnoreCase))
        {
            this.pos++;
            if (this.Cur.Kind != TokKind.Ident)
                throw Err("Expected an alias after 'as'.");
            var alias = this.Cur.Text;
            this.pos++;
            return alias;
        }
        return null;
    }

    // ── having: or ──
    public string ParseHaving(Func<Operand, string> emitOperand)
    {
        var sql = this.ParseOr(emitOperand);
        this.Expect(TokKind.End);
        return sql;
    }

    string ParseOr(Func<Operand, string> emit)
    {
        var left = this.ParseAnd(emit);
        while (this.IsKeyword("or"))
        {
            this.pos++;
            left = $"({left} OR {this.ParseAnd(emit)})";
        }
        return left;
    }

    string ParseAnd(Func<Operand, string> emit)
    {
        var left = this.ParseNot(emit);
        while (this.IsKeyword("and"))
        {
            this.pos++;
            left = $"({left} AND {this.ParseNot(emit)})";
        }
        return left;
    }

    string ParseNot(Func<Operand, string> emit)
    {
        if (this.IsKeyword("not"))
        {
            this.pos++;
            return $"NOT ({this.ParseNot(emit)})";
        }
        if (this.Cur.Kind == TokKind.LParen)
        {
            this.pos++;
            var inner = this.ParseOr(emit);
            this.Expect(TokKind.RParen);
            return $"({inner})";
        }
        return this.ParseComparison(emit);
    }

    string ParseComparison(Func<Operand, string> emit)
    {
        var left = emit(this.ParseOperand());
        if (this.Cur.Kind != TokKind.Operator)
            throw Err($"Expected a comparison operator, found '{this.Cur.Text}'");
        var op = NormalizeOp(this.Cur.Text);
        this.pos++;
        var right = emit(this.ParseOperand());
        return $"{left} {op} {right}";
    }

    Operand ParseOperand()
    {
        if (this.Cur.Kind == TokKind.String)
        {
            var v = this.Cur.Text;
            this.pos++;
            return new Operand(OperandKind.Literal, null, null, null, v);
        }
        if (this.Cur.Kind == TokKind.Number)
        {
            var raw = this.Cur.Text;
            this.pos++;
            object num = raw.Contains('.')
                ? double.Parse(raw, CultureInfo.InvariantCulture)
                : long.Parse(raw, CultureInfo.InvariantCulture);
            return new Operand(OperandKind.Literal, null, null, null, num);
        }
        if (this.Cur.Kind == TokKind.Ident)
        {
            if (this.Peek().Kind == TokKind.LParen && AggFns.Contains(this.Cur.Text))
            {
                var (fn, arg) = this.ParseAggregate();
                return new Operand(OperandKind.Aggregate, fn, arg, null, null);
            }
            var lower = this.Cur.Text.ToLowerInvariant();
            if (lower is "true" or "false")
            {
                this.pos++;
                return new Operand(OperandKind.Literal, null, null, null, lower == "true" ? 1L : 0L);
            }
            var field = this.ParseFieldPath();
            return new Operand(OperandKind.Field, null, null, field, null);
        }
        throw Err($"Unexpected '{this.Cur.Text}' in HAVING.");
    }

    static string NormalizeOp(string op) => op switch
    {
        "==" or "=" => "=",
        "!=" or "<>" => "<>",
        ">" => ">",
        "<" => "<",
        ">=" => ">=",
        "<=" => "<=",
        _ => throw new ArgumentException($"Unsupported operator '{op}'.")
    };

    bool IsKeyword(string kw) => this.Cur.Kind == TokKind.Ident && this.Cur.Text.Equals(kw, StringComparison.OrdinalIgnoreCase);

    Token Peek() => this.pos + 1 < this.tokens.Count ? this.tokens[this.pos + 1] : this.tokens[^1];

    void Expect(TokKind kind)
    {
        if (this.Cur.Kind != kind)
            throw Err($"Expected {kind}, found '{this.Cur.Text}'");
        if (kind != TokKind.End)
            this.pos++;
    }

    static ArgumentException Err(string message) => new(message);

    // ── lexer ──
    enum TokKind { Ident, Number, String, Operator, LParen, RParen, Comma, Dot, End }
    readonly record struct Token(TokKind Kind, string Text);

    static List<Token> Tokenize(string input)
    {
        var tokens = new List<Token>();
        var i = 0;
        while (i < input.Length)
        {
            var c = input[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }
            switch (c)
            {
                case '(': tokens.Add(new Token(TokKind.LParen, "(")); i++; continue;
                case ')': tokens.Add(new Token(TokKind.RParen, ")")); i++; continue;
                case ',': tokens.Add(new Token(TokKind.Comma, ",")); i++; continue;
                case '.': tokens.Add(new Token(TokKind.Dot, ".")); i++; continue;
                case '\'':
                {
                    var sb = new System.Text.StringBuilder();
                    i++;
                    while (i < input.Length && input[i] != '\'')
                        sb.Append(input[i++]);
                    if (i >= input.Length)
                        throw Err("Unterminated string literal.");
                    i++; // closing quote
                    tokens.Add(new Token(TokKind.String, sb.ToString()));
                    continue;
                }
            }
            if (c is '=' or '!' or '<' or '>')
            {
                var start = i++;
                if (i < input.Length && input[i] == '=')
                    i++;
                else if (c == '<' && i < input.Length && input[i] == '>')
                    i++;
                tokens.Add(new Token(TokKind.Operator, input[start..i]));
                continue;
            }
            if (char.IsDigit(c) || (c == '-' && i + 1 < input.Length && char.IsDigit(input[i + 1])))
            {
                var start = i++;
                while (i < input.Length && (char.IsDigit(input[i]) || input[i] == '.'))
                    i++;
                tokens.Add(new Token(TokKind.Number, input[start..i]));
                continue;
            }
            if (char.IsLetter(c) || c == '_')
            {
                var start = i++;
                while (i < input.Length && (char.IsLetterOrDigit(input[i]) || input[i] == '_'))
                    i++;
                tokens.Add(new Token(TokKind.Ident, input[start..i]));
                continue;
            }
            throw Err($"Unexpected character '{c}'.");
        }
        tokens.Add(new Token(TokKind.End, ""));
        return tokens;
    }
}
