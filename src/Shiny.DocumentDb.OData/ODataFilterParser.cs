using System.Globalization;

namespace Shiny.DocumentDb.OData;

/// <summary>
/// A small, dependency-free recursive-descent parser for the OData v4 <c>$filter</c> grammar subset
/// the engine supports: comparison (<c>eq ne gt ge lt le</c>), logical (<c>and or not</c>), arithmetic
/// (<c>add sub mul div mod</c>), the string functions (<c>contains startswith endswith tolower toupper
/// trim length indexof concat</c>) and date parts (<c>year month day …</c>), nested property paths
/// (<c>Address/City</c>), <c>null</c>, and OData string-literal quoting (<c>'O''Brien'</c>).
/// <para>
/// It produces the same provider-neutral <see cref="ODataFilterNode"/> tree the Microsoft parser
/// adapter produces in the host package, so there is exactly one <see cref="FilterExpressionBuilder"/>.
/// </para>
/// </summary>
public static class ODataFilterParser
{
    public static ODataFilterNode Parse(string filter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        var tokens = Tokenize(filter);
        var parser = new Parser(tokens, filter);
        var node = parser.ParseExpression();
        parser.ExpectEnd();
        return node;
    }

    // ── Tokenizer ───────────────────────────────────────────────────────────

    enum TokenType { Identifier, String, Number, OpenParen, CloseParen, Comma, End }

    readonly record struct Token(TokenType Type, string Text, object? Value);

    static List<Token> Tokenize(string input)
    {
        var tokens = new List<Token>();
        var i = 0;
        while (i < input.Length)
        {
            var c = input[i];
            if (char.IsWhiteSpace(c))
            {
                i++;
            }
            else if (c == '(')
            {
                tokens.Add(new Token(TokenType.OpenParen, "(", null));
                i++;
            }
            else if (c == ')')
            {
                tokens.Add(new Token(TokenType.CloseParen, ")", null));
                i++;
            }
            else if (c == ',')
            {
                tokens.Add(new Token(TokenType.Comma, ",", null));
                i++;
            }
            else if (c == '\'')
            {
                tokens.Add(ReadString(input, ref i));
            }
            else if (char.IsDigit(c) || (c == '-' && i + 1 < input.Length && char.IsDigit(input[i + 1])))
            {
                tokens.Add(ReadNumber(input, ref i));
            }
            else if (char.IsLetter(c) || c == '_')
            {
                tokens.Add(ReadIdentifier(input, ref i));
            }
            else
            {
                throw new FormatException($"Unexpected character '{c}' at position {i} in $filter.");
            }
        }
        tokens.Add(new Token(TokenType.End, "", null));
        return tokens;
    }

    static Token ReadString(string input, ref int i)
    {
        i++; // opening quote
        var sb = new System.Text.StringBuilder();
        while (i < input.Length)
        {
            var c = input[i];
            if (c == '\'')
            {
                if (i + 1 < input.Length && input[i + 1] == '\'')
                {
                    sb.Append('\''); // doubled-quote escape
                    i += 2;
                }
                else
                {
                    i++; // closing quote
                    return new Token(TokenType.String, sb.ToString(), sb.ToString());
                }
            }
            else
            {
                sb.Append(c);
                i++;
            }
        }
        throw new FormatException("Unterminated string literal in $filter.");
    }

    static Token ReadNumber(string input, ref int i)
    {
        var start = i;
        if (input[i] == '-') i++;
        var hasDot = false;
        while (i < input.Length && (char.IsDigit(input[i]) || input[i] == '.'))
        {
            if (input[i] == '.') hasDot = true;
            i++;
        }
        var text = input.Substring(start, i - start);
        object value;
        if (!hasDot && long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
            value = l;
        else
            value = double.Parse(text, CultureInfo.InvariantCulture);
        return new Token(TokenType.Number, text, value);
    }

    static Token ReadIdentifier(string input, ref int i)
    {
        var start = i;
        while (i < input.Length && (char.IsLetterOrDigit(input[i]) || input[i] == '_' || input[i] == '.' || input[i] == '/'))
            i++;
        var text = input.Substring(start, i - start);
        return new Token(TokenType.Identifier, text, null);
    }

    // ── Parser ──────────────────────────────────────────────────────────────

    static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "and", "or", "not", "eq", "ne", "gt", "ge", "lt", "le", "add", "sub", "mul", "div", "mod", "null", "true", "false"
    };

    static readonly HashSet<string> Functions = new(StringComparer.OrdinalIgnoreCase)
    {
        "contains", "startswith", "endswith", "tolower", "toupper", "trim", "length", "indexof", "concat",
        "year", "month", "day", "hour", "minute", "second"
    };

    sealed class Parser(List<Token> tokens, string source)
    {
        int pos;

        Token Current => tokens[pos];
        Token Next() => tokens[pos++];
        bool IsKeyword(string kw) => Current.Type == TokenType.Identifier && Current.Text.Equals(kw, StringComparison.OrdinalIgnoreCase);

        public void ExpectEnd()
        {
            if (Current.Type != TokenType.End)
                throw new FormatException($"Unexpected token '{Current.Text}' in $filter '{source}'.");
        }

        // expression := orExpr
        public ODataFilterNode ParseExpression() => ParseOr();

        ODataFilterNode ParseOr()
        {
            var left = ParseAnd();
            while (IsKeyword("or"))
            {
                Next();
                left = ODataFilterNode.Binary(ODataBinaryOperator.Or, left, ParseAnd());
            }
            return left;
        }

        ODataFilterNode ParseAnd()
        {
            var left = ParseComparison();
            while (IsKeyword("and"))
            {
                Next();
                left = ODataFilterNode.Binary(ODataBinaryOperator.And, left, ParseComparison());
            }
            return left;
        }

        ODataFilterNode ParseComparison()
        {
            if (IsKeyword("not"))
            {
                Next();
                return ODataFilterNode.Unary(ODataUnaryOperator.Not, ParseComparison());
            }

            var left = ParseAdditive();
            if (Current.Type == TokenType.Identifier && TryComparisonOp(Current.Text, out var op))
            {
                Next();
                var right = ParseAdditive();
                return ODataFilterNode.Binary(op, left, right);
            }
            return left;
        }

        static bool TryComparisonOp(string text, out ODataBinaryOperator op)
        {
            op = text.ToLowerInvariant() switch
            {
                "eq" => ODataBinaryOperator.Equal,
                "ne" => ODataBinaryOperator.NotEqual,
                "gt" => ODataBinaryOperator.GreaterThan,
                "ge" => ODataBinaryOperator.GreaterThanOrEqual,
                "lt" => ODataBinaryOperator.LessThan,
                "le" => ODataBinaryOperator.LessThanOrEqual,
                _ => (ODataBinaryOperator)(-1)
            };
            return (int)op >= 0;
        }

        ODataFilterNode ParseAdditive()
        {
            var left = ParseMultiplicative();
            while (IsKeyword("add") || IsKeyword("sub"))
            {
                var op = Next().Text.Equals("add", StringComparison.OrdinalIgnoreCase)
                    ? ODataBinaryOperator.Add
                    : ODataBinaryOperator.Subtract;
                left = ODataFilterNode.Binary(op, left, ParseMultiplicative());
            }
            return left;
        }

        ODataFilterNode ParseMultiplicative()
        {
            var left = ParseUnary();
            while (IsKeyword("mul") || IsKeyword("div") || IsKeyword("mod"))
            {
                var t = Next().Text.ToLowerInvariant();
                var op = t switch
                {
                    "mul" => ODataBinaryOperator.Multiply,
                    "div" => ODataBinaryOperator.Divide,
                    _ => ODataBinaryOperator.Modulo
                };
                left = ODataFilterNode.Binary(op, left, ParseUnary());
            }
            return left;
        }

        ODataFilterNode ParseUnary()
        {
            if (Current.Type == TokenType.Identifier && Current.Text == "-")
            {
                Next();
                return ODataFilterNode.Unary(ODataUnaryOperator.Negate, ParsePrimary());
            }
            return ParsePrimary();
        }

        ODataFilterNode ParsePrimary()
        {
            switch (Current.Type)
            {
                case TokenType.OpenParen:
                    Next();
                    var inner = ParseExpression();
                    Expect(TokenType.CloseParen);
                    return inner;

                case TokenType.String:
                    return ODataFilterNode.Constant(Next().Value);

                case TokenType.Number:
                    return ODataFilterNode.Constant(Next().Value);

                case TokenType.Identifier:
                    return ParseIdentifierOrLiteral();

                default:
                    throw new FormatException($"Unexpected token '{Current.Text}' in $filter '{source}'.");
            }
        }

        ODataFilterNode ParseIdentifierOrLiteral()
        {
            var text = Current.Text;

            if (text.Equals("null", StringComparison.OrdinalIgnoreCase))
            {
                Next();
                return ODataFilterNode.Constant(null);
            }
            if (text.Equals("true", StringComparison.OrdinalIgnoreCase))
            {
                Next();
                return ODataFilterNode.Constant(true);
            }
            if (text.Equals("false", StringComparison.OrdinalIgnoreCase))
            {
                Next();
                return ODataFilterNode.Constant(false);
            }

            // Function call?
            if (Functions.Contains(text) && tokens[pos + 1].Type == TokenType.OpenParen)
            {
                Next(); // function name
                Expect(TokenType.OpenParen);
                var args = new List<ODataFilterNode>();
                if (Current.Type != TokenType.CloseParen)
                {
                    args.Add(ParseExpression());
                    while (Current.Type == TokenType.Comma)
                    {
                        Next();
                        args.Add(ParseExpression());
                    }
                }
                Expect(TokenType.CloseParen);
                return ODataFilterNode.Function(text, args.ToArray());
            }

            if (Keywords.Contains(text))
                throw new FormatException($"Unexpected keyword '{text}' in $filter '{source}'.");

            // property path
            Next();
            return ODataFilterNode.Property(text);
        }

        void Expect(TokenType type)
        {
            if (Current.Type != type)
                throw new FormatException($"Expected '{type}' but found '{Current.Text}' in $filter '{source}'.");
            Next();
        }
    }
}
