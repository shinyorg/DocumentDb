using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Parses a human-friendly filter string (e.g. <c>"Age >= 30 and Status == 'open'"</c>) into an
/// <see cref="Expression{TDelegate}"/> of <c>Func&lt;T,bool&gt;</c> that the existing
/// <see cref="IDocumentQuery{T}.Where"/> pipeline compiles to SQL.
/// </summary>
/// <remarks>
/// Builds expression trees programmatically — never calls <c>Compile()</c> — and resolves fields by
/// walking source-generated <see cref="JsonTypeInfo.Properties"/>, so the whole path stays AOT/trim-safe.
/// </remarks>
static class FilterExpressionParser
{
    static readonly MethodInfo StringContains = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;
    static readonly MethodInfo StringStartsWith = typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;
    static readonly MethodInfo StringEndsWith = typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!;

    public static Expression<Func<T, bool>> Parse<T>(string filter, JsonTypeInfo<T> jsonTypeInfo) where T : class
        => Parse(filter, null, jsonTypeInfo);

    /// <summary>
    /// Parses a filter string that may contain interpolation placeholders, binding each placeholder to the
    /// captured value at the matching index in <paramref name="args"/>. See
    /// <see cref="FilterInterpolatedStringHandler"/> for how the placeholders are produced.
    /// </summary>
    public static Expression<Func<T, bool>> Parse<T>(string filter, IReadOnlyList<object?>? args, JsonTypeInfo<T> jsonTypeInfo) where T : class
    {
        var tokens = Lexer.Tokenize(filter);
        var parameter = Expression.Parameter(typeof(T), "x");
        var parser = new Parser(tokens, args, path => DocumentQueryExtensions.BuildMemberAccess(parameter, path, jsonTypeInfo));
        var body = parser.ParseExpression();
        return Expression.Lambda<Func<T, bool>>(body, parameter);
    }

    // ── Lexer ───────────────────────────────────────────────────────

    enum TokenKind { Identifier, String, Number, Operator, LParen, RParen, Comma, Placeholder, End }

    readonly record struct Token(TokenKind Kind, string Text, int Position);

    static class Lexer
    {
        public static List<Token> Tokenize(string input)
        {
            var tokens = new List<Token>();
            var i = 0;
            while (i < input.Length)
            {
                var c = input[i];

                if (c == FilterInterpolatedStringHandler.PlaceholderSentinel)
                {
                    tokens.Add(ReadPlaceholder(input, ref i));
                    continue;
                }

                if (char.IsWhiteSpace(c))
                {
                    i++;
                    continue;
                }

                switch (c)
                {
                    case '(':
                        tokens.Add(new Token(TokenKind.LParen, "(", i++));
                        continue;
                    case ')':
                        tokens.Add(new Token(TokenKind.RParen, ")", i++));
                        continue;
                    case ',':
                        tokens.Add(new Token(TokenKind.Comma, ",", i++));
                        continue;
                }

                if (c is '\'' or '"')
                {
                    tokens.Add(ReadString(input, ref i, c));
                    continue;
                }

                if (c is '=' or '!' or '<' or '>')
                {
                    tokens.Add(ReadOperator(input, ref i));
                    continue;
                }

                if (char.IsDigit(c) || (c == '-' && i + 1 < input.Length && char.IsDigit(input[i + 1])))
                {
                    tokens.Add(ReadNumber(input, ref i));
                    continue;
                }

                if (char.IsLetter(c) || c == '_')
                {
                    tokens.Add(ReadIdentifier(input, ref i));
                    continue;
                }

                throw Error($"Unexpected character '{c}'", i);
            }

            tokens.Add(new Token(TokenKind.End, "", input.Length));
            return tokens;
        }

        static Token ReadString(string input, ref int i, char quote)
        {
            var start = i;
            i++; // opening quote
            var sb = new StringBuilder();
            while (i < input.Length)
            {
                var c = input[i];
                if (c == quote)
                {
                    // Doubled quote is an escaped quote
                    if (i + 1 < input.Length && input[i + 1] == quote)
                    {
                        sb.Append(quote);
                        i += 2;
                        continue;
                    }
                    i++; // closing quote
                    return new Token(TokenKind.String, sb.ToString(), start);
                }
                sb.Append(c);
                i++;
            }
            throw Error("Unterminated string literal", start);
        }

        static Token ReadOperator(string input, ref int i)
        {
            var start = i;
            var c = input[i];
            var next = i + 1 < input.Length ? input[i + 1] : '\0';

            string op = (c, next) switch
            {
                ('=', '=') => "==",
                ('!', '=') => "!=",
                ('<', '>') => "<>",
                ('<', '=') => "<=",
                ('>', '=') => ">=",
                ('=', _) => "=",
                ('<', _) => "<",
                ('>', _) => ">",
                _ => throw Error($"Invalid operator starting with '{c}'", start)
            };
            i += op.Length;
            return new Token(TokenKind.Operator, op, start);
        }

        static Token ReadNumber(string input, ref int i)
        {
            var start = i;
            if (input[i] == '-')
                i++;
            while (i < input.Length && (char.IsDigit(input[i]) || input[i] is '.' or 'e' or 'E' or '+' or '-'))
            {
                // Stop a trailing sign that is not part of an exponent
                if (input[i] is '+' or '-' && i > start && input[i - 1] is not ('e' or 'E'))
                    break;
                i++;
            }
            return new Token(TokenKind.Number, input[start..i], start);
        }

        static Token ReadIdentifier(string input, ref int i)
        {
            var start = i;
            // Dotted paths are read as a single identifier token (e.g. "ShippingAddress.City").
            while (i < input.Length && (char.IsLetterOrDigit(input[i]) || input[i] is '_' or '.'))
                i++;
            return new Token(TokenKind.Identifier, input[start..i], start);
        }

        static Token ReadPlaceholder(string input, ref int i)
        {
            var start = i;
            i++; // opening sentinel
            var indexStart = i;
            while (i < input.Length && input[i] != FilterInterpolatedStringHandler.PlaceholderSentinel)
                i++;
            if (i >= input.Length)
                throw Error("Malformed interpolation placeholder", start);

            var index = input[indexStart..i];
            i++; // closing sentinel
            return new Token(TokenKind.Placeholder, index, start);
        }
    }

    // ── Parser ──────────────────────────────────────────────────────

    sealed class Parser
    {
        readonly List<Token> tokens;
        readonly IReadOnlyList<object?>? args;
        readonly Func<string, (Expression Body, Type LeafType)> resolve;
        int pos;

        public Parser(List<Token> tokens, IReadOnlyList<object?>? args, Func<string, (Expression, Type)> resolve)
        {
            this.tokens = tokens;
            this.args = args;
            this.resolve = resolve;
        }

        Token Current => this.tokens[this.pos];

        public Expression ParseExpression()
        {
            var expr = this.ParseOr();
            if (this.Current.Kind != TokenKind.End)
                throw Error($"Unexpected '{this.Current.Text}'", this.Current.Position);
            return expr;
        }

        Expression ParseOr()
        {
            var left = this.ParseAnd();
            while (this.IsKeyword("or"))
            {
                this.pos++;
                var right = this.ParseAnd();
                left = Expression.OrElse(left, right);
            }
            return left;
        }

        Expression ParseAnd()
        {
            var left = this.ParseNot();
            while (this.IsKeyword("and"))
            {
                this.pos++;
                var right = this.ParseNot();
                left = Expression.AndAlso(left, right);
            }
            return left;
        }

        Expression ParseNot()
        {
            if (this.IsKeyword("not"))
            {
                this.pos++;
                return Expression.Not(this.ParseNot());
            }
            return this.ParsePrimary();
        }

        Expression ParsePrimary()
        {
            if (this.Current.Kind == TokenKind.LParen)
            {
                this.pos++;
                var inner = this.ParseOr();
                this.Expect(TokenKind.RParen, ")");
                return inner;
            }

            if (this.Current.Kind != TokenKind.Identifier)
                throw Error($"Expected a field or '(', found '{this.Current.Text}'", this.Current.Position);

            // String function form: contains(field, 'x')
            var ident = this.Current.Text;
            if (this.tokens[this.pos + 1].Kind == TokenKind.LParen && IsStringFunction(ident))
                return this.ParseStringFunction();

            return this.ParseComparison();
        }

        Expression ParseComparison()
        {
            var fieldToken = this.Current;
            this.pos++;
            var (member, leafType) = this.Resolve(fieldToken);

            // field is [not] null
            if (this.IsKeyword("is"))
            {
                this.pos++;
                var negate = this.IsKeyword("not");
                if (negate)
                    this.pos++;
                this.ExpectKeyword("null");
                return BuildNullCheck(member, isNull: !negate);
            }

            // field in (a, b, c)
            if (this.IsKeyword("in"))
            {
                this.pos++;
                return this.ParseInList(member, leafType);
            }

            if (this.Current.Kind != TokenKind.Operator)
                throw Error($"Expected an operator after field '{fieldToken.Text}', found '{this.Current.Text}'", this.Current.Position);

            var op = this.Current.Text;
            this.pos++;
            var (value, isNull) = this.ParseValue(leafType);

            if (isNull)
            {
                if (op is "==" or "=")
                    return BuildNullCheck(member, isNull: true);
                if (op is "!=" or "<>")
                    return BuildNullCheck(member, isNull: false);
                throw Error($"Operator '{op}' cannot be used with null", fieldToken.Position);
            }

            return BuildBinary(op, member, leafType, value, fieldToken.Position);
        }

        Expression ParseInList(Expression member, Type leafType)
        {
            this.Expect(TokenKind.LParen, "(");
            var values = new List<object?>();
            while (true)
            {
                var (value, isNull) = this.ParseValue(leafType);
                values.Add(isNull ? null : value);

                if (this.Current.Kind == TokenKind.Comma)
                {
                    this.pos++;
                    continue;
                }
                break;
            }
            this.Expect(TokenKind.RParen, ")");

            // Lower to the same canonical Enumerable.Contains form as WhereIn so every provider emits its
            // native IN. Match preserves the historical string-filter semantics where a null in the list
            // matches null fields (… OR field IS NULL).
            return InExpressionBuilder.Build(member, values, NullHandling.Match);
        }

        Expression ParseStringFunction()
        {
            var func = this.Current.Text.ToLowerInvariant();
            this.pos++;
            this.Expect(TokenKind.LParen, "(");

            var fieldToken = this.Current;
            if (fieldToken.Kind != TokenKind.Identifier)
                throw Error($"'{func}' expects a field as its first argument", fieldToken.Position);
            this.pos++;
            var (member, leafType) = this.Resolve(fieldToken);
            if (leafType != typeof(string))
                throw Error($"'{func}' requires a string field but '{fieldToken.Text}' is '{leafType.Name}'", fieldToken.Position);

            this.Expect(TokenKind.Comma, ",");
            var (value, isNull) = this.ParseValue(typeof(string));
            this.Expect(TokenKind.RParen, ")");

            if (isNull)
                throw Error($"'{func}' does not accept a null argument", fieldToken.Position);

            var method = func switch
            {
                "contains" => StringContains,
                "startswith" => StringStartsWith,
                "endswith" => StringEndsWith,
                _ => throw Error($"Unknown string function '{func}'", fieldToken.Position)
            };
            return Expression.Call(member, method, Expression.Constant((string)value!, typeof(string)));
        }

        (object? Value, bool IsNull) ParseValue(Type leafType)
        {
            var token = this.Current;
            this.pos++;
            switch (token.Kind)
            {
                case TokenKind.String:
                    return (CoerceLiteral(token.Text, leafType, token.Position), false);

                case TokenKind.Number:
                    return (CoerceLiteral(token.Text, leafType, token.Position), false);

                case TokenKind.Placeholder:
                    return this.ResolvePlaceholder(token, leafType);

                case TokenKind.Identifier:
                    var lower = token.Text.ToLowerInvariant();
                    return lower switch
                    {
                        "null" => (null, true),
                        "true" => (CoerceLiteral("true", leafType, token.Position), false),
                        "false" => (CoerceLiteral("false", leafType, token.Position), false),
                        _ => throw Error($"Expected a value, found '{token.Text}'", token.Position)
                    };

                default:
                    throw Error($"Expected a value, found '{token.Text}'", token.Position);
            }
        }

        (object? Value, bool IsNull) ResolvePlaceholder(Token token, Type leafType)
        {
            var index = int.Parse(token.Text, CultureInfo.InvariantCulture);
            if (this.args is null || index < 0 || index >= this.args.Count)
                throw Error($"Interpolation argument {index} is missing", token.Position);

            var raw = this.args[index];
            return raw is null
                ? (null, true)
                : (CoercePlaceholder(raw, leafType, token.Position), false);
        }

        (Expression Body, Type LeafType) Resolve(Token fieldToken)
        {
            try
            {
                return this.resolve(fieldToken.Text);
            }
            catch (ArgumentException ex)
            {
                throw Error(ex.Message, fieldToken.Position);
            }
        }

        bool IsKeyword(string keyword)
            => this.Current.Kind == TokenKind.Identifier
               && this.Current.Text.Equals(keyword, StringComparison.OrdinalIgnoreCase);

        void ExpectKeyword(string keyword)
        {
            if (!this.IsKeyword(keyword))
                throw Error($"Expected '{keyword}', found '{this.Current.Text}'", this.Current.Position);
            this.pos++;
        }

        void Expect(TokenKind kind, string text)
        {
            if (this.Current.Kind != kind)
                throw Error($"Expected '{text}', found '{this.Current.Text}'", this.Current.Position);
            this.pos++;
        }
    }

    // ── Expression building ─────────────────────────────────────────

    static Expression BuildNullCheck(Expression member, bool isNull)
    {
        var asObject = Expression.Convert(member, typeof(object));
        var nullConstant = Expression.Constant(null, typeof(object));
        return isNull
            ? Expression.Equal(asObject, nullConstant)
            : Expression.NotEqual(asObject, nullConstant);
    }

    static Expression BuildBinary(string op, Expression member, Type leafType, object? value, int position)
    {
        var underlying = Nullable.GetUnderlyingType(leafType) ?? leafType;
        var isRelational = op is ">" or ">=" or "<" or "<=";
        if (isRelational && (underlying == typeof(string) || underlying == typeof(bool) || underlying == typeof(Guid)))
            throw Error($"Operator '{op}' is not supported for type '{underlying.Name}'", position);

        var constant = BuildConstant(value, leafType, member.Type);
        return op switch
        {
            "==" or "=" => Expression.Equal(member, constant),
            "!=" or "<>" => Expression.NotEqual(member, constant),
            ">" => Expression.GreaterThan(member, constant),
            ">=" => Expression.GreaterThanOrEqual(member, constant),
            "<" => Expression.LessThan(member, constant),
            "<=" => Expression.LessThanOrEqual(member, constant),
            _ => throw Error($"Unknown operator '{op}'", position)
        };
    }

    static Expression BuildConstant(object? value, Type leafType, Type memberType)
    {
        var underlying = Nullable.GetUnderlyingType(leafType) ?? leafType;
        Expression constant = Expression.Constant(value, underlying);
        if (memberType != underlying)
            constant = Expression.Convert(constant, memberType);
        return constant;
    }

    static bool IsStringFunction(string ident)
        => ident.Equals("contains", StringComparison.OrdinalIgnoreCase)
           || ident.Equals("startswith", StringComparison.OrdinalIgnoreCase)
           || ident.Equals("endswith", StringComparison.OrdinalIgnoreCase);

    static object? CoerceLiteral(string raw, Type targetType, int position)
    {
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
        try
        {
            if (underlying == typeof(string)) return raw;
            if (underlying == typeof(bool)) return bool.Parse(raw);
            if (underlying == typeof(Guid)) return Guid.Parse(raw);
            if (underlying == typeof(DateTime)) return DateTime.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            if (underlying == typeof(DateTimeOffset)) return DateTimeOffset.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            if (underlying == typeof(int)) return int.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(long)) return long.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(short)) return short.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(byte)) return byte.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(uint)) return uint.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(ulong)) return ulong.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(double)) return double.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(float)) return float.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying == typeof(decimal)) return decimal.Parse(raw, CultureInfo.InvariantCulture);
            if (underlying.IsEnum) return Enum.Parse(underlying, raw, ignoreCase: true);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or ArgumentException)
        {
            throw Error($"'{raw}' is not a valid '{underlying.Name}' value", position);
        }

        // Fallback: pass the raw string through and let the provider coerce.
        return raw;
    }

    /// <summary>
    /// Coerces an interpolated argument (already a CLR value) to the leaf property's underlying type so it
    /// can be embedded as a <see cref="ConstantExpression"/>. The common case — the value already matches
    /// the field type — is a no-op; strings route through <see cref="CoerceLiteral"/> (handling Guid,
    /// DateTime, enum, numeric parsing), and remaining mismatches fall back to <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>.
    /// </summary>
    static object? CoercePlaceholder(object value, Type leafType, int position)
    {
        var underlying = Nullable.GetUnderlyingType(leafType) ?? leafType;
        if (underlying.IsInstanceOfType(value))
            return value;

        if (value is string s)
            return CoerceLiteral(s, leafType, position);

        try
        {
            if (underlying.IsEnum)
                return Enum.ToObject(underlying, value);

            return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or InvalidCastException or ArgumentException)
        {
            throw Error($"Cannot convert interpolated value of type '{value.GetType().Name}' to '{underlying.Name}'", position);
        }
    }

    static ArgumentException Error(string message, int position)
        => new($"Filter parse error at position {position}: {message}.");
}
