using System.Diagnostics.CodeAnalysis;
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
    static readonly MethodInfo StringToLower = typeof(string).GetMethod(nameof(string.ToLower), Type.EmptyTypes)!;
    static readonly MethodInfo StringToUpper = typeof(string).GetMethod(nameof(string.ToUpper), Type.EmptyTypes)!;
    static readonly MethodInfo StringTrim = typeof(string).GetMethod(nameof(string.Trim), Type.EmptyTypes)!;
    static readonly MethodInfo StringTrimStart = typeof(string).GetMethod(nameof(string.TrimStart), Type.EmptyTypes)!;
    static readonly MethodInfo StringTrimEnd = typeof(string).GetMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;
    static readonly MethodInfo StringSubstring1 = typeof(string).GetMethod(nameof(string.Substring), [typeof(int)])!;
    static readonly MethodInfo StringSubstring2 = typeof(string).GetMethod(nameof(string.Substring), [typeof(int), typeof(int)])!;
    static readonly MethodInfo StringReplace = typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;
    static readonly MethodInfo StringIndexOf = typeof(string).GetMethod(nameof(string.IndexOf), [typeof(string)])!;
    static readonly MethodInfo StringIsNullOrEmpty = typeof(string).GetMethod(nameof(string.IsNullOrEmpty), [typeof(string)])!;
    static readonly MethodInfo EnumHasFlag = typeof(Enum).GetMethod(nameof(Enum.HasFlag))!;
    static readonly MethodInfo SoundexFn = typeof(DocumentFunctions).GetMethod(nameof(DocumentFunctions.Soundex))!;

    static MethodInfo MathFn(string name) => typeof(Math).GetMethod(name, [typeof(double)])!;

    public static Expression<Func<T, bool>> Parse<T>(string filter, JsonTypeInfo<T> jsonTypeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null) where T : class
        => Parse(filter, null, jsonTypeInfo, computed);

    /// <summary>
    /// Parses a filter string that may contain interpolation placeholders, binding each placeholder to the
    /// captured value at the matching index in <paramref name="args"/>. See
    /// <see cref="FilterInterpolatedStringHandler"/> for how the placeholders are produced.
    /// </summary>
    public static Expression<Func<T, bool>> Parse<T>(string filter, IReadOnlyList<object?>? args, JsonTypeInfo<T> jsonTypeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null) where T : class
    {
        var tokens = Lexer.Tokenize(filter);
        var parameter = Expression.Parameter(typeof(T), "x");
        var parser = new Parser(tokens, args, path => DocumentQueryExtensions.BuildMemberAccess(parameter, path, jsonTypeInfo, computed));
        var body = parser.ParseExpression();
        return Expression.Lambda<Func<T, bool>>(body, parameter);
    }

    /// <summary>
    /// A single projected field — either a plain document path (<see cref="FieldPath"/>) or a scalar
    /// function expression (<see cref="ValueExpr"/>). <see cref="Alias"/> is the output key (required for
    /// function projections, optional for plain fields where it defaults to the leaf JSON name).
    /// </summary>
    public readonly record struct ProjectionItem(string? Alias, string? FieldPath, Expression? ValueExpr);

    /// <summary>
    /// Parses a projection list (<c>"name, lower(email) as email, year(created) as yr"</c>) into items.
    /// Reuses the value-function grammar so projections expose the same scalar functions as <c>Where</c>.
    /// </summary>
    public static (ParameterExpression Parameter, List<ProjectionItem> Items) ParseProjection<T>(string projection, JsonTypeInfo<T> jsonTypeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null) where T : class
    {
        var tokens = Lexer.Tokenize(projection);
        var parameter = Expression.Parameter(typeof(T), "x");
        var parser = new Parser(tokens, null, path => DocumentQueryExtensions.BuildMemberAccess(parameter, path, jsonTypeInfo, computed));
        return (parameter, parser.ParseProjectionList());
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

        public List<ProjectionItem> ParseProjectionList()
        {
            var items = new List<ProjectionItem> { this.ParseProjectionItem() };
            while (this.Current.Kind == TokenKind.Comma)
            {
                this.pos++;
                items.Add(this.ParseProjectionItem());
            }
            if (this.Current.Kind != TokenKind.End)
                throw Error($"Unexpected '{this.Current.Text}' in projection", this.Current.Position);
            return items;
        }

        ProjectionItem ParseProjectionItem()
        {
            if (this.Current.Kind != TokenKind.Identifier)
                throw Error($"Expected a field or function, found '{this.Current.Text}'", this.Current.Position);

            // Value-function projection — requires an explicit alias.
            if (this.tokens[this.pos + 1].Kind == TokenKind.LParen && IsValueFunction(this.Current.Text))
            {
                var (expr, _) = this.ParseValueFunction();
                return new ProjectionItem(this.ParseAlias(required: true), null, expr);
            }

            // Plain field path (resolved against JsonTypeInfo by the caller).
            var fieldToken = this.Current;
            this.pos++;
            return new ProjectionItem(this.ParseAlias(required: false), fieldToken.Text, null);
        }

        string? ParseAlias(bool required)
        {
            if (this.IsKeyword("as"))
            {
                this.pos++;
                if (this.Current.Kind != TokenKind.Identifier)
                    throw Error($"Expected an alias after 'as', found '{this.Current.Text}'", this.Current.Position);
                var alias = this.Current.Text;
                this.pos++;
                return alias;
            }
            if (required)
                throw Error("A function projection requires an alias: 'func(field) as name'", this.Current.Position);
            return null;
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

            // Predicate function form: contains(field, 'x'), isnullorempty(field), hasflag(field, 'Flag')
            if (this.tokens[this.pos + 1].Kind == TokenKind.LParen && IsPredicateFunction(this.Current.Text))
                return this.ParsePredicateFunction();

            return this.ParseComparison();
        }

        Expression ParseComparison()
        {
            var startPos = this.Current.Position;
            var (left, leftType) = this.ParseArg();

            // left is [not] null
            if (this.IsKeyword("is"))
            {
                this.pos++;
                var negate = this.IsKeyword("not");
                if (negate)
                    this.pos++;
                this.ExpectKeyword("null");
                return BuildNullCheck(left, isNull: !negate);
            }

            // left in (a, b, c)
            if (this.IsKeyword("in"))
            {
                this.pos++;
                return this.ParseInList(left, leftType);
            }

            if (this.Current.Kind != TokenKind.Operator)
                throw Error($"Expected an operator, found '{this.Current.Text}'", this.Current.Position);

            var op = this.Current.Text;
            this.pos++;

            // RHS is another value function (e.g. soundex(name) = soundex('Smith')) — otherwise a literal.
            if (this.Current.Kind == TokenKind.Identifier
                && this.tokens[this.pos + 1].Kind == TokenKind.LParen
                && IsValueFunction(this.Current.Text))
            {
                var (right, _) = this.ParseArg();
                return BuildBinaryExpr(op, left, right, startPos);
            }

            var (value, isNull) = this.ParseValue(leftType);

            if (isNull)
            {
                if (op is "==" or "=")
                    return BuildNullCheck(left, isNull: true);
                if (op is "!=" or "<>")
                    return BuildNullCheck(left, isNull: false);
                throw Error($"Operator '{op}' cannot be used with null", startPos);
            }

            return BuildBinary(op, left, leftType, value, startPos);
        }

        // An operand: a field, a value-function call (lower/length/substring/abs/year/soundex/…), or a literal.
        (Expression Body, Type Type) ParseArg()
        {
            if (this.Current.Kind == TokenKind.Identifier)
            {
                if (this.tokens[this.pos + 1].Kind == TokenKind.LParen)
                {
                    if (IsValueFunction(this.Current.Text))
                        return this.ParseValueFunction();
                    throw Error($"'{this.Current.Text}' is not a value function", this.Current.Position);
                }

                var lower = this.Current.Text.ToLowerInvariant();
                if (lower is "true" or "false")
                {
                    this.pos++;
                    return (Expression.Constant(lower == "true"), typeof(bool));
                }

                var tok = this.Current;
                this.pos++;
                return this.Resolve(tok);
            }

            if (this.Current.Kind == TokenKind.String)
            {
                var s = this.Current.Text;
                this.pos++;
                return (Expression.Constant(s, typeof(string)), typeof(string));
            }

            if (this.Current.Kind == TokenKind.Number)
            {
                var raw = this.Current.Text;
                this.pos++;
                if (long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l) && l is >= int.MinValue and <= int.MaxValue)
                    return (Expression.Constant((int)l), typeof(int));
                return (Expression.Constant(double.Parse(raw, CultureInfo.InvariantCulture)), typeof(double));
            }

            throw Error($"Expected a field, function, or value, found '{this.Current.Text}'", this.Current.Position);
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026",
            Justification = "Date-part / Length accessors reference BCL properties (DateTime/DateTimeOffset/string) that are always preserved; user fields resolve through source-generated JsonTypeInfo.")]
        (Expression Body, Type Type) ParseValueFunction()
        {
            var func = this.Current.Text.ToLowerInvariant();
            var pos0 = this.Current.Position;
            this.pos++;
            this.Expect(TokenKind.LParen, "(");
            var (arg, argType) = this.ParseArg();

            Expression result;
            Type resultType;
            switch (func)
            {
                case "lower": RequireString(func, argType, pos0); result = Expression.Call(arg, StringToLower); resultType = typeof(string); break;
                case "upper": RequireString(func, argType, pos0); result = Expression.Call(arg, StringToUpper); resultType = typeof(string); break;
                case "trim": RequireString(func, argType, pos0); result = Expression.Call(arg, StringTrim); resultType = typeof(string); break;
                case "ltrim": RequireString(func, argType, pos0); result = Expression.Call(arg, StringTrimStart); resultType = typeof(string); break;
                case "rtrim": RequireString(func, argType, pos0); result = Expression.Call(arg, StringTrimEnd); resultType = typeof(string); break;
                case "length": RequireString(func, argType, pos0); result = Expression.Property(arg, nameof(string.Length)); resultType = typeof(int); break;
                case "substring":
                    this.Expect(TokenKind.Comma, ",");
                    var start = this.ParseIntLiteral();
                    if (this.Current.Kind == TokenKind.Comma)
                    {
                        this.pos++;
                        result = Expression.Call(arg, StringSubstring2, Expression.Constant(start), Expression.Constant(this.ParseIntLiteral()));
                    }
                    else
                    {
                        result = Expression.Call(arg, StringSubstring1, Expression.Constant(start));
                    }
                    resultType = typeof(string);
                    break;
                case "replace":
                    this.Expect(TokenKind.Comma, ","); var rFrom = this.ParseStringLiteral();
                    this.Expect(TokenKind.Comma, ","); var rTo = this.ParseStringLiteral();
                    result = Expression.Call(arg, StringReplace, Expression.Constant(rFrom), Expression.Constant(rTo));
                    resultType = typeof(string);
                    break;
                case "indexof":
                    this.Expect(TokenKind.Comma, ",");
                    result = Expression.Call(arg, StringIndexOf, Expression.Constant(this.ParseStringLiteral()));
                    resultType = typeof(int);
                    break;
                case "abs" or "ceiling" or "ceil" or "floor" or "round" or "sqrt" or "sign":
                    var mathName = func == "ceil" ? "Ceiling" : char.ToUpperInvariant(func[0]) + func[1..];
                    result = Expression.Call(MathFn(mathName), Expression.Convert(arg, typeof(double)));
                    resultType = func == "sign" ? typeof(int) : typeof(double);
                    break;
                case "year" or "month" or "day" or "hour" or "minute" or "second":
                    result = Expression.Property(arg, char.ToUpperInvariant(func[0]) + func[1..]);
                    resultType = typeof(int);
                    break;
                case "soundex": RequireString(func, argType, pos0); result = Expression.Call(SoundexFn, arg); resultType = typeof(string); break;
                default: throw Error($"Unknown function '{func}'", pos0);
            }

            this.Expect(TokenKind.RParen, ")");
            return (result, resultType);
        }

        Expression ParsePredicateFunction()
        {
            var func = this.Current.Text.ToLowerInvariant();
            var pos0 = this.Current.Position;
            this.pos++;
            this.Expect(TokenKind.LParen, "(");
            var (member, leafType) = this.ParseArg();

            switch (func)
            {
                case "contains" or "startswith" or "endswith":
                {
                    RequireString(func, leafType, pos0);
                    this.Expect(TokenKind.Comma, ",");
                    var (value, isNull) = this.ParseValue(typeof(string));
                    this.Expect(TokenKind.RParen, ")");
                    if (isNull)
                        throw Error($"'{func}' does not accept a null argument", pos0);
                    var m = func switch { "contains" => StringContains, "startswith" => StringStartsWith, _ => StringEndsWith };
                    return Expression.Call(member, m, Expression.Constant((string)value!, typeof(string)));
                }
                case "isnullorempty":
                    RequireString(func, leafType, pos0);
                    this.Expect(TokenKind.RParen, ")");
                    return Expression.Call(StringIsNullOrEmpty, member);
                case "hasflag":
                {
                    this.Expect(TokenKind.Comma, ",");
                    var flagToken = this.Current;
                    if (flagToken.Kind != TokenKind.String)
                        throw Error("'hasflag' expects a flag name as a quoted string", flagToken.Position);
                    this.pos++;
                    this.Expect(TokenKind.RParen, ")");
                    var underlying = Nullable.GetUnderlyingType(leafType) ?? leafType;
                    if (!underlying.IsEnum)
                        throw Error($"'hasflag' requires an enum field but got '{underlying.Name}'", pos0);
                    object enumVal;
                    try { enumVal = Enum.Parse(underlying, flagToken.Text, ignoreCase: true); }
                    catch (Exception ex) when (ex is ArgumentException or OverflowException) { throw Error($"'{flagToken.Text}' is not a valid '{underlying.Name}' value", flagToken.Position); }
                    return Expression.Call(Expression.Convert(member, typeof(Enum)), EnumHasFlag, Expression.Constant(enumVal, typeof(Enum)));
                }
                default:
                    throw Error($"Unknown function '{func}'", pos0);
            }
        }

        int ParseIntLiteral()
        {
            if (this.Current.Kind != TokenKind.Number)
                throw Error($"Expected a number, found '{this.Current.Text}'", this.Current.Position);
            var raw = this.Current.Text;
            this.pos++;
            return int.Parse(raw, CultureInfo.InvariantCulture);
        }

        string ParseStringLiteral()
        {
            if (this.Current.Kind != TokenKind.String)
                throw Error($"Expected a string literal, found '{this.Current.Text}'", this.Current.Position);
            var s = this.Current.Text;
            this.pos++;
            return s;
        }

        static void RequireString(string func, Type type, int pos)
        {
            if (type != typeof(string))
                throw Error($"'{func}' requires a string argument but got '{type.Name}'", pos);
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

    // Comparison between two expression operands (e.g. soundex(name) = soundex('Smith')).
    static Expression BuildBinaryExpr(string op, Expression left, Expression right, int position)
    {
        if (left.Type != right.Type)
            right = Expression.Convert(right, left.Type);

        var underlying = Nullable.GetUnderlyingType(left.Type) ?? left.Type;
        var isRelational = op is ">" or ">=" or "<" or "<=";
        if (isRelational && (underlying == typeof(string) || underlying == typeof(bool) || underlying == typeof(Guid)))
            throw Error($"Operator '{op}' is not supported for type '{underlying.Name}'", position);

        return op switch
        {
            "==" or "=" => Expression.Equal(left, right),
            "!=" or "<>" => Expression.NotEqual(left, right),
            ">" => Expression.GreaterThan(left, right),
            ">=" => Expression.GreaterThanOrEqual(left, right),
            "<" => Expression.LessThan(left, right),
            "<=" => Expression.LessThanOrEqual(left, right),
            _ => throw Error($"Unknown operator '{op}'", position)
        };
    }

    static bool IsPredicateFunction(string ident) => ident.ToLowerInvariant() is
        "contains" or "startswith" or "endswith" or "isnullorempty" or "hasflag";

    static bool IsValueFunction(string ident) => ident.ToLowerInvariant() is
        "lower" or "upper" or "length" or "trim" or "ltrim" or "rtrim" or "substring" or "replace" or "indexof"
        or "abs" or "ceiling" or "ceil" or "floor" or "round" or "sqrt" or "sign"
        or "year" or "month" or "day" or "hour" or "minute" or "second" or "soundex";

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
