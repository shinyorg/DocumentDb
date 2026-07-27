using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
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
    static readonly MethodInfo DistanceFn = typeof(DocumentFunctions).GetMethod(nameof(DocumentFunctions.Distance))!;
    static readonly MethodInfo LuceneMatchFn = typeof(DocumentFunctions).GetMethod(nameof(DocumentFunctions.LuceneMatch))!;
    static readonly MethodInfo LuceneScoreFn = typeof(DocumentFunctions).GetMethod(nameof(DocumentFunctions.LuceneScore))!;

    static MethodInfo MathFn(string name) => typeof(Math).GetMethod(name, [typeof(double)])!;
    static readonly MethodInfo MathRound2 = typeof(Math).GetMethod(nameof(Math.Round), [typeof(double), typeof(int)])!;
    static readonly MethodInfo MathPow = typeof(Math).GetMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!;
    static readonly MethodInfo StringConcat2 = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])!;

    // Every DocumentFunctions geo predicate is a single (Geometry, Geometry[, double]) overload — GetMethod by name.
    static MethodInfo SpatialFn(string documentFunctionName) => typeof(DocumentFunctions).GetMethod(documentFunctionName)!;

    // string geo-function name → DocumentFunctions method name (parity with the LINQ surface). "contains" is a
    // geo predicate only when the field is a Geometry — otherwise it stays the string Contains.
    static string? SpatialPredicateDocFn(string func, Type leafType) => func switch
    {
        "intersects" => nameof(DocumentFunctions.Intersects),
        "disjoint" => nameof(DocumentFunctions.Disjoint),
        "within" => nameof(DocumentFunctions.Within),
        "covers" => nameof(DocumentFunctions.Covers),
        "coveredby" => nameof(DocumentFunctions.CoveredBy),
        "touches" => nameof(DocumentFunctions.Touches),
        "crosses" => nameof(DocumentFunctions.Crosses),
        "overlaps" => nameof(DocumentFunctions.Overlaps),
        "geoequals" => nameof(DocumentFunctions.GeoEquals),
        "withindistance" => nameof(DocumentFunctions.WithinDistance),
        "contains" when typeof(Geometry).IsAssignableFrom(leafType) => nameof(DocumentFunctions.Contains),
        _ => null
    };

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
        var parser = new Parser(tokens, args, new JsonTypeInfoFieldBinder<T>(parameter, jsonTypeInfo, computed));
        var body = parser.ParseExpression();
        return Expression.Lambda<Func<T, bool>>(body, parameter);
    }

    // ── Schema-free entry points ────────────────────────────────────
    // Same grammar, same IR, different field resolution: paths become DocumentFieldExpression instead of a
    // CLR member chain, and a `path:type` suffix is accepted where nothing else pins the type.

    // A JSON collection's parameter is the raw body, so fields resolve to a path either way: through the
    // document type's metadata when there is one (full function set, correct JSON names and leaf types), or
    // schema-free with `:type` hints when there isn't.
    static IFieldBinder JsonBinder(JsonTypeInfo? typeInfo)
        => typeInfo is null ? new DynamicFieldBinder() : new JsonPathFieldBinder(typeInfo);

    /// <summary>Parses a filter over a JSON collection into a predicate on the raw body.</summary>
    public static Expression<Func<JsonObject, bool>> ParseJson(string filter, IReadOnlyList<object?>? args, JsonTypeInfo? typeInfo)
    {
        var tokens = Lexer.Tokenize(filter, allowTypeHints: typeInfo is null);
        var parameter = Expression.Parameter(typeof(JsonObject), "x");
        var parser = new Parser(tokens, args, JsonBinder(typeInfo));
        var body = parser.ParseExpression();
        return Expression.Lambda<Func<JsonObject, bool>>(body, parameter);
    }

    /// <summary>JSON-collection twin of <see cref="ParseProjection{T}"/>.</summary>
    public static (ParameterExpression Parameter, List<ProjectionItem> Items) ParseJsonProjection(string projection, JsonTypeInfo? typeInfo)
    {
        var tokens = Lexer.Tokenize(projection, allowTypeHints: typeInfo is null);
        var parameter = Expression.Parameter(typeof(JsonObject), "x");
        var parser = new Parser(tokens, null, JsonBinder(typeInfo));
        return (parameter, parser.ParseProjectionList());
    }

    /// <summary>JSON-collection twin of <see cref="ParseValueSelector{T}"/>.</summary>
    public static Expression<Func<JsonObject, object>> ParseJsonValueSelector(string expression, JsonTypeInfo? typeInfo)
    {
        var tokens = Lexer.Tokenize(expression, allowTypeHints: typeInfo is null);
        var parameter = Expression.Parameter(typeof(JsonObject), "x");
        var parser = new Parser(tokens, null, JsonBinder(typeInfo));
        var body = parser.ParseValueSelectorBody();
        if (body.Type.IsValueType)
            body = Expression.Convert(body, typeof(object));
        return Expression.Lambda<Func<JsonObject, object>>(body, parameter);
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
        var parser = new Parser(tokens, null, new JsonTypeInfoFieldBinder<T>(parameter, jsonTypeInfo, computed));
        return (parameter, parser.ParseProjectionList());
    }

    /// <summary>
    /// Parses a single value expression — a field or a scalar/geo value function such as
    /// <c>distance(area, '&lt;geojson&gt;')</c> — into an <c>Func&lt;T, object&gt;</c> selector for
    /// <c>OrderBy</c>/<c>OrderByDescending</c>. Reuses the same value-function grammar as <c>Where</c>/projections.
    /// </summary>
    public static Expression<Func<T, object>> ParseValueSelector<T>(string expression, JsonTypeInfo<T> jsonTypeInfo,
        IReadOnlyDictionary<string, ComputedMapping>? computed = null) where T : class
    {
        var tokens = Lexer.Tokenize(expression);
        var parameter = Expression.Parameter(typeof(T), "x");
        var parser = new Parser(tokens, null, new JsonTypeInfoFieldBinder<T>(parameter, jsonTypeInfo, computed));
        var body = parser.ParseValueSelectorBody();
        if (body.Type.IsValueType)
            body = Expression.Convert(body, typeof(object));
        return Expression.Lambda<Func<T, object>>(body, parameter);
    }

    // ── Lexer ───────────────────────────────────────────────────────

    enum TokenKind { Identifier, String, Number, Operator, LParen, RParen, Comma, Placeholder, End }

    readonly record struct Token(TokenKind Kind, string Text, int Position);

    static class Lexer
    {
        /// <param name="allowTypeHints">
        /// Schema-free only: lets an identifier carry a <c>:type</c> suffix. Off for the typed grammar, where
        /// <c>:</c> has never been legal and a field's type is never ambiguous — so a hint cannot leak in and
        /// silently become part of a property name.
        /// </param>
        public static List<Token> Tokenize(string input, bool allowTypeHints = false)
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
                    tokens.Add(ReadIdentifier(input, ref i, allowTypeHints));
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

        static Token ReadIdentifier(string input, ref int i, bool allowTypeHints)
        {
            var start = i;
            // Dotted paths are read as a single identifier token (e.g. "ShippingAddress.City"); a schema-free
            // type hint rides along on the same token (e.g. "total:number").
            while (i < input.Length
                   && (char.IsLetterOrDigit(input[i]) || input[i] is '_' or '.' || (allowTypeHints && input[i] == ':')))
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
        readonly IFieldBinder binder;
        int pos;

        public Parser(List<Token> tokens, IReadOnlyList<object?>? args, IFieldBinder binder)
        {
            this.tokens = tokens;
            this.args = args;
            this.binder = binder;
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
                // A schema-free left operand takes its type from the function on the right.
                left = this.binder.AdaptTo(left, right.Type);
                return BuildBinaryExpr(op, left, right, startPos);
            }

            var (value, isNull, valueType) = this.ParseValue(leftType);

            if (isNull)
            {
                if (op is "==" or "=")
                    return BuildNullCheck(left, isNull: true);
                if (op is "!=" or "<>")
                    return BuildNullCheck(left, isNull: false);
                throw Error($"Operator '{op}' cannot be used with null", startPos);
            }

            // Rule 1: infer the field's type from the other operand.
            left = this.binder.AdaptTo(left, valueType);
            return BuildBinary(op, left, valueType, value, startPos);
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
                case "lower": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Call(arg, StringToLower); resultType = typeof(string); break;
                case "upper": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Call(arg, StringToUpper); resultType = typeof(string); break;
                case "trim": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Call(arg, StringTrim); resultType = typeof(string); break;
                case "ltrim": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Call(arg, StringTrimStart); resultType = typeof(string); break;
                case "rtrim": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Call(arg, StringTrimEnd); resultType = typeof(string); break;
                case "length": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Property(arg, nameof(string.Length)); resultType = typeof(int); break;
                case "substring":
                    arg = this.RequireString(arg, argType, func, pos0);
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
                    arg = this.RequireString(arg, argType, func, pos0);
                    this.Expect(TokenKind.Comma, ","); var rFrom = this.ParseStringLiteral();
                    this.Expect(TokenKind.Comma, ","); var rTo = this.ParseStringLiteral();
                    result = Expression.Call(arg, StringReplace, Expression.Constant(rFrom), Expression.Constant(rTo));
                    resultType = typeof(string);
                    break;
                case "indexof":
                    arg = this.RequireString(arg, argType, func, pos0);
                    this.Expect(TokenKind.Comma, ",");
                    result = Expression.Call(arg, StringIndexOf, Expression.Constant(this.ParseStringLiteral()));
                    resultType = typeof(int);
                    break;
                case "abs" or "ceiling" or "ceil" or "floor" or "sqrt" or "sign":
                    var mathName = func == "ceil" ? "Ceiling" : char.ToUpperInvariant(func[0]) + func[1..];
                    result = Expression.Call(MathFn(mathName), this.binder.NumericArg(arg, argType));
                    resultType = func == "sign" ? typeof(int) : typeof(double);
                    break;
                case "round":
                    // round(x) or round(x, digits) — parity with LINQ Math.Round / Math.Round(x, n).
                    if (this.Current.Kind == TokenKind.Comma)
                    {
                        this.pos++;
                        result = Expression.Call(MathRound2, this.binder.NumericArg(arg, argType), Expression.Constant(this.ParseIntLiteral()));
                    }
                    else
                    {
                        result = Expression.Call(MathFn("Round"), this.binder.NumericArg(arg, argType));
                    }
                    resultType = typeof(double);
                    break;
                case "pow":
                    // pow(x, y) — parity with LINQ Math.Pow.
                    this.Expect(TokenKind.Comma, ",");
                    var (exponent, exponentType) = this.ParseArg();
                    result = Expression.Call(MathPow, this.binder.NumericArg(arg, argType), this.binder.NumericArg(exponent, exponentType));
                    resultType = typeof(double);
                    break;
                case "concat":
                    // concat(a, b, ...) — parity with LINQ string concatenation. Left-folds to string.Concat.
                    result = this.RequireString(arg, argType, func, pos0);
                    while (this.Current.Kind == TokenKind.Comma)
                    {
                        this.pos++;
                        var (next, nextType) = this.ParseArg();
                        next = this.RequireString(next, nextType, "concat", this.Current.Position);
                        // Build a string `+` (Add with the Concat method) — the lowerer maps string Add to
                        // ScalarFn.Concat; a plain Expression.Call(string.Concat) isn't recognized.
                        result = Expression.Add(result, next, StringConcat2);
                    }
                    resultType = typeof(string);
                    break;
                case "year" or "month" or "day" or "hour" or "minute" or "second":
                    arg = this.binder.MemberArg(arg, argType, typeof(DateTime));
                    result = Expression.Property(arg, char.ToUpperInvariant(func[0]) + func[1..]);
                    resultType = typeof(int);
                    break;
                case "soundex": arg = this.RequireString(arg, argType, func, pos0); result = Expression.Call(SoundexFn, arg); resultType = typeof(string); break;
                case "lucenescore":
                    this.binder.RequireMapping(func, pos0);
                    arg = this.RequireString(arg, argType, func, pos0);
                    this.Expect(TokenKind.Comma, ",");
                    result = Expression.Call(LuceneScoreFn, arg, Expression.Constant(this.ParseLuceneQueryString(), typeof(string)));
                    resultType = typeof(double);
                    break;
                case "distance":
                {
                    var field = this.binder.GeometryArg(arg, argType, pos0);
                    this.Expect(TokenKind.Comma, ",");
                    result = Expression.Call(DistanceFn, field, Expression.Constant(this.ParseGeometryOperand(), typeof(Geometry)));
                    resultType = typeof(double);
                    break;
                }
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

            // DocumentFunctions geo predicates (parity with the LINQ surface). The geometry argument is an
            // interpolated {value} (Geometry/GeoPoint) or a GeoJSON string literal.
            if (SpatialPredicateDocFn(func, leafType) is { } dfName)
                return this.BuildSpatialPredicate(dfName, member, leafType, pos0);

            switch (func)
            {
                case "lucenematch":
                {
                    this.binder.RequireMapping(func, pos0);
                    member = this.RequireString(member, leafType, func, pos0);
                    this.Expect(TokenKind.Comma, ",");
                    var query = this.ParseLuceneQueryString();
                    this.Expect(TokenKind.RParen, ")");
                    return Expression.Call(LuceneMatchFn, member, Expression.Constant(query, typeof(string)));
                }
                case "contains" or "startswith" or "endswith":
                {
                    member = this.RequireString(member, leafType, func, pos0);
                    this.Expect(TokenKind.Comma, ",");
                    var (value, isNull, _) = this.ParseValue(typeof(string));
                    this.Expect(TokenKind.RParen, ")");
                    if (isNull)
                        throw Error($"'{func}' does not accept a null argument", pos0);
                    var m = func switch { "contains" => StringContains, "startswith" => StringStartsWith, _ => StringEndsWith };
                    return Expression.Call(member, m, Expression.Constant((string)value!, typeof(string)));
                }
                case "isnullorempty":
                    member = this.RequireString(member, leafType, func, pos0);
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
                    var underlying = this.binder.EnumArg(leafType, func, pos0);
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

        Expression RequireString(Expression operand, Type type, string func, int pos)
            => this.binder.Require(operand, type, typeof(string), func, pos);

        Expression ParseInList(Expression member, Type leafType)
        {
            this.Expect(TokenKind.LParen, "(");
            var values = new List<object?>();
            // On a schema-free field the first non-null item fixes the list's type; the rest are coerced to it
            // so the IN set stays homogeneous.
            var effective = leafType;
            while (true)
            {
                var (value, isNull, valueType) = this.ParseValue(effective);
                if (!isNull && this.binder.IsUnresolved(effective))
                    effective = valueType;
                values.Add(isNull ? null : value);

                if (this.Current.Kind == TokenKind.Comma)
                {
                    this.pos++;
                    continue;
                }
                break;
            }
            this.Expect(TokenKind.RParen, ")");

            member = this.binder.AdaptTo(member, effective);

            // Lower to the same canonical Enumerable.Contains form as WhereIn so every provider emits its
            // native IN. Match preserves the historical string-filter semantics where a null in the list
            // matches null fields (… OR field IS NULL).
            return InExpressionBuilder.Build(member, values, NullHandling.Match);
        }


        /// <summary>
        /// Parses a literal against a field's leaf type. <c>ValueType</c> is the type the literal was
        /// actually bound as: identical to <paramref name="leafType"/> on the typed surface, and the
        /// literal's own natural type on a schema-free field that has nothing else pinning it — which is how
        /// <c>total &gt; 100</c> comes out numeric.
        /// </summary>
        (object? Value, bool IsNull, Type ValueType) ParseValue(Type leafType)
        {
            var token = this.Current;
            this.pos++;
            switch (token.Kind)
            {
                case TokenKind.String:
                {
                    var t = this.binder.LiteralType(leafType, typeof(string));
                    return (CoerceLiteral(token.Text, t, token.Position), false, t);
                }

                case TokenKind.Number:
                {
                    var t = this.binder.LiteralType(leafType, NaturalNumberType(token.Text));
                    return (CoerceLiteral(token.Text, t, token.Position), false, t);
                }

                case TokenKind.Placeholder:
                    return this.ResolvePlaceholder(token, leafType);

                case TokenKind.Identifier:
                    var lower = token.Text.ToLowerInvariant();
                    switch (lower)
                    {
                        case "null":
                            return (null, true, leafType);
                        case "true":
                        case "false":
                        {
                            var t = this.binder.LiteralType(leafType, typeof(bool));
                            return (CoerceLiteral(lower, t, token.Position), false, t);
                        }
                        default:
                            throw Error($"Expected a value, found '{token.Text}'", token.Position);
                    }

                default:
                    throw Error($"Expected a value, found '{token.Text}'", token.Position);
            }
        }

        (object? Value, bool IsNull, Type ValueType) ResolvePlaceholder(Token token, Type leafType)
        {
            var index = int.Parse(token.Text, CultureInfo.InvariantCulture);
            if (this.args is null || index < 0 || index >= this.args.Count)
                throw Error($"Interpolation argument {index} is missing", token.Position);

            var raw = this.args[index];
            if (raw is null)
                return (null, true, leafType);

            // An interpolated placeholder already carries a CLR type, so a schema-free field infers straight
            // from it — no CoerceLiteral round-trip through text.
            var t = this.binder.LiteralType(leafType, raw.GetType());
            return (CoercePlaceholder(raw, t, token.Position), false, t);
        }

        // Mirrors ParseArg's literal typing: an integer that fits int stays an int, everything else is double.
        static Type NaturalNumberType(string raw)
            => long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l) && l is >= int.MinValue and <= int.MaxValue
                ? typeof(int)
                : typeof(double);

        (Expression Body, Type LeafType) Resolve(Token fieldToken)
        {
            try
            {
                return this.binder.Resolve(fieldToken.Text);
            }
            catch (ArgumentException ex)
            {
                throw Error(ex.Message, fieldToken.Position);
            }
        }

        // ── Geo function support (shared by Where predicates, OrderBy/Project distance) ──────────────

        public Expression ParseValueSelectorBody()
        {
            var (body, _) = this.ParseArg();
            if (this.Current.Kind != TokenKind.End)
                throw Error($"Unexpected '{this.Current.Text}' after the order-by expression", this.Current.Position);
            return body;
        }

        Expression BuildSpatialPredicate(string dfName, Expression member, Type leafType, int pos0)
        {
            var field = this.binder.GeometryArg(member, leafType, pos0);
            this.Expect(TokenKind.Comma, ",");
            var geometry = Expression.Constant(this.ParseGeometryOperand(), typeof(Geometry));
            if (dfName == nameof(DocumentFunctions.WithinDistance))
            {
                this.Expect(TokenKind.Comma, ",");
                var meters = this.ParseMeters();
                this.Expect(TokenKind.RParen, ")");
                return Expression.Call(SpatialFn(dfName), field, geometry, Expression.Constant(meters, typeof(double)));
            }
            this.Expect(TokenKind.RParen, ")");
            return Expression.Call(SpatialFn(dfName), field, geometry);
        }

        // The query geometry: an interpolated {value} (Geometry or GeoPoint) or an inline GeoJSON string literal.
        [UnconditionalSuppressMessage("Trimming", "IL2026",
            Justification = "Geometry carries an explicit [JsonConverter(GeometryJsonConverter)] that reads GeoJSON by hand — no reflection over unknown types.")]
        [UnconditionalSuppressMessage("AOT", "IL3050",
            Justification = "GeometryJsonConverter is a hand-written converter; no runtime code generation is required.")]
        Geometry ParseGeometryOperand()
        {
            var token = this.Current;
            if (token.Kind == TokenKind.Placeholder)
            {
                this.pos++;
                var index = int.Parse(token.Text, CultureInfo.InvariantCulture);
                if (this.args is null || index < 0 || index >= this.args.Count)
                    throw Error($"Interpolation argument {index} is missing", token.Position);
                return this.args[index] switch
                {
                    Geometry g => g,
                    GeoPoint p => p, // implicit → point geometry
                    null => throw Error("A geo function's geometry argument cannot be null", token.Position),
                    var other => throw Error($"A geo function's geometry argument must be a Geometry or GeoPoint, but got '{other.GetType().Name}'", token.Position)
                };
            }
            if (token.Kind == TokenKind.String)
            {
                this.pos++;
                try
                {
                    return JsonSerializer.Deserialize<Geometry>(token.Text)
                        ?? throw Error("The GeoJSON geometry literal deserialized to null", token.Position);
                }
                catch (JsonException ex)
                {
                    throw Error($"Invalid GeoJSON geometry literal: {ex.Message}", token.Position);
                }
            }
            throw Error("Expected a geometry — an interpolated {value} or a GeoJSON string literal", token.Position);
        }

        // A Lucene query operand: an inline string literal or an interpolated {value} stringified.
        string ParseLuceneQueryString()
        {
            var token = this.Current;
            if (token.Kind == TokenKind.String)
            {
                this.pos++;
                return token.Text;
            }
            if (token.Kind == TokenKind.Placeholder)
            {
                this.pos++;
                var index = int.Parse(token.Text, CultureInfo.InvariantCulture);
                if (this.args is null || index < 0 || index >= this.args.Count || this.args[index] is null)
                    throw Error("The Lucene query argument is missing", token.Position);
                return this.args[index]!.ToString()!;
            }
            throw Error("A lucene query must be a string literal or an interpolated {value}", token.Position);
        }

        double ParseMeters()
        {
            var token = this.Current;
            if (token.Kind == TokenKind.Number)
            {
                this.pos++;
                return double.Parse(token.Text, CultureInfo.InvariantCulture);
            }
            if (token.Kind == TokenKind.Placeholder)
            {
                this.pos++;
                var index = int.Parse(token.Text, CultureInfo.InvariantCulture);
                if (this.args is null || index < 0 || index >= this.args.Count || this.args[index] is null)
                    throw Error("The WithinDistance meters argument is missing", token.Position);
                return Convert.ToDouble(this.args[index], CultureInfo.InvariantCulture);
            }
            throw Error("WithinDistance expects a numeric meters argument", token.Position);
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
        "contains" or "startswith" or "endswith" or "isnullorempty" or "hasflag"
        // Geo predicates (DocumentFunctions parity)
        or "intersects" or "disjoint" or "within" or "covers" or "coveredby"
        or "touches" or "crosses" or "overlaps" or "geoequals" or "withindistance"
        // Full-text (DocumentFunctions.LuceneMatch parity)
        or "lucenematch";

    static bool IsValueFunction(string ident) => ident.ToLowerInvariant() is
        "lower" or "upper" or "length" or "trim" or "ltrim" or "rtrim" or "substring" or "replace" or "indexof"
        or "abs" or "ceiling" or "ceil" or "floor" or "round" or "sqrt" or "sign" or "pow" or "concat"
        or "year" or "month" or "day" or "hour" or "minute" or "second" or "soundex" or "distance"
        // Full-text score (DocumentFunctions.LuceneScore parity)
        or "lucenescore";

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
        => FilterParseError.Create(message, position);
}
