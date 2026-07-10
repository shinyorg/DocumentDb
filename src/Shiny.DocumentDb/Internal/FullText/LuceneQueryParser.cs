using System.Globalization;

namespace Shiny.DocumentDb.Internal.FullText;

/// <summary>
/// A dependency-free recursive-descent parser for a practical subset of the Lucene query syntax, lowering
/// a query string to the <see cref="FtQuery"/> AST. No Lucene library is referenced — the grammar is
/// implemented by hand so the engine stays trim/AOT-clean and provider-neutral.
/// </summary>
/// <remarks>
/// Supported: terms, <c>"phrases"</c>, <c>AND</c>/<c>OR</c>/<c>NOT</c> (plus <c>&amp;&amp;</c>/<c>||</c>/<c>!</c>
/// and <c>+</c>/<c>-</c> modifiers), <c>(</c>grouping<c>)</c>, trailing-wildcard prefixes (<c>foo*</c>),
/// fuzzy (<c>foo~</c>/<c>foo~1</c>), proximity (<c>"a b"~5</c>), boost (<c>foo^2</c>), and field scoping
/// (<c>title:foo</c>). Boolean precedence follows Lucene's classic QueryParser: <c>AND</c> promotes its
/// operands to required, the default operator between bare terms is <c>OR</c>, and grouping overrides both.
/// Range queries (<c>[a TO b]</c>) and non-trailing wildcards are rejected with a clear message.
/// </remarks>
public sealed class LuceneQueryParser
{
    readonly string text;
    int pos;

    LuceneQueryParser(string text) => this.text = text;

    public static FtQuery Parse(string query)
    {
        ArgumentNullException.ThrowIfNull(query);
        var parser = new LuceneQueryParser(query);
        var root = parser.ParseBool(topLevel: true);
        parser.SkipWhitespace();
        if (!parser.Eof)
            throw parser.Error($"unexpected character '{parser.Current}'");
        if (root is null)
            throw new FormatException("The full-text query is empty.");
        return new FtQuery(root, query);
    }

    // A boolean level: a run of modified clauses until EOF or a closing ')'.
    FtNode? ParseBool(bool topLevel)
    {
        var clauses = new List<FtClause>();
        while (true)
        {
            this.SkipWhitespace();
            if (this.Eof || this.Current == ')')
                break;

            var infix = this.ReadInfixOperator();      // AND / OR / (none)
            var modifier = this.ReadPrefixModifier();   // + / - / NOT / (none)

            var node = this.ParsePrimary();
            node = this.MaybeBoost(node);

            var occur = modifier switch
            {
                Modifier.Plus => FtOccur.Must,
                Modifier.Minus => FtOccur.MustNot,
                _ => infix == Infix.And ? FtOccur.Must : FtOccur.Should
            };

            // Lucene's AND makes the *preceding* clause required too (a AND b ⇒ +a +b).
            if (infix == Infix.And && clauses.Count > 0 && clauses[^1].Occur == FtOccur.Should)
                clauses[^1] = clauses[^1] with { Occur = FtOccur.Must };

            clauses.Add(new FtClause(occur, node));
        }

        if (clauses.Count == 0)
            return topLevel ? null : throw this.Error("empty group '()'");

        // Collapse a single plain SHOULD clause to its inner node — keeps simple queries flat.
        if (clauses.Count == 1 && clauses[0].Occur == FtOccur.Should)
            return clauses[0].Node;

        return new FtBool(clauses);
    }

    FtNode ParsePrimary()
    {
        this.SkipWhitespace();
        if (this.Eof)
            throw this.Error("expected a term");

        if (this.Current == '(')
        {
            this.pos++;
            var inner = this.ParseBool(topLevel: false) ?? throw this.Error("empty group '()'");
            this.SkipWhitespace();
            if (this.Eof || this.Current != ')')
                throw this.Error("expected ')'");
            this.pos++;
            return inner;
        }

        // Optional field scope: an identifier immediately followed by ':'.
        var field = this.TryReadFieldPrefix();
        var atom = this.ParseAtom();
        return field is null ? atom : new FtField(field, atom);
    }

    FtNode ParseAtom()
    {
        this.SkipWhitespace();
        if (this.Eof)
            throw this.Error("expected a term or phrase");

        if (this.Current is '[' or '{')
            throw this.Error("range queries ('[a TO b]') are not supported in full-text queries");

        if (this.Current == '"')
            return this.ParsePhrase();

        var term = this.ReadTermText();
        if (term.Length == 0)
            throw this.Error($"unexpected character '{this.Current}'");

        // Trailing wildcard → prefix term.
        if (!this.Eof && this.Current == '*')
        {
            this.pos++;
            return new FtPrefix(term);
        }

        // Fuzzy: term~ or term~N.
        if (!this.Eof && this.Current == '~')
        {
            this.pos++;
            var edits = this.TryReadNumber(out var n) ? Math.Clamp((int)n, 0, 2) : 2;
            return new FtFuzzy(term, edits);
        }

        return new FtTerm(term);
    }

    FtNode ParsePhrase()
    {
        this.pos++; // opening quote
        var terms = new List<string>();
        var start = this.pos;
        while (!this.Eof && this.Current != '"')
            this.pos++;
        if (this.Eof)
            throw this.Error("unterminated phrase (missing closing '\"')");

        var body = this.text.Substring(start, this.pos - start);
        this.pos++; // closing quote
        foreach (var token in body.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))
            terms.Add(token.ToLowerInvariant());

        if (terms.Count == 0)
            throw this.Error("empty phrase");

        // Proximity: "a b"~N.
        if (!this.Eof && this.Current == '~')
        {
            this.pos++;
            var slop = this.TryReadNumber(out var n) ? Math.Max(0, (int)n) : 0;
            return new FtProximity(terms, slop);
        }

        return new FtPhrase(terms);
    }

    FtNode MaybeBoost(FtNode node)
    {
        this.SkipWhitespace();
        if (this.Eof || this.Current != '^')
            return node;
        this.pos++;
        if (!this.TryReadNumber(out var factor))
            throw this.Error("expected a boost factor after '^'");
        return new FtBoost(node, factor);
    }

    // ── Lexing helpers ──────────────────────────────────────────────────────

    enum Infix { None, And, Or }
    enum Modifier { None, Plus, Minus }

    Infix ReadInfixOperator()
    {
        this.SkipWhitespace();
        if (this.TryReadKeyword("AND") || this.TryReadSymbol("&&"))
            return Infix.And;
        if (this.TryReadKeyword("OR") || this.TryReadSymbol("||"))
            return Infix.Or;
        return Infix.None;
    }

    Modifier ReadPrefixModifier()
    {
        this.SkipWhitespace();
        if (this.TryReadKeyword("NOT") || this.TryReadSymbol("!"))
            return Modifier.Minus;
        if (this.Eof)
            return Modifier.None;
        if (this.Current == '+') { this.pos++; return Modifier.Plus; }
        if (this.Current == '-') { this.pos++; return Modifier.Minus; }
        return Modifier.None;
    }

    string? TryReadFieldPrefix()
    {
        var save = this.pos;
        var name = this.ReadTermText();
        if (name.Length > 0 && !this.Eof && this.Current == ':')
        {
            this.pos++; // consume ':'
            return name;
        }
        this.pos = save;
        return null;
    }

    // A term is a run of characters that are not whitespace or a syntax character. Interior '*'/'?'
    // (non-trailing wildcards) are rejected — only a single trailing '*' is supported (handled by the caller).
    string ReadTermText()
    {
        var start = this.pos;
        while (!this.Eof)
        {
            var c = this.Current;
            if (char.IsWhiteSpace(c) || c is '(' or ')' or '"' or ':' or '^' or '~' or '+')
                break;
            // Stop before a trailing '*' so the caller can turn the term into a prefix.
            if (c == '*')
                break;
            if (c == '?')
                throw this.Error("single-character wildcard '?' is not supported (only trailing '*')");
            // A '-' only breaks a term when it starts one (operator); mid-term hyphens are kept.
            if (c == '-' && this.pos == start)
                break;
            this.pos++;
        }
        // Reject a non-trailing wildcard: text* is fine, te*t is not.
        if (!this.Eof && this.Current == '*')
        {
            var after = this.pos + 1;
            if (after < this.text.Length && !char.IsWhiteSpace(this.text[after]) && this.text[after] is not (')' or '^'))
                throw this.Error("only a trailing '*' wildcard is supported (e.g. 'foo*')");
        }
        return this.text.Substring(start, this.pos - start).ToLowerInvariant();
    }

    bool TryReadNumber(out double value)
    {
        var start = this.pos;
        while (!this.Eof && (char.IsDigit(this.Current) || this.Current == '.'))
            this.pos++;
        if (this.pos == start)
        {
            value = 0;
            return false;
        }
        value = double.Parse(this.text.Substring(start, this.pos - start), CultureInfo.InvariantCulture);
        return true;
    }

    bool TryReadKeyword(string keyword)
    {
        this.SkipWhitespace();
        if (this.pos + keyword.Length > this.text.Length)
            return false;
        if (string.CompareOrdinal(this.text, this.pos, keyword, 0, keyword.Length) != 0)
            return false;
        // Must be followed by a word boundary so a term like "ANDroid" isn't read as the AND keyword.
        var after = this.pos + keyword.Length;
        if (after < this.text.Length && !char.IsWhiteSpace(this.text[after]) && this.text[after] is not ('(' or '"' or '-' or '+'))
            return false;
        this.pos = after;
        return true;
    }

    bool TryReadSymbol(string symbol)
    {
        this.SkipWhitespace();
        if (this.pos + symbol.Length > this.text.Length)
            return false;
        if (string.CompareOrdinal(this.text, this.pos, symbol, 0, symbol.Length) != 0)
            return false;
        this.pos += symbol.Length;
        return true;
    }

    void SkipWhitespace()
    {
        while (!this.Eof && char.IsWhiteSpace(this.Current))
            this.pos++;
    }

    bool Eof => this.pos >= this.text.Length;
    char Current => this.text[this.pos];

    FormatException Error(string message) =>
        new($"Invalid Lucene query at position {this.pos}: {message}. Query: \"{this.text}\"");
}
