namespace Shiny.DocumentDb.Internal.FullText;

/// <summary>
/// Evaluates a parsed <see cref="FtQuery"/> against a single document's text in-memory. Backs the
/// <c>DocumentFunctions.LuceneMatch</c>/<c>LuceneScore</c> BCL bodies (so LINQ-to-objects and the
/// LiteDB/IndexedDB providers execute Lucene queries directly) and is the behavioural oracle the pushdown
/// providers are tested against. Supports the full grammar except field scoping, which is unsupported on
/// every provider in v1 and therefore throws here too — keeping in-memory and pushdown results consistent.
/// </summary>
public static class FtQueryEvaluator
{
    public static bool Matches(FtQuery query, string? text)
    {
        var tokens = Tokenize(text);
        return Match(query.Root, tokens);
    }

    /// <summary>A frequency-based relevance heuristic (higher = better); 0 when the query does not match.
    /// No corpus is available in-memory, so this is term-frequency weighted by boosts, not true BM25/TF-IDF.</summary>
    public static double Score(FtQuery query, string? text)
    {
        var tokens = Tokenize(text);
        return Match(query.Root, tokens) ? Math.Max(double.Epsilon, ScoreNode(query.Root, tokens, 1.0)) : 0;
    }

    static bool Match(FtNode node, IReadOnlyList<string> tokens) => node switch
    {
        FtTerm t => tokens.Contains(t.Text),
        FtPrefix p => tokens.Any(tok => tok.StartsWith(p.Text, StringComparison.Ordinal)),
        FtPhrase ph => PhraseMatch(ph.Terms, tokens, slop: 0),
        FtProximity pr => PhraseMatch(pr.Terms, tokens, pr.Slop),
        FtFuzzy fz => tokens.Any(tok => Levenshtein(tok, fz.Text) <= fz.MaxEdits),
        FtBoost b => Match(b.Node, tokens),
        FtField => throw FieldUnsupported(),
        FtBool b => BoolMatch(b, tokens),
        _ => throw new NotSupportedException($"Full-text node '{node.GetType().Name}' cannot be evaluated in-memory.")
    };

    static bool BoolMatch(FtBool node, IReadOnlyList<string> tokens)
    {
        var hasMust = false;
        var anyShould = false;
        var shouldSeen = false;
        foreach (var clause in node.Clauses)
        {
            switch (clause.Occur)
            {
                case FtOccur.Must:
                    hasMust = true;
                    if (!Match(clause.Node, tokens))
                        return false;
                    break;
                case FtOccur.MustNot:
                    if (Match(clause.Node, tokens))
                        return false;
                    break;
                default:
                    shouldSeen = true;
                    anyShould |= Match(clause.Node, tokens);
                    break;
            }
        }
        // Should clauses are required only when there is no Must clause anchoring the match.
        return hasMust || !shouldSeen || anyShould;
    }

    static double ScoreNode(FtNode node, IReadOnlyList<string> tokens, double boost) => node switch
    {
        FtTerm t => boost * tokens.Count(tok => tok == t.Text),
        FtPrefix p => boost * tokens.Count(tok => tok.StartsWith(p.Text, StringComparison.Ordinal)),
        FtPhrase ph => boost * (PhraseMatch(ph.Terms, tokens, 0) ? 1 : 0),
        FtProximity pr => boost * (PhraseMatch(pr.Terms, tokens, pr.Slop) ? 1 : 0),
        FtFuzzy fz => boost * tokens.Count(tok => Levenshtein(tok, fz.Text) <= fz.MaxEdits),
        FtBoost b => ScoreNode(b.Node, tokens, boost * b.Factor),
        FtBool b => b.Clauses.Where(c => c.Occur != FtOccur.MustNot).Sum(c => ScoreNode(c.Node, tokens, boost)),
        FtField => throw FieldUnsupported(),
        _ => 0
    };

    // All phrase terms occur within a window of (terms.Count + slop) positions, in any order.
    static bool PhraseMatch(IReadOnlyList<string> terms, IReadOnlyList<string> tokens, int slop)
    {
        if (terms.Count == 0)
            return false;
        if (terms.Count == 1)
            return tokens.Contains(terms[0]);

        var window = terms.Count + slop;
        for (var start = 0; start + terms.Count <= tokens.Count + slop && start < tokens.Count; start++)
        {
            var end = Math.Min(tokens.Count, start + window);
            var slice = tokens.Skip(start).Take(end - start).ToList();
            if (terms.All(slice.Contains))
                return true;
        }
        return false;
    }

    static int Levenshtein(string a, string b)
    {
        var prev = new int[b.Length + 1];
        var curr = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++)
            prev[j] = j;
        for (var i = 1; i <= a.Length; i++)
        {
            curr[0] = i;
            for (var j = 1; j <= b.Length; j++)
            {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                curr[j] = Math.Min(Math.Min(prev[j] + 1, curr[j - 1] + 1), prev[j - 1] + cost);
            }
            (prev, curr) = (curr, prev);
        }
        return prev[b.Length];
    }

    static NotSupportedException FieldUnsupported() => new(
        "Field-scoped Lucene terms ('field:foo') are not supported in v1 — the query searches all mapped full-text properties.");

    static List<string> Tokenize(string? text)
    {
        var tokens = new List<string>();
        if (string.IsNullOrEmpty(text))
            return tokens;

        var start = -1;
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsLetterOrDigit(text[i]))
            {
                if (start < 0) start = i;
            }
            else if (start >= 0)
            {
                tokens.Add(text.Substring(start, i - start).ToLowerInvariant());
                start = -1;
            }
        }
        if (start >= 0)
            tokens.Add(text.Substring(start).ToLowerInvariant());
        return tokens;
    }
}
