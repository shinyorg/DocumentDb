namespace Shiny.DocumentDb.Internal.FullText;

/// <summary>
/// The parsed, normalized representation of a Lucene query string produced by
/// <see cref="LuceneQueryParser"/>. Backends translate this AST to their native full-text dialect
/// (FTS5 MATCH, <c>to_tsquery</c>, SQL Server <c>CONTAINS</c>, Oracle <c>CONTAINS</c>, …) or evaluate it
/// in-memory (<see cref="FtQueryEvaluator"/>). It is deliberately provider-agnostic — no Lucene library
/// is referenced anywhere.
/// </summary>
public abstract record FtNode;

/// <summary>A single search term: <c>quick</c>.</summary>
public sealed record FtTerm(string Text) : FtNode;

/// <summary>A trailing-wildcard prefix term: <c>qui*</c> (matches any token starting with the text).</summary>
public sealed record FtPrefix(string Text) : FtNode;

/// <summary>An exact phrase of adjacent terms: <c>"quick brown"</c>.</summary>
public sealed record FtPhrase(IReadOnlyList<string> Terms) : FtNode;

/// <summary>A proximity phrase — the terms within <see cref="Slop"/> positions: <c>"quick fox"~5</c>.</summary>
public sealed record FtProximity(IReadOnlyList<string> Terms, int Slop) : FtNode;

/// <summary>A fuzzy term matching within <see cref="MaxEdits"/> edit distance: <c>roam~</c> / <c>roam~1</c>.</summary>
public sealed record FtFuzzy(string Text, int MaxEdits) : FtNode;

/// <summary>A field-scoped sub-query: <c>title:quick</c>. Not supported by any provider in v1 (single combined index).</summary>
public sealed record FtField(string Field, FtNode Node) : FtNode;

/// <summary>A relevance-boosted sub-query: <c>quick^2</c>. A no-op for boolean match; affects scoring only.</summary>
public sealed record FtBoost(FtNode Node, double Factor) : FtNode;

/// <summary>How a clause participates in its parent <see cref="FtBool"/> (Lucene BooleanQuery semantics).</summary>
public enum FtOccur { Must, Should, MustNot }

public sealed record FtClause(FtOccur Occur, FtNode Node);

/// <summary>
/// A boolean combination of clauses. A document matches when every <see cref="FtOccur.Must"/> clause
/// matches, no <see cref="FtOccur.MustNot"/> clause matches, and — if there are no Must clauses — at
/// least one <see cref="FtOccur.Should"/> clause matches. (Should clauses affect only scoring when a
/// Must clause is present.)
/// </summary>
public sealed record FtBool(IReadOnlyList<FtClause> Clauses) : FtNode;

/// <summary>A parsed Lucene query: the AST <see cref="Root"/> plus the original <see cref="RawText"/> (for diagnostics).</summary>
public sealed record FtQuery(FtNode Root, string RawText);

/// <summary>
/// The advanced operators a provider's full-text engine can express. Terms, phrases, and boolean
/// combination (AND/OR/NOT + grouping) are the assumed baseline for any full-text provider and are not
/// flagged here; only operators that some backends lack carry a flag.
/// </summary>
[Flags]
public enum FtCapabilities
{
    None = 0,
    /// <summary>Trailing-wildcard prefix terms (<c>foo*</c>).</summary>
    Prefix = 1,
    /// <summary>Fuzzy terms (<c>foo~</c>).</summary>
    Fuzzy = 2,
    /// <summary>Proximity phrases (<c>"a b"~N</c>).</summary>
    Proximity = 4,
    /// <summary>Per-term relevance boosting (<c>foo^2</c>) honored in scoring.</summary>
    Boost = 8,
    /// <summary>Field-scoped terms (<c>title:foo</c>).</summary>
    Field = 16,
    /// <summary>Everything the in-memory evaluator supports.</summary>
    All = Prefix | Fuzzy | Proximity | Boost | Field
}

/// <summary>Validates a parsed query against a provider's <see cref="FtCapabilities"/>, throwing a clear
/// <see cref="NotSupportedException"/> for the first operator the provider cannot express.</summary>
public static class FtCapabilityCheck
{
    /// <param name="forScore">When false (a boolean <c>LuceneMatch</c>), <see cref="FtBoost"/> is ignored
    /// because boosting only influences ranking; when true (<c>LuceneScore</c>) an unsupported boost throws.</param>
    public static void EnsureSupported(FtQuery query, FtCapabilities caps, string providerName, bool forScore)
    {
        Walk(query.Root, caps, providerName, forScore);

        // A query that can only exclude (top-level MustNot with nothing to match) selects nothing and is
        // rejected by most engines — surface it as a clear authoring error rather than an opaque SQL failure.
        if (query.Root is FtBool b && b.Clauses.All(c => c.Occur == FtOccur.MustNot))
            throw new NotSupportedException(
                "A purely negative full-text query (only NOT / '-' terms) matches nothing — add at least one positive term.");
    }

    static void Walk(FtNode node, FtCapabilities caps, string provider, bool forScore)
    {
        switch (node)
        {
            case FtPrefix when !caps.HasFlag(FtCapabilities.Prefix):
                throw Unsupported("prefix wildcard ('foo*')", provider);
            case FtFuzzy when !caps.HasFlag(FtCapabilities.Fuzzy):
                throw Unsupported("fuzzy ('foo~')", provider);
            case FtProximity when !caps.HasFlag(FtCapabilities.Proximity):
                throw Unsupported("proximity ('\"a b\"~N')", provider);
            case FtField when !caps.HasFlag(FtCapabilities.Field):
                throw Unsupported("field-scoped terms ('field:foo')", provider);
            case FtField f:
                Walk(f.Node, caps, provider, forScore);
                break;
            case FtBoost when forScore && !caps.HasFlag(FtCapabilities.Boost):
                throw Unsupported("boost ('foo^2')", provider);
            case FtBoost boost:
                Walk(boost.Node, caps, provider, forScore);
                break;
            case FtBool b:
                foreach (var clause in b.Clauses)
                    Walk(clause.Node, caps, provider, forScore);
                break;
        }
    }

    static NotSupportedException Unsupported(string op, string provider) => new(
        $"The Lucene {op} operator is not supported by {provider}'s full-text engine. " +
        "Remove it or use a provider that supports it (see the full-text capability matrix).");
}
