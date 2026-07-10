namespace Shiny.DocumentDb.Internal.FullText;

/// <summary>
/// Walks an <see cref="FtQuery"/> AST and renders it to a provider's native full-text query dialect,
/// deferring only the leaf term/phrase spelling and the boolean joiners to the subclass. The boolean
/// flattening (Lucene <see cref="FtBool"/> Must/Should/MustNot semantics → AND/OR/AND-NOT) lives here so
/// every relational provider shares it. Operators the provider's <see cref="FtCapabilities"/> exclude are
/// rejected earlier by <see cref="FtCapabilityCheck"/>; the <c>throw</c> arms here are defensive.
/// </summary>
public abstract class FtSqlTranslator
{
    public string Write(FtNode node) => node switch
    {
        FtTerm t => this.Term(t.Text),
        FtPrefix p => this.Prefix(p.Text),
        FtPhrase ph => this.Phrase(ph.Terms),
        FtProximity pr => this.Proximity(pr.Terms, pr.Slop),
        FtFuzzy fz => this.Fuzzy(fz.Text, fz.MaxEdits),
        FtBoost b => this.Write(b.Node), // boosting affects ranking only; a boolean match ignores it
        FtBool b => this.WriteBool(b),
        FtField => throw new NotSupportedException("Field-scoped full-text terms are not supported."),
        _ => throw new NotSupportedException($"Full-text node '{node.GetType().Name}' cannot be translated.")
    };

    string WriteBool(FtBool node)
    {
        var must = new List<string>();
        var should = new List<string>();
        var mustNot = new List<string>();
        foreach (var clause in node.Clauses)
        {
            var sql = this.Group(this.Write(clause.Node));
            switch (clause.Occur)
            {
                case FtOccur.Must: must.Add(sql); break;
                case FtOccur.MustNot: mustNot.Add(sql); break;
                default: should.Add(sql); break;
            }
        }

        var basePart = must.Count > 0 ? this.And(must)
            : should.Count > 0 ? this.Or(should)
            : throw new NotSupportedException("A full-text query group with only NOT terms matches nothing.");

        foreach (var negated in mustNot)
            basePart = this.AndNot(basePart, negated);

        return basePart;
    }

    protected abstract string Term(string term);
    protected abstract string Prefix(string term);
    protected abstract string Phrase(IReadOnlyList<string> terms);
    protected abstract string And(IReadOnlyList<string> parts);
    protected abstract string Or(IReadOnlyList<string> parts);

    /// <summary><c>basePart AND NOT negated</c> in the provider's dialect.</summary>
    protected abstract string AndNot(string basePart, string negated);

    protected virtual string Proximity(IReadOnlyList<string> terms, int slop) =>
        throw new NotSupportedException("Proximity queries are not supported by this provider.");

    protected virtual string Fuzzy(string term, int maxEdits) =>
        throw new NotSupportedException("Fuzzy queries are not supported by this provider.");

    protected virtual string Group(string sql) => "(" + sql + ")";
}
