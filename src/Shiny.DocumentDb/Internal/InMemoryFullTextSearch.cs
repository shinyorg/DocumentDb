namespace Shiny.DocumentDb.Internal;

/// <summary>
/// TF-IDF relevance ranking used by the fallback providers (LiteDB, IndexedDB) that have no native
/// full-text engine. Terms are OR-combined: a document matches if it contains any query term, and its
/// score is the sum of <c>termFrequency × inverseDocumentFrequency</c> over the matched terms. Scores
/// are positive (higher = more relevant) and comparable only within one result set.
/// </summary>
static class InMemoryFullTextSearch
{
    public static IReadOnlyList<FullTextResult<T>> Rank<T>(
        IReadOnlyList<(T Document, string Text)> documents,
        string searchText,
        int maxResults) where T : class
    {
        var queryTerms = Tokenize(searchText).Distinct().ToList();
        if (queryTerms.Count == 0 || documents.Count == 0)
            return Array.Empty<FullTextResult<T>>();

        // Tokenize each document once, then compute document frequency per query term.
        var docTokens = new List<Dictionary<string, int>>(documents.Count);
        foreach (var (_, text) in documents)
        {
            var counts = new Dictionary<string, int>();
            foreach (var token in Tokenize(text))
                counts[token] = counts.TryGetValue(token, out var c) ? c + 1 : 1;
            docTokens.Add(counts);
        }

        var df = new Dictionary<string, int>();
        foreach (var term in queryTerms)
        {
            var count = 0;
            foreach (var counts in docTokens)
            {
                if (counts.ContainsKey(term))
                    count++;
            }
            df[term] = count;
        }

        var n = documents.Count;
        var scored = new List<FullTextResult<T>>();
        for (var i = 0; i < documents.Count; i++)
        {
            double score = 0;
            foreach (var term in queryTerms)
            {
                if (!docTokens[i].TryGetValue(term, out var tf) || df[term] == 0)
                    continue;
                // Smoothed IDF keeps a term that appears in every document from scoring zero.
                var idf = Math.Log(1.0 + (double)n / df[term]);
                score += tf * idf;
            }
            if (score > 0)
                scored.Add(new FullTextResult<T> { Document = documents[i].Document, Score = score });
        }

        scored.Sort((a, b) => b.Score.CompareTo(a.Score));
        if (scored.Count > maxResults)
            scored.RemoveRange(maxResults, scored.Count - maxResults);
        return scored;
    }

    static IEnumerable<string> Tokenize(string? text)
    {
        if (string.IsNullOrEmpty(text))
            yield break;

        var start = -1;
        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsLetterOrDigit(text[i]))
            {
                if (start < 0) start = i;
            }
            else if (start >= 0)
            {
                yield return text.Substring(start, i - start).ToLowerInvariant();
                start = -1;
            }
        }
        if (start >= 0)
            yield return text.Substring(start).ToLowerInvariant();
    }
}
