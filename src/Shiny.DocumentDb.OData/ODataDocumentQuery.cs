using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.OData;

/// <summary>
/// Drives the fluent <see cref="IDocumentQuery{T}"/> surface from a provider-neutral
/// <see cref="ODataQueryModel"/>. <see cref="Apply{T}"/> maps <c>$filter/$orderby/$top/$skip</c> onto an
/// existing query (no execution); <see cref="Execute{T}"/> runs the full pipeline including the
/// <c>$select</c> projection path and the pre-paging <c>$count</c> envelope value.
/// <para>AOT/trim-clean: member resolution flows through <see cref="JsonTypeInfo{T}"/> only.</para>
/// </summary>
public static class ODataDocumentQuery
{
    /// <summary>
    /// Applies <c>$filter</c>, <c>$orderby</c>, and the combined <c>$skip</c>/<c>$top</c> paging onto an
    /// existing fluent query. Does <b>not</b> run <c>$select</c> or <c>$count</c> (those need execution
    /// / a separate materialization path — see <see cref="Execute{T}"/>).
    /// </summary>
    public static IDocumentQuery<T> Apply<T>(
        this IDocumentQuery<T> query,
        ODataQueryModel model,
        JsonTypeInfo<T>? typeInfo = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(model);

        if (model.HasExpand)
            throw new ODataNotSupportedException(
                "$expand is not supported: documents are self-contained JSON with no relationships to expand.");

        var info = typeInfo ?? query.QueryTypeInfo
            ?? throw new InvalidOperationException(
                $"No JsonTypeInfo<{typeof(T).Name}> was supplied and none could be resolved from the query.");

        if (model.Filter != null)
        {
            var predicate = FilterExpressionBuilder.Build(model.Filter, info);
            query = query.Where(predicate);
        }

        foreach (var item in model.OrderBy)
        {
            var path = NormalizePath(item.PropertyPath);
            query = item.Descending
                ? query.OrderByDescending(path, info)
                : query.OrderBy(path, info);
        }

        if (model.Skip is > 0 || model.Top is not null)
        {
            var skip = model.Skip ?? 0;
            var take = model.Top ?? int.MaxValue;
            query = query.Paginate(skip, take);
        }

        return query;
    }

    /// <summary>
    /// Executes the full OData query: builds the query for <typeparamref name="T"/>, computes the
    /// pre-paging <c>$count</c> when requested, then materializes the page either as projected
    /// <see cref="JsonObject"/> shapes (<c>$select</c>) or as whole entities serialized through
    /// <paramref name="typeInfo"/>.
    /// </summary>
    public static async Task<ODataResult> Execute<T>(
        IDocumentStore store,
        ODataQueryModel model,
        JsonTypeInfo<T>? typeInfo = null,
        CancellationToken ct = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(model);

        if (model.HasExpand)
            throw new ODataNotSupportedException(
                "$expand is not supported: documents are self-contained JSON with no relationships to expand.");

        var query = store.Query(typeInfo);
        var info = typeInfo ?? query.QueryTypeInfo
            ?? throw new InvalidOperationException(
                $"No JsonTypeInfo<{typeof(T).Name}> was supplied and none could be resolved from the store.");

        // $filter + $orderby only — count must run before paging, so apply paging separately below.
        if (model.Filter != null)
            query = query.Where(FilterExpressionBuilder.Build(model.Filter, info));

        long? count = null;
        if (model.Count)
            count = await query.Count(ct).ConfigureAwait(false);

        foreach (var item in model.OrderBy)
        {
            var path = NormalizePath(item.PropertyPath);
            query = item.Descending ? query.OrderByDescending(path, info) : query.OrderBy(path, info);
        }

        if (model.Skip is > 0 || model.Top is not null)
            query = query.Paginate(model.Skip ?? 0, model.Top ?? int.MaxValue);

        IReadOnlyList<JsonNode> value;
        if (!string.IsNullOrWhiteSpace(model.Select))
        {
            var projected = await query.Project(model.Select!, info).ToList(ct).ConfigureAwait(false);
            var list = new List<JsonNode>(projected.Count);
            foreach (var obj in projected)
                list.Add(obj);
            value = list;
        }
        else if (query.SupportsRawJson)
        {
            // The envelope wants JSON and the store already has it. Materializing a T only to serialize it
            // straight back out is two passes and a discarded object graph per document, so read the stored
            // body instead — untouched on the relational providers and Cosmos.
            value = await query.ToJsonList(ct).ConfigureAwait(false);
        }
        else
        {
            // Encrypted properties: the stored body is ciphertext and only the typed materialization decrypts,
            // so the response has to be built from documents. It is written through the plaintext view — the
            // encrypting converters are symmetric, so serializing straight back through `info` would hand the
            // client a re-encrypted envelope instead of the value every other read API returns.
            var entities = await query.ToList(ct).ConfigureAwait(false);
            var outputInfo = DocumentEncryption.PlaintextView(info);
            var list = new List<JsonNode>(entities.Count);
            foreach (var entity in entities)
            {
                var node = JsonSerializer.SerializeToNode(entity, outputInfo)
                    ?? throw new InvalidOperationException($"Failed to serialize a '{typeof(T).Name}' document.");
                list.Add(node);
            }
            value = list;
        }

        return new ODataResult { Value = value, Count = count };
    }

    // OData paths use '/'; the fluent string overloads use '.'.
    static string NormalizePath(string path) => path.Replace('/', '.');
}
