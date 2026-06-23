using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.OData.Query;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OData.Edm;
using Microsoft.OData.UriParser;
using Shiny.DocumentDb.OData;

namespace Shiny.DocumentDb.AspNetCore.OData;

/// <summary>
/// Endpoint mapping for document OData entity sets. A mapped endpoint reads the OData system query
/// options off the request, converts them into the engine's <see cref="ODataQueryModel"/>, executes
/// against the configured <see cref="IDocumentStore"/>, and writes the OData envelope
/// (<c>@odata.context</c>, <c>@odata.count</c>, <c>value</c>).
/// </summary>
public static class DocumentODataEndpointExtensions
{
    /// <summary>
    /// Maps a GET endpoint at <paramref name="pattern"/> serving <typeparamref name="T"/> as an OData
    /// entity set. Supports <c>$filter/$orderby/$top/$skip/$count/$select</c>; returns
    /// <c>501 Not Implemented</c> for <c>$expand</c> (no relationships) and spatial filters on a
    /// non-spatial provider.
    /// </summary>
    public static RouteHandlerBuilder MapDocumentODataEntitySet<T>(
        this IEndpointRouteBuilder endpoints,
        string pattern) where T : class
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);

        return endpoints.MapGet(pattern, async (HttpContext http, CancellationToken ct) =>
        {
            var docOptions = http.RequestServices.GetRequiredService<DocumentODataOptions>();
            var store = http.RequestServices.GetRequiredService<IDocumentStore>();

            var queryOptions = BuildQueryOptions<T>(http, docOptions);

            ODataQueryModel model;
            try
            {
                model = ODataModelAdapter.ToModel(queryOptions);
            }
            catch (ODataNotSupportedException ex)
            {
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status501NotImplemented);
            }
            catch (Microsoft.OData.ODataException ex)
            {
                // The Microsoft parser binds $expand/$select lazily; an $expand naming a non-navigation
                // property (documents have none) throws here. Surface it as 501, matching $expand's contract.
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status501NotImplemented);
            }

            try
            {
                var result = await ODataDocumentQuery.Execute<T>(store, model, ct: ct).ConfigureAwait(false);
                var envelope = BuildEnvelope(http, docOptions.GetEntitySetName<T>(), result);
                return Results.Json(envelope);
            }
            catch (ODataNotSupportedException ex)
            {
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status501NotImplemented);
            }
            catch (NotSupportedException ex)
            {
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status501NotImplemented);
            }
        });
    }

    static ODataQueryOptions<T> BuildQueryOptions<T>(HttpContext http, DocumentODataOptions docOptions) where T : class
    {
        var edmModel = docOptions.GetEdmModel();
        var entitySetName = docOptions.GetEntitySetName<T>();
        var entitySet = edmModel.EntityContainer.FindEntitySet(entitySetName)
            ?? throw new InvalidOperationException($"Entity set '{entitySetName}' not found in the EDM model.");

        var context = new ODataQueryContext(edmModel, typeof(T), new ODataPath(new EntitySetSegment(entitySet)));
        return new ODataQueryOptions<T>(context, http.Request);
    }

    static Dictionary<string, object?> BuildEnvelope(HttpContext http, string entitySetName, ODataResult result)
    {
        var basePath = $"{http.Request.Scheme}://{http.Request.Host}{http.Request.PathBase}/odata";
        var envelope = new Dictionary<string, object?>
        {
            ["@odata.context"] = $"{basePath}/$metadata#{entitySetName}"
        };
        if (result.Count is { } count)
            envelope["@odata.count"] = count;

        envelope["value"] = result.Value;
        return envelope;
    }
}
