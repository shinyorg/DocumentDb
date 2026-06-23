using Microsoft.AspNetCore.OData;
using Microsoft.Extensions.DependencyInjection;

namespace Shiny.DocumentDb.AspNetCore.OData;

/// <summary>
/// Registration entry points for exposing <see cref="IDocumentStore"/> document types as OData v4
/// query endpoints. Pairs with <c>MapDocumentODataEntitySet&lt;T&gt;</c> on the endpoint side.
/// </summary>
public static class DocumentODataServiceCollectionExtensions
{
    /// <summary>
    /// Registers the document OData services and builds the shared EDM model from the registered entity
    /// sets. Also wires the core <c>AddOData</c> services so the request pipeline can parse system query
    /// options.
    /// </summary>
    public static IServiceCollection AddDocumentODataEndpoints(
        this IServiceCollection services,
        Action<DocumentODataOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new DocumentODataOptions();
        configure(options);

        services.AddSingleton(options);

        var edmModel = options.GetEdmModel();
        services.AddControllers().AddOData(opt =>
            opt.AddRouteComponents("odata", edmModel)
               .Select()
               .Filter()
               .OrderBy()
               .Count()
               .SetMaxTop(null)
               .SkipToken()
               .Expand());

        return services;
    }
}
