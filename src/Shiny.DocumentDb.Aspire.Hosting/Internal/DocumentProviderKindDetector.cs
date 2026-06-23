using Aspire.Hosting.ApplicationModel;

namespace Shiny.DocumentDb.Aspire.Hosting.Internal;

/// <summary>
/// Maps an Aspire database resource to its <see cref="DocumentProviderKind"/> by resource type name,
/// mirroring the Orleans hosting integration's <c>DatabaseTypeDetector</c>.
/// </summary>
internal static class DocumentProviderKindDetector
{
    public static DocumentProviderKind Detect(IResourceBuilder<IResourceWithConnectionString> database)
    {
        var resourceType = database.Resource.GetType().Name;

        return resourceType switch
        {
            _ when resourceType.StartsWith("Postgres", StringComparison.OrdinalIgnoreCase) => DocumentProviderKind.Postgres,
            _ when resourceType.StartsWith("SqlServer", StringComparison.OrdinalIgnoreCase) => DocumentProviderKind.SqlServer,
            _ when resourceType.StartsWith("MySql", StringComparison.OrdinalIgnoreCase) => DocumentProviderKind.MySql,
            _ when resourceType.StartsWith("Sqlite", StringComparison.OrdinalIgnoreCase) => DocumentProviderKind.Sqlite,
            // ConnectionStringResource / ParameterResource (e.g. SQLite modeled as a parameter) can't be
            // auto-detected — the caller must pass an explicit DocumentProviderKind.
            _ => throw new InvalidOperationException(
                $"Could not auto-detect a DocumentProviderKind from resource type '{resourceType}'. " +
                "Pass an explicit kind to AsDocumentStore(name, kind). " +
                "Supported auto-detected types are Postgres, SqlServer, MySql and Sqlite.")
        };
    }
}
