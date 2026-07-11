namespace Shiny.DocumentDb.Aspire.Client;

/// <summary>
/// The backing database provider a DocumentDb store is configured against.
/// This MUST stay spelling-identical to <c>Shiny.DocumentDb.Aspire.Hosting.DocumentProviderKind</c> —
/// the host writes the value (<c>Shiny:DocumentDb:&lt;name&gt;:Provider</c>) and the client parses it here.
/// </summary>
public enum DocumentProviderKind
{
    Sqlite,
    Postgres,
    CockroachDb,
    SqlServer,
    MySql,
    MariaDb
    // TODO: Mongo/Cosmos follow-up — divergent client registration (MongoDbDocumentStoreOptions /
    // CosmosDbDocumentStoreOptions) not yet implemented.
}
