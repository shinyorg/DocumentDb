namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// The backing database provider a DocumentDb store is configured against.
/// This value is the host↔client contract: the hosting integration stamps it onto
/// the store resource (config key <c>Shiny:DocumentDb:&lt;name&gt;:Provider</c>) and the
/// client integration reads it to select the matching <c>IDatabaseProvider</c>.
/// </summary>
/// <remarks>
/// v1 covers the relational providers + SQLite, which all configure uniformly via
/// <c>DocumentStoreOptions.DatabaseProvider = new XProvider(connectionString)</c>.
/// MongoDB and Cosmos are deferred (divergent client registration / newer Aspire hosting packages).
/// </remarks>
public enum DocumentProviderKind
{
    Sqlite,
    Postgres,
    SqlServer,
    MySql
    // TODO: Mongo/Cosmos follow-up — these need MongoDbDocumentStoreOptions / CosmosDbDocumentStoreOptions
    // registration paths and the Aspire.Hosting.MongoDB / Azure Cosmos hosting packages.
}
