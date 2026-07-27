namespace ShinyDocDbMyAdmin.Providers;

/// <summary>
/// The relational Shiny.DocumentDb backends this tool can administer. Deliberately not the full
/// provider set - the document stores (Mongo, Cosmos, LiteDb, ...) do not expose the shared
/// <c>Id / TypeName / Data / CreatedAt / UpdatedAt</c> envelope over ADO.NET that everything here
/// is built on.
/// </summary>
public enum ProviderKind
{
    Sqlite,
    SqlCipher,
    PostgreSql,
    SqlServer,
    MySql,
    MariaDb,
    Oracle,
    DuckDb,
    CockroachDb
}
