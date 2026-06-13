using System.Text.Json;
using Orleans.Runtime;

namespace Shiny.DocumentDb.Orleans;

/// <summary>
/// Options for a named Orleans grain-storage provider backed by Shiny.DocumentDb.
/// <para>
/// Supply <b>either</b> a relational <see cref="DatabaseProvider"/> (the built-in path — covers SQLite,
/// SQL Server, PostgreSQL, MySQL, Oracle and DuckDB, with the envelope type/version mapping wired up for
/// you) <b>or</b> a <see cref="StoreFactory"/> that returns a fully-configured <see cref="IDocumentStore"/>
/// (the generic escape hatch for Cosmos/Mongo/LiteDB/IndexedDB, where the store is built with its own
/// provider-specific options).
/// </para>
/// </summary>
public class DocumentDbGrainStorageOptions
{
    /// <summary>
    /// A relational backend provider (e.g. <c>SqliteDatabaseProvider</c>, <c>PostgreSqlDatabaseProvider</c>).
    /// When set, the provider constructs its own <see cref="DocumentStore"/> with the
    /// <see cref="GrainStateRecord"/> type mapped to <see cref="TableName"/> and its <c>Version</c> mapped
    /// for optimistic concurrency. Ignored when <see cref="StoreFactory"/> is set.
    /// </summary>
    public IDatabaseProvider? DatabaseProvider { get; set; }

    /// <summary>
    /// Generic escape hatch: returns a fully-configured <see cref="IDocumentStore"/>. The factory is
    /// responsible for mapping <see cref="GrainStateRecord"/> (type→table/container and version property)
    /// on whatever options type that store uses. Use <see cref="DocumentDbGrainStorage.ConfigureGrainState"/>
    /// for the relational options shape; Cosmos/Mongo expose equivalent <c>MapVersionProperty</c> /
    /// <c>MapTypeToContainer</c> methods on their own options.
    /// </summary>
    public Func<IServiceProvider, IDocumentStore>? StoreFactory { get; set; }

    /// <summary>
    /// Table/collection/container name for the grain-state envelope. Defaults to
    /// <c>"orleans_{providerName}"</c>. Only used by the relational <see cref="DatabaseProvider"/> path.
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// When true (default), <c>ClearStateAsync</c> deletes the row. When false, it writes a tombstone
    /// (null state) and bumps the version, preserving the row and its ETag chain.
    /// </summary>
    public bool DeleteStateOnClear { get; set; } = true;

    /// <summary>
    /// JSON options used to (de)serialize the grain state into the envelope's nested document. When null,
    /// a permissive default is used. For the relational path this is also handed to the underlying store.
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>Silo lifecycle stage at which the store is initialized. Defaults to ApplicationServices.</summary>
    public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
}
