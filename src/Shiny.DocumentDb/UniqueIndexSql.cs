namespace Shiny.DocumentDb;

/// <summary>
/// Everything a relational provider needs to emit the DDL for one <see cref="UniqueIndexMapping"/> — see
/// <see cref="IDatabaseProvider.BuildCreateUniqueIndexSql"/>. The SQL fragments are already lowered to the
/// provider's own JSON dialect with every literal inlined, because index DDL cannot bind parameters.
/// </summary>
/// <param name="Name">The index name (<see cref="UniqueIndexMapping.Name"/>).</param>
/// <param name="JsonPaths">The stored JSON path of each key part, in order.</param>
/// <param name="KeySql">Each key part as the query layer reads it (typed extraction over <c>Data</c>), in order.</param>
/// <param name="FilterSql">The index filter as a boolean SQL expression over <c>Data</c>, or <c>null</c> for none.</param>
/// <param name="TenantScoped">True when the store is multi-tenant, so the key must lead with <c>TenantId</c>.</param>
public sealed record UniqueIndexSql(
    string Name,
    IReadOnlyList<string> JsonPaths,
    IReadOnlyList<string> KeySql,
    string? FilterSql,
    bool TenantScoped);
