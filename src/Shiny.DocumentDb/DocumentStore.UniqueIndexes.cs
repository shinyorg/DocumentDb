using System.Globalization;
using Shiny.DocumentDb.Internal;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb;

// Unique indexes (MapUniqueIndex) on the relational store. The engine enforces them: a native unique index is created
// once at table init, and the constraint error it raises on a violating write is translated back into a
// UniqueConstraintException naming the mapping.
public partial class DocumentStore
{
    // Unlike the rest of the init DDL a failure here is not swallowed: a unique index that silently failed to create
    // stops enforcing uniqueness, and the likeliest cause — documents already stored that share a key — is something
    // only the caller can resolve.
    async Task CreateUniqueIndexesAsync(DocumentStoreSession session, string tableName, CancellationToken ct)
    {
        if (!this.options.Mappings.HasUniqueIndexes || !this.provider.SupportsUniqueIndexes)
            return;

        foreach (var mapping in this.options.Mappings.UniqueIndexes)
        {
            var typeName = TypeNameResolver.Resolve(mapping.DocumentType, this.options.TypeNameResolution);
            if (this.options.ResolveTableName(typeName) != tableName)
                continue;

            try
            {
                var existsSql = this.provider.BuildIndexExistsSql(tableName, mapping.GetStorageName(tableName));
                if (existsSql != null)
                {
                    await using var probe = session.CreateCommand();
                    probe.CommandText = existsSql;
                    this.Log(existsSql);
                    var found = await probe.ExecuteScalarAsync(ct).ConfigureAwait(false);
                    if (found is not null and not DBNull && Convert.ToInt64(found, CultureInfo.InvariantCulture) > 0)
                        continue;
                }

                foreach (var sql in this.provider.BuildCreateUniqueIndexSql(tableName, typeName, this.BuildUniqueIndexSql(mapping, tableName)))
                {
                    await using var cmd = session.CreateCommand();
                    cmd.CommandText = sql;
                    this.Log(sql);
                    await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new DocumentConfigurationException(
                    $"Could not create unique index '{mapping.Name}' for '{mapping.DocumentType.Name}' on table '{tableName}'. " +
                    "If documents already stored there share a key, resolve the duplicates first; otherwise see the inner exception.",
                    ex);
            }
        }
    }

    // The key parts and filter lowered through the same pipeline a query uses, so the index reads each value exactly
    // the way a Where over the same property does.
    UniqueIndexSql BuildUniqueIndexSql(UniqueIndexMapping mapping, string tableName)
    {
        var typeInfo = this.jsonOptions.GetTypeInfo(mapping.DocumentType);
        var computed = this.options.ResolveComputedLookup(mapping.DocumentType);
        var keySql = mapping.KeySelectors
            .Select(key => SqlPredicateEmitter.EmitValueInline(ExpressionLowerer.LowerValue(key, this.jsonOptions, typeInfo, computed), this.provider))
            .ToList();
        var filterSql = mapping.Filter == null
            ? null
            : SqlPredicateEmitter.EmitPredicateInline(
                ExpressionLowerer.Lower(mapping.Filter.Body, this.jsonOptions, typeInfo, this.options.FunctionRegistry, computed),
                this.provider);

        return new UniqueIndexSql(mapping.GetStorageName(tableName), mapping.GetJsonPaths(this.jsonOptions), keySql, filterSql, this.tenantIdAccessor != null);
    }

    // Index DDL cannot run inside a unit's transaction — a benign failure aborts a PostgreSQL transaction and any DDL
    // implicitly commits a MySQL one — and TransactionalDocumentStore's own table init is deliberately minimal. So
    // before a transaction begins, fully initialize every table a unique index lives in.
    async Task EnsureUniqueIndexTablesAsync(DocumentStoreSession session, CancellationToken ct)
    {
        if (!this.options.Mappings.HasUniqueIndexes)
            return;

        var tables = this.options.Mappings.UniqueIndexes
            .Select(m => this.options.ResolveTableName(TypeNameResolver.Resolve(m.DocumentType, this.options.TypeNameResolution)))
            .Distinct(StringComparer.Ordinal);
        foreach (var table in tables)
            await this.EnsureTableInitializedAsync(session, table, ct).ConfigureAwait(false);
    }

    UniqueConstraintException? MatchUniqueViolation(Exception ex, string? documentId)
    {
        if (!this.options.Mappings.HasUniqueIndexes)
            return null;

        string TypeNameOf(UniqueIndexMapping m) => TypeNameResolver.Resolve(m.DocumentType, this.options.TypeNameResolution);
        return MatchUniqueViolation(ex, this.provider, this.options.Mappings.UniqueIndexes, TypeNameOf,
            m => this.options.ResolveTableName(TypeNameOf(m)), documentId);
    }

    // The backend names the index it rejected the write on, which is the mapping's storage name on its table.
    static UniqueConstraintException? MatchUniqueViolation(
        Exception ex,
        IDatabaseProvider provider,
        IEnumerable<UniqueIndexMapping> candidates,
        Func<UniqueIndexMapping, string> typeNameOf,
        Func<UniqueIndexMapping, string> tableNameOf,
        string? documentId)
    {
        if (ex is UniqueConstraintException)
            return null;

        foreach (var mapping in candidates)
        {
            if (provider.IsUniqueIndexViolation(ex, mapping.GetStorageName(tableNameOf(mapping))))
                return new UniqueConstraintException(typeNameOf(mapping), mapping, documentId, ex);
        }
        return null;
    }

    // MySQL's ON DUPLICATE KEY UPDATE and INSERT IGNORE fire on ANY unique key, not just (Id, TypeName): a native upsert
    // of a new document whose unique value is taken would silently merge into — or be skipped in favour of — the other
    // document. On such a provider a type with unique indexes takes the read-modify-write path instead.
    bool MustAvoidNativeUpsert(string typeName)
        => this.provider.UpsertConflictsOnAnyUniqueKey
            && this.options.Mappings.HasUniqueIndexes
            && this.options.Mappings.UniqueIndexes.Any(m => TypeNameResolver.Resolve(m.DocumentType, this.options.TypeNameResolution) == typeName);

    // An insert's duplicate-key error is either a unique index or the (Id, TypeName) primary key — only the latter
    // means the id is taken.
    Exception DuplicateInsertException(Exception ex, string typeName, string id)
        => (Exception?)this.MatchUniqueViolation(ex, id)
            ?? new InvalidOperationException($"A document of type '{typeName}' with Id '{id}' already exists.", ex);
}
