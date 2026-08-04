using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// One sweep over a store's configuration when it is built, reporting <b>every</b> problem at once through
/// <see cref="DocumentConfigurationException"/>.
/// <para>
/// It exists because <see cref="DocumentTypeBuilder{T}"/> is provider-agnostic on purpose: the builder accepts
/// any mapping, and this is where "LiteDB has no vector search" or "you cannot full-text index a randomized-
/// encrypted property" gets said — once, at startup, with every offending type and property named, instead of
/// as a surprise on the first query.
/// </para>
/// </summary>
public static class DocumentConfigurationValidator
{
    /// <summary>Validates <paramref name="options"/> against its backend, throwing if anything is wrong.</summary>
    public static void Validate(IDocumentStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var errors = Collect(options);
        if (errors.Count > 0)
            throw new DocumentConfigurationException(errors);
    }

    /// <summary>The problems, without throwing — for tooling that wants to report rather than fail.</summary>
    public static IReadOnlyList<string> Collect(IDocumentStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var caps = options.Capabilities;
        var mappings = options.Mappings;
        var errors = new List<string>();

        void Unsupported(bool supported, IEnumerable<Type> types, string feature, string alternative)
        {
            if (supported)
                return;
            foreach (var type in types)
                errors.Add($"'{type.Name}' maps {feature}, which {caps.ProviderName} does not support. {alternative}");
        }

        Unsupported(caps.Spatial, mappings.SpatialMappings.Keys, "a spatial property",
            "Remove the mapping, or move the type to a spatial-capable provider (SQLite, PostgreSQL, SQL Server, Oracle, MySQL, Cosmos, MongoDB, Redis).");
        Unsupported(caps.Vector, mappings.VectorMappings.Keys, "a vector property",
            "Remove the mapping, or move the type to a vector-capable provider (the relational providers, Cosmos, MongoDB Atlas, Redis).");
        Unsupported(caps.FullText, mappings.FullTextMappings.Keys, "a full-text index",
            "Remove the mapping, or filter with Where/Contains instead.");
        Unsupported(caps.Temporal, mappings.TemporalTypes, "temporal history",
            "Remove MapTemporal, or move the type to a temporal-capable provider (the relational providers, Cosmos, MongoDB, LiteDB, IndexedDB).");
        Unsupported(caps.Blobs, mappings.BlobMappings.Keys, "a blob property",
            "Remove the mapping, or store the payload in the document body.");
        Unsupported(caps.ComputedProperties, mappings.Computed.All().Select(m => m.DocumentType).Distinct(),
            "a computed property", "Remove the mapping, or compute the value in your own code after the read.");

        if (!caps.PerTypeStorageName)
        {
            foreach (var name in mappings.MappedNames)
                errors.Add($"A document type is mapped to '{name}', but {caps.ProviderName} has no per-type storage unit — every type shares one. Remove cfg.Table.");
        }

        CollectEncryptionConflicts(mappings, errors);
        return errors;
    }

    // A randomized-encrypted value is ciphertext everywhere it is stored, so anything the database has to read
    // *through* — an index, a computed expression, a spatial/vector payload — is meaningless over it. Catching it
    // here beats an empty result set or a runtime "cannot filter an encrypted property" on first use.
    static void CollectEncryptionConflicts(DocumentMappingRegistry mappings, List<string> errors)
    {
        static bool IsOpaque(Type documentType, string clrName)
            => EncryptionRegistry.TryGet(documentType, clrName, out var mapped) && mapped.Mode == EncryptionMode.Randomized;

        foreach (var (type, fullText) in mappings.FullTextMappings)
        {
            foreach (var property in fullText.PropertyNames.Where(p => IsOpaque(type, p)))
                errors.Add($"'{type.Name}.{property}' is randomized-encrypted and cannot feed a full-text index — the index would only ever see ciphertext. Map it Deterministic, or drop it from the index.");
        }

        foreach (var (type, spatial) in mappings.SpatialMappings)
        {
            if (IsOpaque(type, spatial.PropertyName))
                errors.Add($"'{type.Name}.{spatial.PropertyName}' is randomized-encrypted and cannot be a spatial property — the coordinates would be stored as ciphertext.");
        }

        foreach (var (type, vector) in mappings.VectorMappings)
        {
            if (IsOpaque(type, vector.PropertyName))
                errors.Add($"'{type.Name}.{vector.PropertyName}' is randomized-encrypted and cannot be a vector property — the ANN index would only ever see ciphertext.");
        }

        foreach (var computed in mappings.Computed.All())
        {
            foreach (var referenced in ComputedPropertyReferences(computed).Where(p => IsOpaque(computed.DocumentType, p)))
                errors.Add($"Computed property '{computed.DocumentType.Name}.{computed.PropertyName}' reads '{referenced}', which is randomized-encrypted — the database cannot evaluate the expression over ciphertext. Map '{referenced}' Deterministic, or drop the computed property.");
        }

        foreach (var version in mappings.VersionMappings)
        {
            if (IsOpaque(version.DocumentType, version.PropertyName))
                errors.Add($"'{version.DocumentType.Name}.{version.PropertyName}' is the optimistic-concurrency version and cannot be randomized-encrypted — the store compares it on every update.");
        }
    }

    static IEnumerable<string> ComputedPropertyReferences(ComputedMapping mapping)
        => new MemberNameCollector().Collect(mapping.Definition);

    sealed class MemberNameCollector : System.Linq.Expressions.ExpressionVisitor
    {
        readonly HashSet<string> names = new(StringComparer.Ordinal);

        public IReadOnlyCollection<string> Collect(System.Linq.Expressions.LambdaExpression definition)
        {
            this.Visit(definition.Body);
            return this.names;
        }

        protected override System.Linq.Expressions.Expression VisitMember(System.Linq.Expressions.MemberExpression node)
        {
            // Only direct members of the document parameter — a nested a.B.C is not an encryptable mapping target.
            if (node.Expression is System.Linq.Expressions.ParameterExpression)
                this.names.Add(node.Member.Name);
            return base.VisitMember(node);
        }
    }
}
