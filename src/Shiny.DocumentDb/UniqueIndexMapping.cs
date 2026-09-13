using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// A unique index declared over one or more JSON values of a document type, from
/// <see cref="DocumentTypeBuilder{T}.MapUniqueIndex"/> or <see cref="DocumentPropertyBuilder{T}.Unique"/>.
/// <para>
/// The rules are the same on every provider: the index is scoped to the one document type (and, on a
/// multi-tenant relational store, to the tenant), a document whose key has a <c>null</c> or missing value
/// is not constrained, a document the <see cref="Filter"/> rejects is not constrained, and values compare
/// exactly — case-sensitively — as they are stored in the JSON.
/// </para>
/// </summary>
public sealed class UniqueIndexMapping
{
    readonly Func<object, bool>? filterPredicate;
    (JsonSerializerOptions Options, IReadOnlyList<string> Paths)? resolved;

    internal UniqueIndexMapping(
        Type documentType,
        string name,
        ParameterExpression parameter,
        IReadOnlyList<Expression> keySelectors,
        IReadOnlyList<IReadOnlyList<string>> propertyChains,
        LambdaExpression? filter,
        Func<object, bool>? filterPredicate)
    {
        this.DocumentType = documentType;
        this.Name = name;
        this.Parameter = parameter;
        this.KeySelectors = keySelectors;
        this.PropertyChains = propertyChains;
        this.PropertyNames = propertyChains.Select(c => string.Join('.', c)).ToList();
        this.Filter = filter;
        this.filterPredicate = filterPredicate;
    }

    /// <summary>The document type the index belongs to.</summary>
    public Type DocumentType { get; }

    /// <summary>
    /// The index name — <c>uq_{Type}_{Properties}</c> (or <c>uq_{Type}_{name}</c> when one was given), capped at
    /// 51 characters. Deterministic, because it is how a backend's constraint error is traced back to this mapping; a
    /// relational index is created under <see cref="GetStorageName"/>.
    /// </summary>
    public string Name { get; }

    /// <summary>The CLR property paths making up the key, in order (<c>Email</c>, <c>Address.City</c>).</summary>
    public IReadOnlyList<string> PropertyNames { get; }

    /// <summary>The CLR property chains making up the key, in order — one member name per segment.</summary>
    public IReadOnlyList<IReadOnlyList<string>> PropertyChains { get; }

    /// <summary>The document parameter the <see cref="KeySelectors"/> are written against.</summary>
    public ParameterExpression Parameter { get; }

    /// <summary>One member-access expression per key part, over <see cref="Parameter"/>.</summary>
    public IReadOnlyList<Expression> KeySelectors { get; }

    /// <summary>Only documents matching this predicate are constrained; <c>null</c> constrains every document.</summary>
    public LambdaExpression? Filter { get; }

    /// <summary>True when <paramref name="document"/> passes the <see cref="Filter"/> (always true without one).</summary>
    public bool AppliesTo(object document) => this.filterPredicate?.Invoke(document) ?? true;

    /// <summary>
    /// The name of the native index backing this mapping on <paramref name="tableName"/>: <see cref="Name"/> plus a short
    /// hash of the table. PostgreSQL, Oracle and DuckDB scope index names to the schema rather than the table, so the
    /// same type stored in two tables needs two names — without this the second <c>CREATE INDEX IF NOT EXISTS</c>
    /// silently does nothing and the second table is never constrained.
    /// </summary>
    public string GetStorageName(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        return $"{this.Name}_{UniqueIndexKeys.HashKey(tableName)[..8]}";
    }

    /// <summary>
    /// The stored JSON path of each key part (<c>email</c>, <c>address.city</c>), resolved through the
    /// serializer so naming policies, <c>[JsonPropertyName]</c> and source-generated contexts are honored.
    /// </summary>
    public IReadOnlyList<string> GetJsonPaths(JsonSerializerOptions jsonOptions)
    {
        ArgumentNullException.ThrowIfNull(jsonOptions);
        var cached = this.resolved;
        if (cached is { } hit && ReferenceEquals(hit.Options, jsonOptions))
            return hit.Paths;

        var paths = this.PropertyChains.Select(chain => ResolvePath(jsonOptions, this.DocumentType, chain)).ToList();
        this.resolved = (jsonOptions, paths);
        return paths;
    }

    static string ResolvePath(JsonSerializerOptions jsonOptions, Type documentType, IReadOnlyList<string> chain)
    {
        try
        {
            return JsonPropertyNameResolver.BuildJsonPath(jsonOptions, jsonOptions.GetTypeInfo(documentType), chain.ToList());
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException)
        {
            // No metadata for the type (no resolver, or a source-gen context that omits it) — fall back to the
            // naming policy, the same way full-text and computed mappings resolve their names.
            return string.Join('.', chain.Select(segment => jsonOptions.PropertyNamingPolicy?.ConvertName(segment) ?? segment));
        }
    }
}
