using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.DocumentDb.Internal;

/// <summary>One mapped property: what it is, how it is encrypted, and by which key ring.</summary>
sealed record EncryptedProperty(
    Type DeclaringType,
    string ClrName,
    Type PropertyType,
    EncryptionMode Mode,
    IDocumentEncryptor Encryptor);

/// <summary>
/// The mapping table behind <c>MapEncryptedProperty</c>, the <see cref="JsonTypeInfo"/> modifier that installs the
/// converters, and the lookup the predicate rewriter uses.
/// </summary>
/// <remarks>
/// Mappings are process-wide and keyed by document type, matching the soft-delete registry: which properties of a
/// type are sensitive is a property of the type, not of one store. Two stores mapping the same property the same
/// way is a no-op; mapping it differently throws rather than letting one store silently write values the other
/// cannot read.
/// </remarks>
static class EncryptionRegistry
{
    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, EncryptedProperty>> byType = new();

    // Marks the JsonSerializerOptions instances whose resolver already carries our modifier, so mapping several
    // properties (or configuring several stores over one options object) installs it exactly once.
    static readonly ConditionalWeakTable<JsonSerializerOptions, object> installed = new();

    // The UseEncryptor default, per options instance.
    static readonly ConditionalWeakTable<object, IDocumentEncryptor> defaults = new();

    // The same options minus the modifier, captured before it goes on. Serializing an already-materialized
    // (decrypted) document through it writes plaintext instead of re-encrypting — see PlaintextView.
    static readonly ConditionalWeakTable<JsonSerializerOptions, JsonSerializerOptions> plaintextViews = new();

    public static void SetDefaultEncryptor(IDocumentStoreOptions options, IDocumentEncryptor encryptor)
    {
        defaults.Remove(options);
        defaults.Add(options, encryptor);
    }

    public static void Map<T>(
        IDocumentStoreOptions options,
        Expression<Func<T, object?>> property,
        EncryptionMode mode,
        IDocumentEncryptor? encryptor) where T : class
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(property);

        var member = ResolveProperty(property);
        var resolved = encryptor ?? ResolveDefaultEncryptor(options);

        if (!EncryptedConverterFactory.IsSupported(member.PropertyType))
            throw new ArgumentException(
                $"'{typeof(T).Name}.{member.Name}' is a {member.PropertyType.Name}, which field-level encryption does not support. " +
                "Encryptable properties are string, bool, int, long, double, decimal, Guid, DateTime, DateTimeOffset and their nullable forms.",
                nameof(property));

        if (mode == EncryptionMode.Deterministic && member.PropertyType != typeof(string))
            throw new ArgumentException(
                $"Deterministic encryption is supported on string properties only, and '{typeof(T).Name}.{member.Name}' is a {member.PropertyType.Name}. " +
                "A non-string property would have to be compared against a ciphertext string, which changes the type of the predicate. " +
                "Hold it as a string, or map it as Randomized and stop querying it.",
                nameof(mode));

        var mapping = new EncryptedProperty(typeof(T), member.Name, member.PropertyType, mode, resolved);
        var forType = byType.GetOrAdd(typeof(T), _ => new ConcurrentDictionary<string, EncryptedProperty>(StringComparer.Ordinal));
        var existing = forType.GetOrAdd(member.Name, mapping);

        if (!ReferenceEquals(existing, mapping) && (existing.Mode != mapping.Mode || !ReferenceEquals(existing.Encryptor, mapping.Encryptor)))
            throw new InvalidOperationException(
                $"'{typeof(T).Name}.{member.Name}' is already mapped for encryption with a different mode or encryptor. " +
                "A property has one encryption mapping — documents written under one and read under the other would not decrypt.");

        InstallModifier(options);

        // Registered for both modes: deterministic predicates are rewritten into ciphertext comparisons, and a
        // predicate over a randomized property is rejected with an explanation instead of silently matching nothing.
        DocumentPredicateRewriters.Register<T>("encryption", new EncryptedPredicateRewriter<T>());
    }

    public static bool TryGet(Type documentType, string clrName, out EncryptedProperty property)
    {
        property = null!;
        return byType.TryGetValue(documentType, out var forType) && forType.TryGetValue(clrName, out property!);
    }

    public static IReadOnlyCollection<EncryptedProperty> For(Type documentType)
        => byType.TryGetValue(documentType, out var forType) ? forType.Values.ToArray() : [];

    /// <summary>
    /// The write-plaintext twin of <paramref name="options"/> — the same resolver without the encrypting
    /// converters, so serializing an already-materialized document emits its values rather than re-encrypting
    /// them. Returns <paramref name="options"/> unchanged when no encryption is configured on it.
    /// </summary>
    /// <remarks>
    /// The converters are symmetric: <c>Read</c> decrypts and <c>Write</c> encrypts. That is right for the
    /// store — the persisted body must hold ciphertext — and wrong for everything downstream of a
    /// materialized document, where a round trip back through the same options silently re-encrypts. This is
    /// the writer those paths need. It does <b>not</b> decrypt on read; feed it objects, never stored bodies.
    /// </remarks>
    public static JsonSerializerOptions PlaintextView(JsonSerializerOptions options)
        => plaintextViews.TryGetValue(options, out var view) ? view : options;

    /// <summary>The <see cref="JsonTypeInfo"/> modifier — swaps in the encrypting converter for every mapped property.</summary>
    public static void ApplyConverters(JsonTypeInfo typeInfo)
    {
        if (!byType.TryGetValue(typeInfo.Type, out var forType))
            return;

        foreach (var property in typeInfo.Properties)
        {
            // Match on the CLR name: the mapping is declared with a member selector, while JsonPropertyInfo.Name
            // has already been through the naming policy / [JsonPropertyName].
            var clrName = (property.AttributeProvider as PropertyInfo)?.Name ?? property.Name;
            if (forType.TryGetValue(clrName, out var mapped))
                property.CustomConverter = EncryptedConverterFactory.Create(property.PropertyType, mapped);
        }
    }

    static void InstallModifier(IDocumentStoreOptions options)
    {
        var json = options.EnsureSerializerOptions();
        if (installed.TryGetValue(json, out _))
            return;

        var resolver = json.TypeInfoResolver
            ?? throw new InvalidOperationException(
                "Field-level encryption needs a JsonSerializerOptions with a TypeInfoResolver — it installs a JsonTypeInfo " +
                "modifier to attach the encrypting converters. Set JsonSerializerOptions.TypeInfoResolver to your " +
                "JsonSerializerContext (the AOT-safe path) before calling MapEncryptedProperty.");

        // Capture the un-modified view first — once the modifier is on there is no way back to a resolver
        // that writes plaintext, and both options instances have to be built while json is still mutable.
        var plaintext = new JsonSerializerOptions(json) { TypeInfoResolver = resolver };

        try
        {
            json.TypeInfoResolver = resolver.WithAddedModifier(ApplyConverters);
        }
        catch (InvalidOperationException ex)
        {
            throw new InvalidOperationException(
                "The store's JsonSerializerOptions are already in use and can no longer be modified. Call " +
                "MapEncryptedProperty while configuring the store (before the first read or write), not afterwards.", ex);
        }

        plaintextViews.Add(json, plaintext);
        installed.Add(json, new object());
    }

    static IDocumentEncryptor ResolveDefaultEncryptor(IDocumentStoreOptions options)
        => defaults.TryGetValue(options, out var encryptor)
            ? encryptor
            : throw new InvalidOperationException(
                "No encryptor was supplied and none is configured for this store. Call options.UseEncryptor(...) once, " +
                "or pass an IDocumentEncryptor to MapEncryptedProperty.");

    static PropertyInfo ResolveProperty<T>(Expression<Func<T, object?>> property)
    {
        // x => x.Ssn arrives as Convert(member, object) because of the object-typed lambda.
        var body = property.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            body = unary.Operand;

        if (body is not MemberExpression { Member: PropertyInfo info } member || member.Expression is not ParameterExpression)
            throw new ArgumentException(
                $"An encrypted property must be a direct property of '{typeof(T).Name}' (e.g. x => x.Ssn). Nested paths are not " +
                "supported — map the property on the nested type instead.",
                nameof(property));

        return info;
    }
}
