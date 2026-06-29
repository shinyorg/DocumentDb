using System.Text.Json.Serialization;

namespace Shiny.DocumentDb;

/// <summary>
/// Controls how a document type declared on a strongly-typed <c>DocumentContext</c> is serialized to and
/// from JSON.
/// </summary>
/// <remarks>
/// AOT/trimming safety is a primary goal of DocumentDb, so the recommended modes resolve a source-generated
/// <see cref="System.Text.Json.Serialization.Metadata.JsonTypeInfo{T}"/> with no runtime reflection. The
/// store's query translator reads <c>JsonTypeInfo.Properties</c> to map CLR members to JSON paths, so the
/// resolved metadata must be in metadata mode (a value-converter wrapper has no properties and breaks every
/// property-based <c>Where</c>/<c>OrderBy</c>/<c>Select</c>). All three working modes below satisfy that.
/// <para>
/// <see cref="Reflection"/> is the explicit opt-out for callers who do not target AOT and do not want to
/// maintain a context — it is convenient but not trimming-safe. It exists because we cannot always solve for
/// a caller who will not declare a <c>JsonSerializerContext</c>; it is never the path you should ship in an
/// AOT/trimmed app.
/// </para>
/// </remarks>
public enum DocumentSerialization
{
    /// <summary>
    /// Inherit the store's behavior (default). The set resolves its <c>JsonTypeInfo&lt;T&gt;</c> from the
    /// resolver already on the store's <see cref="System.Text.Json.JsonSerializerOptions"/> — i.e. a
    /// <see cref="JsonSerializerContext"/> you registered (AOT-safe), and only if none is found does it fall
    /// back to reflection when <c>UseReflectionFallback</c> is enabled. Register a context and this mode is
    /// fully AOT-safe with zero extra annotation.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// You own serialization: the type's metadata comes from the <see cref="JsonSerializerContext"/> named in
    /// <see cref="DocumentAttribute.JsonContext"/> (or any context registered on the store). This is the
    /// recommended AOT-safe mode — Shiny.DocumentDb does not generate metadata for you; System.Text.Json's
    /// own source generator does, and the result drops straight into the store's resolver chain.
    /// </summary>
    JsonContext = 1,

    /// <summary>
    /// Force reflection-based serialization for this type, ignoring any registered resolver. Convenient for a
    /// caller who will not declare a context, but <b>not AOT/trimming-safe</b> — it produces hard-to-diagnose
    /// failures under trimming. Use only in apps that are not AOT/trim targets.
    /// </summary>
    Reflection = 2,

    /// <summary>
    /// <b>Planned, not yet emitted.</b> Reserved for a future <c>Shiny.DocumentDb.Generators</c> mode that
    /// generates the metadata-mode <c>JsonTypeInfo&lt;T&gt;</c> for you, giving AOT safety from the single
    /// <c>[Document]</c> declaration with no hand-written <see cref="JsonSerializerContext"/>. Until the
    /// generator ships this, prefer <see cref="JsonContext"/> for AOT.
    /// </summary>
    Generated = 3
}

/// <summary>
/// Declares a document type on a strongly-typed, source-generated <c>DocumentContext</c>. Each attribute both
/// registers the type (so a typed <c>DocumentSet&lt;T&gt;</c> is generated for it) and optionally overrides
/// its table/id mapping and serialization strategy. The attribute is sugar over <c>DocumentStoreOptions</c> —
/// it lowers into the same <c>MapTypeToTable</c>/<c>MapIdProperty</c> calls; fluent configuration still wins.
/// </summary>
/// <remarks>
/// Consumed at compile time by <c>Shiny.DocumentDb.Generators</c>; there is no runtime reflection over these
/// attributes. See <see cref="DocumentSerialization"/> for how each type's <c>JsonTypeInfo</c> is resolved and
/// the AOT trade-offs — AOT is the default goal, with <see cref="DocumentSerialization.Reflection"/> the
/// explicit escape hatch.
/// </remarks>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
public sealed class DocumentAttribute : Attribute
{
    /// <summary>
    /// Declares a document type managed by the context.
    /// </summary>
    /// <param name="documentType">The aggregate/document CLR type to expose as a typed set.</param>
    public DocumentAttribute(Type documentType) => this.DocumentType = documentType;

    /// <summary>The document CLR type this declaration configures.</summary>
    public Type DocumentType { get; }

    /// <summary>
    /// Overrides the backing table/collection name. Defaults to the store's <c>TypeNameResolution</c>.
    /// </summary>
    public string? Table { get; set; }

    /// <summary>
    /// Overrides the id property name (e.g. <c>nameof(User.Email)</c>). Defaults to convention.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>
    /// Overrides the generated <c>DocumentSet&lt;T&gt;</c> property name. Defaults to a pluralized type name
    /// (<c>User</c> → <c>Users</c>); set this to resolve collisions or awkward pluralization.
    /// </summary>
    public string? Set { get; set; }

    /// <summary>
    /// How this type is serialized — see <see cref="DocumentSerialization"/>. Defaults to
    /// <see cref="DocumentSerialization.Auto"/> (inherit the store's resolver/fallback).
    /// </summary>
    public DocumentSerialization Serialization { get; set; } = DocumentSerialization.Auto;

    /// <summary>
    /// The <see cref="JsonSerializerContext"/> that supplies this type's metadata when
    /// <see cref="Serialization"/> is <see cref="DocumentSerialization.JsonContext"/> (or <c>Auto</c> and you
    /// want to bind a specific context). Its source-generated <c>JsonTypeInfo&lt;T&gt;</c> is AOT-safe and is
    /// added to the store's resolver chain.
    /// </summary>
    public Type? JsonContext { get; set; }
}
