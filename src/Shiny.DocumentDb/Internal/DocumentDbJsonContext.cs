using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Source-generated metadata for the payloads DocumentDb serializes on its <i>own</i> behalf — the spatial
/// geometry it binds into query parameters, the blob metadata it reads out of a document body, and the scalar
/// values <c>DocumentStore.ToJsonLiteral</c> writes into an <c>ExecuteUpdate</c> / <c>SetProperty</c>
/// statement. Distinct from the caller's <c>JsonSerializerOptions</c>, which cover the document types themselves.
/// </summary>
/// <remarks>
/// Geometry and DocumentBlob carry a type-level <c>[JsonConverter]</c> and the scalars have built-in converters,
/// so each generated contract is a converter lookup and nothing ever reflects over a member. That is what makes
/// these paths AOT-clean without threading a <c>JsonTypeInfo</c> through every call site.
/// </remarks>
[JsonSerializable(typeof(Geometry))]
[JsonSerializable(typeof(DocumentBlob))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(DateTime))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(DateOnly))]
[JsonSerializable(typeof(TimeOnly))]
[JsonSerializable(typeof(TimeSpan))]
[JsonSerializable(typeof(Guid))]
internal sealed partial class DocumentDbJsonContext : JsonSerializerContext;
