using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Source-generated metadata for the payloads DocumentDb serializes on its <i>own</i> behalf — the spatial
/// geometry it binds into query parameters and the blob metadata it reads out of a document body. Distinct
/// from the caller's <c>JsonSerializerOptions</c>, which cover the document types themselves.
/// </summary>
/// <remarks>
/// Both types carry a type-level <c>[JsonConverter]</c>, so each generated contract is a converter lookup and
/// nothing ever reflects over their members. That is what makes these paths AOT-clean without threading a
/// <c>JsonTypeInfo</c> through every call site.
/// </remarks>
[JsonSerializable(typeof(Geometry))]
[JsonSerializable(typeof(DocumentBlob))]
internal sealed partial class DocumentDbJsonContext : JsonSerializerContext;
