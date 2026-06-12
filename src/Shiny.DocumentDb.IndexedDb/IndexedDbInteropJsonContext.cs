using System.Text.Json.Serialization;

namespace Shiny.DocumentDb.IndexedDb;

/// <summary>
/// Source-generated JsonTypeInfo metadata for the wire types this provider
/// marshals through [JSImport] as JSON strings. Used by IndexedDbDocumentStore
/// to encode outgoing DocumentRecord(s) before calling JS, and to decode
/// returned JSON arrays back into typed records — without reflection.
///
/// camelCase is required for backwards compatibility: the prior implementation
/// shipped records through Blazor's IJSRuntime, which defaults to camelCase
/// serialization. Existing IndexedDB databases store records with camelCase
/// keys ('data', 'typeName', etc.) — the context must read/write the same.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(DocumentRecord))]
[JsonSerializable(typeof(DocumentRecord[]))]
[JsonSerializable(typeof(HistoryRecord))]
[JsonSerializable(typeof(HistoryRecord[]))]
internal partial class IndexedDbInteropJsonContext : JsonSerializerContext;
