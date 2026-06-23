using System.Text.Json.Serialization;
using Shiny.DocumentDb;

namespace Sample.ODataApi;

/// <summary>
/// Source-generated JSON metadata for the document types, so the store serializes and queries
/// <b>without reflection</b> (<c>UseReflectionFallback = false</c>) — the fast, AOT/trim-safe path.
/// <para>
/// camelCase matches the store's default naming. <see cref="DocumentSeedMarker"/> is included because the
/// run-once seeder persists it through the same store, so it too needs source-gen metadata once reflection
/// fallback is off. (The ASP.NET OData host itself still uses reflection for its EDM model — that part is
/// JIT-only by design; this only removes reflection from DocumentDb's own read/write path.)
/// </para>
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(Customer))]
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(DocumentSeedMarker))]
public partial class SampleJsonContext : JsonSerializerContext;
