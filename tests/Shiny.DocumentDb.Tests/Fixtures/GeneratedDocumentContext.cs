namespace Shiny.DocumentDb.Tests.Fixtures;

// Exercises DocumentSerialization.Generated: NO JsonSerializerContext is supplied — the generator emits the
// metadata-mode JsonTypeInfo resolver itself. GenModel covers the tricky shapes (enum, nullable value type,
// array, List<T>, nested object) so the generated resolver's closure walk is fully exercised.
//
// Location/OptionalLocation/Boundary cover type-level [JsonConverter]: an immutable struct, a nullable of
// that struct, and an abstract polymorphic base. Before the converter fix these raised DDB005 at build time —
// this file failing to compile IS the regression test.
public class GenModel
{
    public string Id { get; set; } = "";
    public int Count { get; set; }
    public int? OptionalCount { get; set; }
    public Priority Level { get; set; }
    public decimal Price { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public List<string> Tags { get; set; } = [];
    public int[] Scores { get; set; } = [];
    public Address? Home { get; set; }
    public GeoPoint Location { get; set; }
    public GeoPoint? OptionalLocation { get; set; }
    public Geometry? Boundary { get; set; }

    // DocumentBlob and DocumentBlobCollection also carry type-level [JsonConverter]; the collection would
    // otherwise be rejected by the non-List<T> collection guard. That this compiles under Generated proves
    // the converter branch runs before that guard.
    public DocumentBlob? Attachment { get; set; }
    public DocumentBlobCollection Files { get; set; } = new();
}

[Document(typeof(GenModel), Serialization = DocumentSerialization.Generated)]
[Document(typeof(User), Serialization = DocumentSerialization.Generated)]
public partial class GeneratedDocumentContext : DocumentContext;
