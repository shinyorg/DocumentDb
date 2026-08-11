// Native AOT regression guard. This is deliberately a breadth sample rather than a deep one: it touches
// every mapping kind and every query surface once, because a trim/AOT warning only surfaces for code the
// ILC compiler can actually reach. Publishing it is the check — running it just proves the reached code
// still behaves. Adding a feature to the store means adding a line here.
//
//   dotnet publish samples/Sample.Aot -r osx-arm64 -c Release   (any RID)
//
// Trim/AOT warnings are errors in this project, so a regression fails the publish.

using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.VectorData;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Extensions.VectorData;
using Shiny.DocumentDb.Hosting;
using Shiny.DocumentDb.Sqlite;

var dbPath = Path.Combine(Path.GetTempPath(), $"documentdb-aot-{Guid.NewGuid():N}.db");
var services = new ServiceCollection();
services.AddDocumentStore(o =>
{
    o.DatabaseProvider = new SqliteDatabaseProvider($"Data Source={dbPath}");
    // An AOT app supplies a source-generated resolver — never the reflection default.
    o.JsonSerializerOptions = new() { TypeInfoResolver = AotSampleJsonContext.Default };
    o.ConfigureDocument<Person>(cfg =>
    {
        cfg.Table = cfg.TypeName;
        cfg.MapVersionProperty(x => x.Version);
        cfg.MapSpatialProperty(x => x.Area);
        cfg.MapVectorProperty(x => x.Embedding, 4);
        cfg.MapFullTextProperty(x => x.Name);
        cfg.MapComputedProperty<int>(x => x.AgeDoubled, x => x.Age * 2);
        cfg.MapTemporal();
        cfg.MapBlob(x => x.Photo);
        cfg.AddQueryFilter(x => x.Age > 0);
    });
    o.MapVectorRecord<AiNote>();
});

var sp = services.BuildServiceProvider();
var store = sp.GetRequiredService<IDocumentStore>();

// ── Typed lane ──────────────────────────────────────────────────────────
await store.Insert(new Person { Id = "1", Name = "ada", Age = 36 });
var fetched = await store.Get<Person>("1");
var filtered = await store.Query<Person>().Where(x => x.Age > 1).OrderBy(x => x.Name).ToList();
var counted = await store.Query<Person>().Count();

var grouped = await GroupedProjection(store);

// ── String-expression grammar (lowers to the same IR as the LINQ surface) ──
var viaGrammar = await store.Query<Person>().Where("Age > 1").ToList();

// ── JSON collections: raw JSON keyed by type, and schema-free by name ───
await store.Collection(typeof(Person)).Insert(JsonNode.Parse(
    """{"Id":"2","Name":"grace","Age":45,"Area":null,"Embedding":[],"Version":0,"AgeDoubled":0,"Photo":null}""")!.AsObject());
var viaJsonLane = await store.Collection(typeof(Person)).Get("2");

var events = store.Collection("audit_events");
var eventId = await events.Insert(JsonNode.Parse("""{"action":"login","weight":10}""")!.AsObject());
var recentEvents = await events.Query().Where("weight:number > 5").OrderBy("action").ToList();

// ── Spatial ─────────────────────────────────────────────────────────────
var square = new GeoPolygon(new[] { new GeoPoint(0, 0), new GeoPoint(0, 1), new GeoPoint(1, 1), new GeoPoint(0, 0) });
var within = await store.GeoWithinDistance<Person>(square, 100);
var covers = await store.GeoCovers<Person>(square);

// ── Microsoft.Extensions.VectorData connector ───────────────────────────
// The connector refuses a store with no ANN engine, and this sample runs plain SQLite without sqlite-vec.
// ILC walks the call graph statically, so the whole surface is still analyzed — which is the point here.
var mevdReached = false;
try
{
    var notes = new DocumentDbVectorStore(store).GetCollection<string, AiNote>(nameof(AiNote));
    await notes.UpsertAsync(new AiNote { Id = "n1", Tag = "a", Embedding = new[] { 1f, 0f, 0f, 0f } });
    await foreach (var hit in notes.SearchAsync(
        new[] { 1f, 0f, 0f, 0f }, top: 1, new VectorSearchOptions<AiNote> { Filter = n => n.Tag == "a" }))
    {
        mevdReached = hit.Record.Id == "n1";
    }
}
catch (NotSupportedException)
{
    // Expected: no sqlite-vec loaded here.
}

// ── Public host surface ─────────────────────────────────────────────────
// Shiny.DocumentDb.Hosting is what a package hosting the store over a transport depends on, and the reason it
// exists at all is the AOT guarantee: DocumentPredicate.Compile promises never to reach Reflection.Emit, which
// is a promise only a real ILC pass can keep honest. Every member is called here for exactly that reason.
Func<Person, bool> inScope = DocumentPredicate.Compile<Person>(p => p.Age > 1 && p.Name != "");
var parsed = DocumentPredicate.Compile(DocumentFilter.Parse("Age > 1 and Name != ''", AotSampleJsonContext.Default.Person));
var parsedJson = DocumentPredicate.Compile(DocumentFilter.ParseJson("weight:number > 5"));
var hostSurface = inScope(fetched!)
    && parsed(fetched!)
    && parsedJson(JsonNode.Parse("""{"weight":10}""")!.AsObject())
    && store.GetVersionMapping(typeof(Person)) != null
    && store.GetMappings() != null;

// ── Temporal ────────────────────────────────────────────────────────────
var history = await ((ITemporalDocumentStore)store).History<Person>("1");

Console.WriteLine($"""
    get={fetched?.Name}  where={filtered.Count}  count={counted}  groupBy={grouped.Count}
    grammar={viaGrammar.Count}  jsonLane={viaJsonLane != null}  spatial={within.Count}/{covers.Count}
    history={history.Count}  mevd={mevdReached}  hostSurface={hostSurface}
    """);

File.Delete(dbPath);

/// <summary>
/// A grouped projection, isolated so its one unavoidable warning can be scoped tightly.
/// <para>
/// Two AOT rules apply to <c>GroupBy(...).Select(...)</c>, and both are permanent:
/// </para>
/// <list type="number">
/// <item>The projection target must be a <b>named</b> type with a <c>JsonTypeInfo</c>. An anonymous type
/// cannot have one, so the anonymous-type overload throws at runtime under AOT.</item>
/// <item>Building the projection still emits IL2026 in <b>caller</b> code, because
/// <c>Expression.Bind</c> / <c>Expression.New</c> are <c>RequiresUnreferencedCode</c> — inherent to LINQ
/// expression trees, not something DocumentDb can annotate away (EF Core behaves identically). It is safe
/// here because <c>[JsonSerializable(typeof(NameCount))]</c> preserves the members the tree names.</item>
/// </list>
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026",
    Justification = "Expression.Bind is RequiresUnreferencedCode for every LINQ member-init projection. The "
                  + "members it binds are preserved by the NameCount entry in AotSampleJsonContext, so the "
                  + "tree cannot name anything the trimmer removed.")]
static Task<IReadOnlyList<NameCount>> GroupedProjection(IDocumentStore store)
    => store.Query<Person>()
        .GroupBy(x => x.Name)
        .Select(g => new NameCount { Name = g.Key, Count = g.Count() }, AotSampleJsonContext.Default.NameCount)
        .ToList();

public class Person
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public int AgeDoubled { get; set; }
    public int Version { get; set; }
    public Geometry? Area { get; set; }
    public ReadOnlyMemory<float> Embedding { get; set; }
    public DocumentBlob? Photo { get; set; }
}

/// <summary>Named projection target — a grouped Select needs a JsonTypeInfo, which anonymous types cannot have.</summary>
public class NameCount
{
    public string Name { get; set; } = "";
    public int Count { get; set; }
}

/// <summary>MEVD record for the VectorData connector — decorated with the MEVD attributes, not [Document].</summary>
public class AiNote
{
    [VectorStoreKey]
    public string Id { get; set; } = "";

    [VectorStoreData(IsIndexed = true)]
    public string Tag { get; set; } = "";

    [VectorStoreVector(4, DistanceFunction = DistanceFunction.CosineDistance)]
    public ReadOnlyMemory<float> Embedding { get; set; }
}

[JsonSerializable(typeof(Person))]
[JsonSerializable(typeof(AiNote))]
[JsonSerializable(typeof(NameCount))]
public partial class AotSampleJsonContext : JsonSerializerContext;
