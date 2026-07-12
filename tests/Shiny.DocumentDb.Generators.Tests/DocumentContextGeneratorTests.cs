using Xunit;

namespace Shiny.DocumentDb.Generators.Tests;

// These tests assert the generator's emitted text and diagnostics. Full end-to-end compilation (with the
// System.Text.Json generator also in the pipeline) is proven by the integration suite in
// Shiny.DocumentDb.Tests; here the hand-written JsonSerializerContext stays a stub, so we do not compile the
// output — only inspect generator output and generator diagnostics.
public class DocumentContextGeneratorTests
{
    // A single file-scoped namespace covers everything appended after it.
    const string Models = """
        using System.Text.Json.Serialization;
        using Shiny.DocumentDb;

        namespace Sample;

        public class User { public string Id { get; set; } = ""; public int Age { get; set; } }
        public class Product { public string Id { get; set; } = ""; }
        public class Category { public string Id { get; set; } = ""; }

        [JsonSerializable(typeof(User))]
        [JsonSerializable(typeof(Product))]
        public partial class SampleJsonContext : JsonSerializerContext;
        """;

    [Fact]
    public void Emits_sets_configure_model_and_di_extension()
    {
        var src = Models + """

            [Document(typeof(User), Id = "Id", JsonContext = typeof(SampleJsonContext))]
            [Document(typeof(Product), Table = "products", Set = "Catalog")]
            public partial class AppContext : DocumentContext { }
            """;

        var (generated, diagnostics, _) = GeneratorHarness.Run(src);

        Assert.Empty(diagnostics);

        // pluralized + overridden set names, each a DocumentSet<T>
        Assert.Contains("DocumentSet<global::Sample.User> Users", generated);
        Assert.Contains("DocumentSet<global::Sample.Product> Catalog", generated);

        // ConfigureModel lowering
        Assert.Contains("ConfigureModel", generated);
        Assert.Contains("MapTypeToTable<global::Sample.Product>(\"products\")", generated);
        Assert.Contains("MapIdProperty<global::Sample.User>(\"Id\")", generated);

        // JsonContext mode chains the resolver
        Assert.Contains("TypeInfoResolverChain.Add(global::Sample.SampleJsonContext.Default)", generated);

        // DI extension — scoped registration + the MAUI/Blazor factory registration
        Assert.Contains("AddAppContext", generated);
        Assert.Contains("AddAppContextFactory", generated);
        Assert.Contains("AddDocumentContext<global::Sample.AppContext>", generated);
        Assert.Contains("AddDocumentContextFactory<global::Sample.AppContext>", generated);
    }

    [Fact]
    public void Emits_constructor_only_when_absent()
    {
        var withoutCtor = Models + """

            [Document(typeof(User))]
            public partial class AppContext : DocumentContext { }
            """;
        var (genA, _, _) = GeneratorHarness.Run(withoutCtor);
        Assert.Contains("public AppContext(global::Shiny.DocumentDb.IDocumentSession session) : base(session)", genA);

        var withCtor = Models + """

            [Document(typeof(User))]
            public partial class AppContext : DocumentContext
            {
                public AppContext(IDocumentSession session) : base(session) { }
            }
            """;
        var (genB, _, _) = GeneratorHarness.Run(withCtor);
        Assert.DoesNotContain(": base(session)", genB);
    }

    [Fact]
    public void DDB001_when_not_partial()
    {
        var src = Models + """

            [Document(typeof(User))]
            public class AppContext : DocumentContext { }
            """;
        var (_, diagnostics, _) = GeneratorHarness.Run(src);
        Assert.Contains(diagnostics, d => d.Id == "DDB001");
    }

    [Fact]
    public void DDB002_when_not_document_context()
    {
        var src = Models + """

            [Document(typeof(User))]
            public partial class NotAContext { }
            """;
        var (_, diagnostics, _) = GeneratorHarness.Run(src);
        Assert.Contains(diagnostics, d => d.Id == "DDB002");
    }

    [Fact]
    public void DDB003_on_duplicate_set_name()
    {
        var src = Models + """

            [Document(typeof(Product), Set = "Items")]
            [Document(typeof(Category), Set = "Items")]
            public partial class AppContext : DocumentContext { }
            """;
        var (_, diagnostics, _) = GeneratorHarness.Run(src);
        Assert.Contains(diagnostics, d => d.Id == "DDB003");
    }

    [Fact]
    public void Generated_mode_emits_metadata_resolver_and_compiles()
    {
        // No JsonSerializerContext — the generator owns serialization. Covers enum, nullable value, array,
        // List<T>, and a nested object so the closure walk is fully exercised.
        const string src = """
            using Shiny.DocumentDb;
            namespace Sample;

            public enum Pri { Low, High }
            public class Addr { public string City { get; set; } = ""; }
            public class Doc
            {
                public string Id { get; set; } = "";
                public int Count { get; set; }
                public int? Opt { get; set; }
                public Pri Level { get; set; }
                public System.Collections.Generic.List<string> Tags { get; set; } = new();
                public int[] Scores { get; set; } = System.Array.Empty<int>();
                public Addr? Home { get; set; }
            }

            [Document(typeof(Doc), Serialization = DocumentSerialization.Generated)]
            public partial class GenContext : DocumentContext { }
            """;

        var (generated, diagnostics, output) = GeneratorHarness.Run(src);

        Assert.Empty(diagnostics);
        Assert.Empty(GeneratorHarness.OutputErrors(output));   // generated metadata resolver actually compiles

        Assert.Contains("GenContextGeneratedResolver", generated);
        Assert.Contains("CreateObjectInfo<global::Sample.Doc>", generated);
        Assert.Contains("CreateObjectInfo<global::Sample.Addr>", generated);   // nested object walked
        Assert.Contains("CreateListInfo", generated);
        Assert.Contains("CreateArrayInfo", generated);
        Assert.Contains("GetEnumConverter<global::Sample.Pri>", generated);
        Assert.Contains("GetNullableConverter<int>", generated);
        Assert.Contains("TypeInfoResolverChain.Add(GenContextGeneratedResolver.Default)", generated);
    }

    [Fact]
    public void DDB005_on_unsupported_generated_type()
    {
        // positional record → no parameterless constructor → unsupported by Generated mode
        const string src = """
            using Shiny.DocumentDb;
            namespace Sample;

            public record Rec(string Id);

            [Document(typeof(Rec), Serialization = DocumentSerialization.Generated)]
            public partial class GenContext : DocumentContext { }
            """;
        var (_, diagnostics, _) = GeneratorHarness.Run(src);
        Assert.Contains(diagnostics, d => d.Id == "DDB005");
    }

    [Fact]
    public void DDB005_on_dictionary_property()
    {
        // Regression: a Dictionary (or any non-List/array collection) used to be walked as a plain object and
        // silently serialize as an empty {}. It must now raise DDB005.
        const string src = """
            using Shiny.DocumentDb;
            namespace Sample;

            public class Doc
            {
                public string Id { get; set; } = "";
                public System.Collections.Generic.Dictionary<string, int> Counts { get; set; } = new();
            }

            [Document(typeof(Doc), Serialization = DocumentSerialization.Generated)]
            public partial class GenContext : DocumentContext { }
            """;
        var (_, diagnostics, _) = GeneratorHarness.Run(src);
        Assert.Contains(diagnostics, d => d.Id == "DDB005");
    }

    [Fact]
    public void DDB005_on_init_only_property()
    {
        // Regression: an init-only property used to be silently dropped (never persisted). It must now raise
        // DDB005 so the data loss is surfaced at build time.
        const string src = """
            using Shiny.DocumentDb;
            namespace Sample;

            public class Doc
            {
                public string Id { get; set; } = "";
                public string Name { get; init; } = "";
            }

            [Document(typeof(Doc), Serialization = DocumentSerialization.Generated)]
            public partial class GenContext : DocumentContext { }
            """;
        var (_, diagnostics, _) = GeneratorHarness.Run(src);
        Assert.Contains(diagnostics, d => d.Id == "DDB005");
    }
}
