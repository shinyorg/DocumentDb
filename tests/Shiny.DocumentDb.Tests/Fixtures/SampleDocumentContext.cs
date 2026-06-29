namespace Shiny.DocumentDb.Tests.Fixtures;

// Exercises the source generator end-to-end: a partial DocumentContext whose DocumentSet<T> properties,
// ConfigureModel, and DI extension are emitted by Shiny.DocumentDb.Generators. JsonContext mode binds each
// type to the hand-written TestJsonContext so the suite runs AOT-clean (UseReflectionFallback = false).
[Document(typeof(User), JsonContext = typeof(TestJsonContext))]
[Document(typeof(Product), Set = "Catalog", JsonContext = typeof(TestJsonContext))]   // Set override + pluralization bypass
[Document(typeof(Order), Table = "orders", JsonContext = typeof(TestJsonContext))]    // Table override
public partial class SampleDocumentContext : DocumentContext;
