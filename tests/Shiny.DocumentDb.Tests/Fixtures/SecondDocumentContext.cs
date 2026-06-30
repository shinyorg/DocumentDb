namespace Shiny.DocumentDb.Tests.Fixtures;

// A second generated context registered alongside SampleDocumentContext, used to prove that two contexts in
// one container each bind to their OWN store (keyed by context type) rather than shadowing each other on a
// single un-keyed IDocumentStore singleton.
[Document(typeof(Product), JsonContext = typeof(TestJsonContext))]
public partial class SecondDocumentContext : DocumentContext;
