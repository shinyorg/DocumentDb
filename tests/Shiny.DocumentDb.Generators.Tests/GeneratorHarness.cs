using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Shiny.DocumentDb.Generators.Tests;

static class GeneratorHarness
{
    static readonly IReadOnlyList<MetadataReference> References = BuildReferences();

    static IReadOnlyList<MetadataReference> BuildReferences()
    {
        var tpa = (string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!;
        var refs = tpa
            .Split(Path.PathSeparator)
            .Where(p => p.Length > 0 && File.Exists(p))
            .Select(p => (MetadataReference)MetadataReference.CreateFromFile(p))
            .ToList();
        return refs;
    }

    public static (string GeneratedSource, ImmutableArray<Diagnostic> Diagnostics, Compilation Output) Run(string source)
    {
        var tree = CSharpSyntaxTree.ParseText(source);
        var compilation = CSharpCompilation.Create(
            "GeneratorTestAssembly",
            new[] { tree },
            References,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var driver = CSharpGeneratorDriver.Create(new DocumentContextGenerator());
        driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);

        var generated = string.Join(
            "\n\n",
            output.SyntaxTrees
                .Where(t => t != tree)
                .Select(t => t.ToString()));

        return (generated, diagnostics, output);
    }

    public static ImmutableArray<Diagnostic> OutputErrors(Compilation output)
        => output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToImmutableArray();
}
