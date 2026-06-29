using Microsoft.CodeAnalysis;

namespace Shiny.DocumentDb.Generators;

static class Diagnostics
{
    const string Category = "Shiny.DocumentDb";

    public static readonly DiagnosticDescriptor NotPartial = new(
        "DDB001",
        "DocumentContext must be partial",
        "'{0}' is decorated with [Document] but is not declared 'partial'; the source generator cannot extend it",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor NotDocumentContext = new(
        "DDB002",
        "Type must derive from DocumentContext",
        "'{0}' is decorated with [Document] but does not derive from Shiny.DocumentDb.DocumentContext",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor DuplicateSetName = new(
        "DDB003",
        "Duplicate document set name",
        "Two [Document] declarations on '{0}' resolve to the same set name '{1}'; set 'Set =' on one of them to disambiguate",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor UnsupportedGeneratedType = new(
        "DDB005",
        "Type not supported by Generated serialization",
        "'{0}' cannot use DocumentSerialization.Generated ({1}); set JsonContext = typeof(YourJsonContext) on the [Document] instead",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

}
