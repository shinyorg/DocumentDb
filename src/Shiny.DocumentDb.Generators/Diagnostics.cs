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

    public static readonly DiagnosticDescriptor GeneratedModeUnsupported = new(
        "DDB004",
        "DocumentSerialization.Generated is not yet available",
        "DocumentSerialization.Generated is not yet emitted by the generator for '{0}'; declare a JsonContext for AOT or use Reflection. Falling back to Auto",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true);
}
