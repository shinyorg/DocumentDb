using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Shiny.DocumentDb.Generators;

[Generator(Microsoft.CodeAnalysis.LanguageNames.CSharp)]
public sealed class DocumentContextGenerator : IIncrementalGenerator
{
    const string DocumentAttribute = "Shiny.DocumentDb.DocumentAttribute";
    const string DocumentContextType = "Shiny.DocumentDb.DocumentContext";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var contexts = context.SyntaxProvider.ForAttributeWithMetadataName(
            DocumentAttribute,
            predicate: static (node, _) => node is ClassDeclarationSyntax,
            transform: static (ctx, _) => Build(ctx)
        );

        context.RegisterSourceOutput(contexts, static (spc, model) =>
        {
            foreach (var d in model.Diagnostics)
                spc.ReportDiagnostic(d.ToDiagnostic());

            if (!model.CanEmit)
                return;

            spc.AddSource(model.HintName, SourceText.From(Emit(model), Encoding.UTF8));
        });
    }

    // ── model construction ──────────────────────────────────────────────────

    static ContextModel Build(GeneratorAttributeSyntaxContext ctx)
    {
        var symbol = (INamedTypeSymbol)ctx.TargetSymbol;
        var node = (ClassDeclarationSyntax)ctx.TargetNode;
        var diagnostics = new List<DiagInfo>();

        var isPartial = node.Modifiers.Any(m => m.IsKind(Microsoft.CodeAnalysis.CSharp.SyntaxKind.PartialKeyword));
        if (!isPartial)
            diagnostics.Add(new DiagInfo(Diagnostics.NotPartial, node.Identifier.GetLocation(), symbol.Name));

        var derives = DerivesFromDocumentContext(symbol);
        if (!derives)
            diagnostics.Add(new DiagInfo(Diagnostics.NotDocumentContext, node.Identifier.GetLocation(), symbol.Name));

        var docs = new List<DocumentDecl>();
        var generatedRoots = new List<INamedTypeSymbol>();
        var usedSetNames = new Dictionary<string, bool>(System.StringComparer.Ordinal);
        foreach (var attr in ctx.Attributes)
        {
            if (attr.ConstructorArguments.Length == 0 || attr.ConstructorArguments[0].Value is not INamedTypeSymbol docType)
                continue;

            string? table = null, id = null, setName = null, jsonContext = null;
            var serialization = 0;
            foreach (var na in attr.NamedArguments)
            {
                switch (na.Key)
                {
                    case "Table": table = na.Value.Value as string; break;
                    case "Id": id = na.Value.Value as string; break;
                    case "Set": setName = na.Value.Value as string; break;
                    case "Serialization": serialization = na.Value.Value is int i ? i : 0; break;
                    case "JsonContext":
                        if (na.Value.Value is INamedTypeSymbol ctxType)
                            jsonContext = ctxType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                        break;
                }
            }

            if (serialization == 3) // DocumentSerialization.Generated → we emit metadata-mode JsonTypeInfo
                generatedRoots.Add(docType);

            var resolvedSet = string.IsNullOrWhiteSpace(setName) ? Pluralize(docType.Name) : setName!;
            if (usedSetNames.ContainsKey(resolvedSet))
                diagnostics.Add(new DiagInfo(Diagnostics.DuplicateSetName, node.Identifier.GetLocation(), symbol.Name, resolvedSet));
            else
                usedSetNames.Add(resolvedSet, true);

            docs.Add(new DocumentDecl(
                docType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                resolvedSet,
                table,
                id,
                jsonContext
            ));
        }

        var compilation = ctx.SemanticModel.Compilation;
        var emitDi =
            compilation.GetTypeByMetadataName("Microsoft.Extensions.DependencyInjection.IServiceCollection") != null &&
            compilation.GetTypeByMetadataName("Shiny.DocumentDb.DocumentContextServiceCollectionExtensions") != null;

        var ns = symbol.ContainingNamespace.IsGlobalNamespace
            ? null
            : symbol.ContainingNamespace.ToDisplayString();

        var generatedMeta = generatedRoots.Count > 0
            ? MetadataCollector.Collect(generatedRoots, compilation, node.Identifier.GetLocation(), diagnostics)
            : ImmutableArray<MetaType>.Empty;

        return new ContextModel(
            ns,
            symbol.Name,
            symbol.DeclaredAccessibility == Accessibility.Public ? "public" : "internal",
            HasStoreConstructor(symbol),
            isPartial && derives,
            emitDi,
            new EquatableArray<DocumentDecl>(docs.ToImmutableArray()),
            new EquatableArray<MetaType>(generatedMeta),
            new EquatableArray<DiagInfo>(diagnostics.ToImmutableArray())
        );
    }

    static bool DerivesFromDocumentContext(INamedTypeSymbol symbol)
    {
        for (var t = symbol.BaseType; t != null; t = t.BaseType)
        {
            if (t.ToDisplayString() == DocumentContextType)
                return true;
        }
        return false;
    }

    static Location AttrLocation(AttributeData attr, ClassDeclarationSyntax node)
        => attr.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? node.Identifier.GetLocation();

    static bool HasStoreConstructor(INamedTypeSymbol symbol)
    {
        foreach (var c in symbol.InstanceConstructors)
        {
            if (c.Parameters.Length == 1 && c.Parameters[0].Type.ToDisplayString() == "Shiny.DocumentDb.IDocumentSession")
                return true;
        }
        return false;
    }

    // ── emission ────────────────────────────────────────────────────────────

    static string Emit(ContextModel m)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();

        var indent = "";
        if (m.Namespace != null)
        {
            sb.Append("namespace ").Append(m.Namespace).AppendLine();
            sb.AppendLine("{");
            indent = "    ";
        }

        // ── the context partial ──
        sb.Append(indent).Append("partial class ").Append(m.Name).AppendLine();
        sb.Append(indent).AppendLine("{");
        var body = indent + "    ";

        if (!m.HasStoreConstructor)
        {
            sb.Append(body).Append("public ").Append(m.Name)
              .AppendLine("(global::Shiny.DocumentDb.IDocumentSession session) : base(session) { }");
            sb.AppendLine();
        }

        var idx = 0;
        foreach (var d in m.Documents)
        {
            sb.Append(body).Append("private global::Shiny.DocumentDb.DocumentSet<").Append(d.TypeFullName)
              .Append(">? __set_").Append(idx).AppendLine(";");
            sb.Append(body).Append("public global::Shiny.DocumentDb.DocumentSet<").Append(d.TypeFullName).Append("> ")
              .Append(d.SetName).Append(" => this.__set_").Append(idx)
              .Append(" ??= this.Set<").Append(d.TypeFullName).AppendLine(">();");
            sb.AppendLine();
            idx++;
        }

        EmitConfigureModel(sb, body, m);

        sb.Append(indent).AppendLine("}");

        // ── DI extension ──
        if (m.EmitDi)
        {
            sb.AppendLine();
            EmitRegistration(sb, indent, m);
        }

        // ── generated metadata resolver (DocumentSerialization.Generated) ──
        if (m.HasGenerated)
        {
            sb.AppendLine();
            MetadataEmitter.Emit(sb, indent, m);
        }

        if (m.Namespace != null)
            sb.AppendLine("}");

        return sb.ToString();
    }

    static void EmitConfigureModel(StringBuilder sb, string body, ContextModel m)
    {
        var inner = body + "    ";
        sb.Append(body)
          .AppendLine("/// <summary>Lowers the [Document] declarations into the store options. Called by the generated DI extension before user configuration.</summary>");
        sb.Append(body)
          .AppendLine("internal static void ConfigureModel(global::Shiny.DocumentDb.DocumentStoreOptions options)");
        sb.Append(body).AppendLine("{");

        var contexts = new List<string>();
        foreach (var d in m.Documents)
        {
            if (d.JsonContextFullName != null && !contexts.Contains(d.JsonContextFullName))
                contexts.Add(d.JsonContextFullName);
        }

        if (contexts.Count > 0 || m.HasGenerated)
        {
            sb.Append(inner)
              .AppendLine("options.JsonSerializerOptions ??= new global::System.Text.Json.JsonSerializerOptions { PropertyNamingPolicy = global::System.Text.Json.JsonNamingPolicy.CamelCase };");
            foreach (var c in contexts)
            {
                sb.Append(inner).Append("options.JsonSerializerOptions.TypeInfoResolverChain.Add(")
                  .Append(c).AppendLine(".Default);");
            }
            if (m.HasGenerated)
                sb.Append(inner).Append("options.JsonSerializerOptions.TypeInfoResolverChain.Add(")
                  .Append(m.ResolverName).AppendLine(".Default);");
            sb.AppendLine();
        }

        foreach (var d in m.Documents)
        {
            if (d.Table == null && d.IdProperty == null)
                continue;

            sb.Append(inner).Append("options.ConfigureDocument<").Append(d.TypeFullName).AppendLine(">(cfg =>");
            sb.Append(inner).AppendLine("{");
            if (d.Table != null)
                sb.Append(inner).Append("    cfg.Table = \"").Append(Escape(d.Table)).AppendLine("\";");
            if (d.IdProperty != null)
                sb.Append(inner).Append("    cfg.MapIdProperty(\"").Append(Escape(d.IdProperty)).AppendLine("\");");
            sb.Append(inner).AppendLine("});");
        }

        // The context's own model hook, so a context-based app declares its mappings next to its [Document]
        // list rather than inside AddDocumentStore. Static partial: unimplemented, the call compiles away.
        sb.AppendLine();
        sb.Append(inner).AppendLine("OnConfiguring(new global::Shiny.DocumentDb.DocumentModelBuilder(options));");

        sb.Append(body).AppendLine("}");
        sb.AppendLine();
        sb.Append(body)
          .AppendLine("/// <summary>Implement this on your context partial to configure the document model in one place.</summary>");
        sb.Append(body)
          .AppendLine("static partial void OnConfiguring(global::Shiny.DocumentDb.DocumentModelBuilder model);");
    }

    static void EmitRegistration(StringBuilder sb, string indent, ContextModel m)
    {
        var ctxName = m.Namespace == null ? "global::" + m.Name : "global::" + m.Namespace + "." + m.Name;
        var body = indent + "    ";

        sb.Append(indent).Append(m.Accessibility).Append(" static class ").Append(m.Name).AppendLine("Registration");
        sb.Append(indent).AppendLine("{");

        // Add<Context> — per-context store + scoped context. The request-scoped registration for ASP.NET Core.
        EmitAddMethod(sb, body, m, ctxName,
            "Add" + m.Name,
            "AddDocumentContext",
            "Registers a per-context document store (from this context's [Document] declarations) and the context itself, scoped — the request-scoped registration for ASP.NET Core.");

        sb.AppendLine();

        // Add<Context>Factory — singleton IDocumentContextFactory<TContext>. The no-ambient-scope registration
        // for MAUI / Blazor / desktop, mirroring EF Core's AddDbContextFactory.
        EmitAddMethod(sb, body, m, ctxName,
            "Add" + m.Name + "Factory",
            "AddDocumentContextFactory",
            "Registers an IDocumentContextFactory&lt;" + m.Name + "&gt; (singleton) that creates short-lived contexts on demand — the registration for apps with no ambient DI scope (MAUI, Blazor, desktop).");

        sb.Append(indent).AppendLine("}");
    }

    static void EmitAddMethod(StringBuilder sb, string body, ContextModel m, string ctxName, string methodName, string helper, string summary)
    {
        var inner = body + "    ";
        var deep = inner + "    ";

        sb.Append(body).Append("/// <summary>").Append(summary).AppendLine("</summary>");
        sb.Append(body).Append(m.Accessibility)
          .Append(" static global::Microsoft.Extensions.DependencyInjection.IServiceCollection ").Append(methodName).AppendLine("(");
        sb.Append(inner).AppendLine("this global::Microsoft.Extensions.DependencyInjection.IServiceCollection services,");
        sb.Append(inner).AppendLine("global::System.Action<global::Shiny.DocumentDb.DocumentStoreOptions> configure)");
        sb.Append(body).AppendLine("{");
        sb.Append(inner).AppendLine("global::System.ArgumentNullException.ThrowIfNull(configure);");
        sb.Append(inner).Append("return global::Shiny.DocumentDb.DocumentContextServiceCollectionExtensions.")
          .Append(helper).Append('<').Append(ctxName).AppendLine(">(");
        sb.Append(deep).AppendLine("services,");
        sb.Append(deep).AppendLine("options =>");
        sb.Append(deep).AppendLine("{");
        sb.Append(deep).Append("    ").Append(m.Name).AppendLine(".ConfigureModel(options);");
        sb.Append(deep).AppendLine("    configure(options);");
        sb.Append(deep).AppendLine("},");
        sb.Append(deep).Append("static session => new ").Append(ctxName).AppendLine("(session));");
        sb.Append(body).AppendLine("}");
    }

    static string Escape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    // Dumb pluralization; [Document(Set = "...")] overrides for irregulars.
    static string Pluralize(string name)
    {
        if (name.Length == 0)
            return name;

        if (name.EndsWith("y") && name.Length > 1 && !IsVowel(name[name.Length - 2]))
            return name.Substring(0, name.Length - 1) + "ies";

        if (name.EndsWith("s") || name.EndsWith("x") || name.EndsWith("z") ||
            name.EndsWith("ch") || name.EndsWith("sh"))
            return name + "es";

        return name + "s";
    }

    static bool IsVowel(char c) => "aeiouAEIOU".IndexOf(c) >= 0;
}
