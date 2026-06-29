using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;

namespace Shiny.DocumentDb.Generators;

// Walks the reachable type closure of the DocumentSerialization.Generated [Document] types and produces the
// metadata entries the generated IJsonTypeInfoResolver must emit (objects, value primitives/enums/nullables,
// List<T>, T[]). Unsupported shapes raise DDB005.
static class MetadataCollector
{
    const string JsonPropertyName = "System.Text.Json.Serialization.JsonPropertyNameAttribute";
    const string JsonIgnore = "System.Text.Json.Serialization.JsonIgnoreAttribute";

    public static ImmutableArray<MetaType> Collect(
        IReadOnlyList<INamedTypeSymbol> roots, Location location, List<DiagInfo> diagnostics)
    {
        var byFullName = new Dictionary<string, MetaType>();
        var usedSafe = new HashSet<string>();
        var objectQueue = new Queue<INamedTypeSymbol>();
        var enqueued = new HashSet<string>();

        foreach (var r in roots)
            EnqueueObject(r);

        void EnqueueObject(INamedTypeSymbol t)
        {
            var key = t.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (enqueued.Add(key))
                objectQueue.Enqueue(t);
        }

        string Safe(string fullName)
        {
            var sb = new StringBuilder();
            foreach (var c in fullName)
                sb.Append(char.IsLetterOrDigit(c) ? c : '_');
            var baseName = sb.ToString().Trim('_');
            if (baseName.Length == 0)
                baseName = "T";
            var name = baseName;
            var i = 1;
            while (!usedSafe.Add(name))
                name = baseName + (i++);
            return name;
        }

        // Ensure a (non-object) referenced type is registered; objects get enqueued. Returns nothing —
        // the property keeps its own declared type as TProp regardless.
        void Register(ITypeSymbol t, Location loc)
        {
            var full = t.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (byFullName.ContainsKey(full) || enqueued.Contains(full))
                return;

            // Nullable<T> value type
            if (t is INamedTypeSymbol nn && nn.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            {
                var underlying = nn.TypeArguments[0];
                var uFull = underlying.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var conv = $"global::System.Text.Json.Serialization.Metadata.JsonMetadataServices.GetNullableConverter<{uFull}>(o)";
                if (PrimitiveConverter(underlying) == null && underlying.TypeKind != TypeKind.Enum)
                {
                    diagnostics.Add(new DiagInfo(Diagnostics.UnsupportedGeneratedType, loc, full, "nullable of an unsupported value type"));
                    return;
                }
                byFullName[full] = new MetaType(MetaKind.Value, full, Safe(full), conv, null, null, Empty);
                return;
            }

            // array
            if (t is IArrayTypeSymbol arr)
            {
                if (arr.Rank != 1)
                {
                    diagnostics.Add(new DiagInfo(Diagnostics.UnsupportedGeneratedType, loc, full, "multi-dimensional arrays are not supported"));
                    return;
                }
                var elem = arr.ElementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                byFullName[full] = new MetaType(MetaKind.Array, full, Safe(full), null, elem, null, Empty);
                Register(arr.ElementType, loc);
                return;
            }

            // List<T>
            if (t is INamedTypeSymbol ln && ln.IsGenericType &&
                ln.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.List<T>")
            {
                var elemSym = ln.TypeArguments[0];
                var elem = elemSym.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                byFullName[full] = new MetaType(MetaKind.List, full, Safe(full), null, elem, $"new {full}()", Empty);
                Register(elemSym, loc);
                return;
            }

            // primitive value
            var prim = PrimitiveConverter(t);
            if (prim != null)
            {
                byFullName[full] = new MetaType(MetaKind.Value, full, Safe(full), prim, null, null, Empty);
                return;
            }

            // enum
            if (t.TypeKind == TypeKind.Enum)
            {
                var conv = $"global::System.Text.Json.Serialization.Metadata.JsonMetadataServices.GetEnumConverter<{full}>(o)";
                byFullName[full] = new MetaType(MetaKind.Value, full, Safe(full), conv, null, null, Empty);
                return;
            }

            // plain object
            if (t is INamedTypeSymbol obj && IsPlainObject(obj))
            {
                EnqueueObject(obj);
                return;
            }

            diagnostics.Add(new DiagInfo(Diagnostics.UnsupportedGeneratedType, loc, full, "unsupported property type"));
        }

        while (objectQueue.Count > 0)
        {
            var obj = objectQueue.Dequeue();
            var full = obj.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (byFullName.ContainsKey(full))
                continue;

            if (!IsPlainObject(obj))
            {
                diagnostics.Add(new DiagInfo(Diagnostics.UnsupportedGeneratedType, location, full,
                    "no accessible parameterless constructor (records / parameterized constructors are not supported)"));
                continue;
            }

            var props = new List<MetaProp>();
            foreach (var member in EnumerateProperties(obj))
            {
                var (clr, json, propType) = member;
                props.Add(new MetaProp(clr, json, propType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), full));
                Register(propType, location);
            }

            byFullName[full] = new MetaType(MetaKind.Object, full, Safe(full), null, null, $"new {full}()",
                new EquatableArray<MetaProp>(props.ToImmutableArray()));
        }

        return byFullName.Values.ToImmutableArray();
    }

    static readonly EquatableArray<MetaProp> Empty = new(ImmutableArray<MetaProp>.Empty);

    static IEnumerable<(string Clr, string? Json, ITypeSymbol Type)> EnumerateProperties(INamedTypeSymbol type)
    {
        // include inherited public settable properties, derived-most first; dedupe by name
        var seen = new HashSet<string>();
        for (var t = type; t != null && t.SpecialType != SpecialType.System_Object; t = t.BaseType)
        {
            foreach (var m in t.GetMembers())
            {
                if (m is not IPropertySymbol p || p.IsStatic || p.IsIndexer)
                    continue;
                if (p.DeclaredAccessibility != Accessibility.Public)
                    continue;
                if (p.GetMethod is not { DeclaredAccessibility: Accessibility.Public })
                    continue;
                if (p.SetMethod is not { DeclaredAccessibility: Accessibility.Public } setter || setter.IsInitOnly)
                    continue;
                if (!seen.Add(p.Name))
                    continue;

                string? jsonName = null;
                var ignored = false;
                foreach (var a in p.GetAttributes())
                {
                    var an = a.AttributeClass?.ToDisplayString();
                    if (an == JsonIgnore)
                        ignored = true;
                    else if (an == JsonPropertyName && a.ConstructorArguments.Length == 1)
                        jsonName = a.ConstructorArguments[0].Value as string;
                }
                if (ignored)
                    continue;

                yield return (p.Name, jsonName, p.Type);
            }
        }
    }

    static bool IsPlainObject(INamedTypeSymbol t)
    {
        if (t.TypeKind != TypeKind.Class && t.TypeKind != TypeKind.Struct)
            return false;
        if (t.IsAbstract)
            return false;
        foreach (var c in t.InstanceConstructors)
        {
            if (c.Parameters.Length == 0 && c.DeclaredAccessibility == Accessibility.Public)
                return true;
        }
        return false;
    }

    static string? PrimitiveConverter(ITypeSymbol t)
    {
        const string M = "global::System.Text.Json.Serialization.Metadata.JsonMetadataServices.";
        switch (t.SpecialType)
        {
            case SpecialType.System_String: return M + "StringConverter";
            case SpecialType.System_Boolean: return M + "BooleanConverter";
            case SpecialType.System_Byte: return M + "ByteConverter";
            case SpecialType.System_SByte: return M + "SByteConverter";
            case SpecialType.System_Int16: return M + "Int16Converter";
            case SpecialType.System_UInt16: return M + "UInt16Converter";
            case SpecialType.System_Int32: return M + "Int32Converter";
            case SpecialType.System_UInt32: return M + "UInt32Converter";
            case SpecialType.System_Int64: return M + "Int64Converter";
            case SpecialType.System_UInt64: return M + "UInt64Converter";
            case SpecialType.System_Single: return M + "SingleConverter";
            case SpecialType.System_Double: return M + "DoubleConverter";
            case SpecialType.System_Decimal: return M + "DecimalConverter";
            case SpecialType.System_Char: return M + "CharConverter";
            case SpecialType.System_DateTime: return M + "DateTimeConverter";
        }
        switch (t.ToDisplayString())
        {
            case "System.Guid": return M + "GuidConverter";
            case "System.DateTimeOffset": return M + "DateTimeOffsetConverter";
            case "System.TimeSpan": return M + "TimeSpanConverter";
            case "System.Uri": return M + "UriConverter";
            case "System.Version": return M + "VersionConverter";
            default: return null;
        }
    }
}

static class MetadataEmitter
{
    const string Svc = "global::System.Text.Json.Serialization.Metadata.JsonMetadataServices";
    const string Meta = "global::System.Text.Json.Serialization.Metadata";

    public static void Emit(StringBuilder sb, string indent, ContextModel m)
    {
        var body = indent + "    ";
        var inner = body + "    ";

        sb.Append(indent).Append("file sealed class ").Append(m.ResolverName)
          .AppendLine(" : global::System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver");
        sb.Append(indent).AppendLine("{");
        sb.Append(body).Append("public static readonly ").Append(m.ResolverName).Append(" Default = new ")
          .Append(m.ResolverName).AppendLine("();");
        sb.AppendLine();

        // GetTypeInfo dispatch
        sb.Append(body)
          .AppendLine($"public {Meta}.JsonTypeInfo? GetTypeInfo(global::System.Type type, global::System.Text.Json.JsonSerializerOptions options)");
        sb.Append(body).AppendLine("{");
        foreach (var t in m.GeneratedMetadata)
            sb.Append(inner).Append("if (type == typeof(").Append(t.FullName).Append(")) return Create_")
              .Append(t.SafeName).AppendLine("(options);");
        sb.Append(inner).AppendLine("return null;");
        sb.Append(body).AppendLine("}");

        // Prop helper
        sb.AppendLine();
        EmitPropHelper(sb, body);

        // per-type factories
        foreach (var t in m.GeneratedMetadata)
        {
            sb.AppendLine();
            EmitType(sb, body, t);
        }

        sb.Append(indent).AppendLine("}");
    }

    static void EmitPropHelper(StringBuilder sb, string body)
    {
        var inner = body + "    ";
        sb.Append(body).AppendLine("[global::System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage(\"Trimming\", \"IL2070\", Justification = \"User-rooted document type; mirrors STJ source-gen AttributeProvider.\")]");
        sb.Append(body).AppendLine($"static {Meta}.JsonPropertyInfo Prop<TProp>(global::System.Text.Json.JsonSerializerOptions o, global::System.Type declaring, string clr, string? jsonName, global::System.Func<object, TProp?> get, global::System.Action<object, TProp?> set)");
        sb.Append(body).AppendLine("{");
        sb.Append(inner).AppendLine($"var p = {Svc}.CreatePropertyInfo<TProp>(o, new {Meta}.JsonPropertyInfoValues<TProp>");
        sb.Append(inner).AppendLine("{");
        sb.Append(inner).AppendLine("    IsProperty = true, IsPublic = true, IsVirtual = false, DeclaringType = declaring,");
        sb.Append(inner).AppendLine("    Converter = null, Getter = get, Setter = set, IgnoreCondition = null,");
        sb.Append(inner).AppendLine("    HasJsonInclude = false, IsExtensionData = false, NumberHandling = null,");
        sb.Append(inner).AppendLine("    PropertyName = clr, JsonPropertyName = jsonName");
        sb.Append(inner).AppendLine("});");
        sb.Append(inner).AppendLine("p.AttributeProvider = declaring.GetProperty(clr);");
        sb.Append(inner).AppendLine("return p;");
        sb.Append(body).AppendLine("}");
    }

    static void EmitType(StringBuilder sb, string body, MetaType t)
    {
        var inner = body + "    ";
        sb.Append(body).Append($"static {Meta}.JsonTypeInfo<").Append(t.FullName).Append("> Create_")
          .Append(t.SafeName).AppendLine("(global::System.Text.Json.JsonSerializerOptions o)");
        sb.Append(body).AppendLine("{");

        switch (t.Kind)
        {
            case MetaKind.Value:
                sb.Append(inner).Append("return ").Append(Svc).Append(".CreateValueInfo<").Append(t.FullName)
                  .Append(">(o, ").Append(t.ConverterExpr).AppendLine(");");
                break;

            case MetaKind.Array:
                sb.Append(inner).Append("return ").Append(Svc).Append(".CreateArrayInfo<").Append(t.ElementFullName)
                  .Append(">(o, new ").Append(Meta).Append(".JsonCollectionInfoValues<").Append(t.ElementFullName)
                  .AppendLine("[]> { });");
                break;

            case MetaKind.List:
                sb.Append(inner).Append("return ").Append(Svc).Append(".CreateListInfo<").Append(t.FullName).Append(", ")
                  .Append(t.ElementFullName).Append(">(o, new ").Append(Meta).Append(".JsonCollectionInfoValues<")
                  .Append(t.FullName).Append("> { ObjectCreator = static () => ").Append(t.ObjectCreator).AppendLine(" });");
                break;

            case MetaKind.Object:
                sb.Append(inner).Append("return ").Append(Svc).Append(".CreateObjectInfo<").Append(t.FullName)
                  .Append(">(o, new ").Append(Meta).Append(".JsonObjectInfoValues<").Append(t.FullName).AppendLine(">");
                sb.Append(inner).AppendLine("{");
                sb.Append(inner).Append("    ObjectCreator = static () => ").Append(t.ObjectCreator).AppendLine(",");
                sb.Append(inner).Append("    PropertyMetadataInitializer = _ => new ").Append(Meta).AppendLine(".JsonPropertyInfo[]");
                sb.Append(inner).AppendLine("    {");
                foreach (var p in t.Properties)
                {
                    var json = p.JsonName == null ? "null" : "\"" + Escape(p.JsonName) + "\"";
                    sb.Append(inner).Append("        Prop<").Append(p.PropTypeFullName).Append(">(o, typeof(")
                      .Append(p.OwnerFullName).Append("), \"").Append(Escape(p.ClrName)).Append("\", ").Append(json)
                      .Append(", static x => ((").Append(p.OwnerFullName).Append(")x).").Append(p.ClrName)
                      .Append(", static (x, v) => ((").Append(p.OwnerFullName).Append(")x).").Append(p.ClrName)
                      .AppendLine(" = v!),");
                }
                sb.Append(inner).AppendLine("    }");
                sb.Append(inner).AppendLine("});");
                break;
        }

        sb.Append(body).AppendLine("}");
    }

    static string Escape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
