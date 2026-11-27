using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Text.Json;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// C# union types (<c>union Pet(Cat, Dog);</c>, .NET 11+) in queries. System.Text.Json stores a union as just its case
/// value — <c>"pet": { "name": "Rex", "goodBoy": true }</c>, <c>"ref": 42</c> — with no wrapper and no tag, so:
/// <list type="bullet">
/// <item>the union's <c>.Value</c> member is the union property's own JSON path (<c>x.Pet.Value</c> → <c>pet</c>,
/// <c>((Dog)x.Pet.Value).GoodBoy</c> → <c>pet.goodBoy</c>), and a cast to a case type gives the value its CLR type;</item>
/// <item>a case test (<c>x.Pet.Value is Dog</c>) is answered from the stored JSON: by its JSON kind when the case is the
/// only one of that kind (<c>is int</c> → a number), and for one of several object cases by a property only that case
/// has — the same evidence System.Text.Json's structural classifier reads.</item>
/// </list>
/// Union types are recognized by the compiler-emitted <c>System.Runtime.CompilerServices.IUnion</c> interface, by name,
/// so this compiles on every target framework and simply never matches before .NET 11.
/// </summary>
static class UnionSupport
{
    const string UnionInterface = "System.Runtime.CompilerServices.IUnion";
    static readonly ConcurrentDictionary<Type, bool> isUnion = new();
    static readonly ConcurrentDictionary<(Type Union, Type Case, JsonSerializerOptions Options), UnionCaseTest> caseTests = new();

    /// <summary>True when <paramref name="type"/> (or its nullable underlying type) is a C# union type.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Interfaces of a type referenced from an expression tree are preserved.")]
    public static bool IsUnion(Type type)
        => isUnion.GetOrAdd(Nullable.GetUnderlyingType(type) ?? type, static t => t.GetInterfaces().Any(i => i.FullName == UnionInterface));

    /// <summary>True when <paramref name="member"/> is a union's <c>.Value</c> — a hop that adds nothing to the JSON path.</summary>
    public static bool IsUnionValue(MemberExpression member)
        => member.Member.Name == "Value" && member.Expression != null && IsUnion(member.Expression.Type);

    /// <summary>Strips casts and union <c>.Value</c> hops off the top of <paramref name="expression"/>.</summary>
    public static Expression StripCastsAndUnionValues(Expression expression)
    {
        var current = expression;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert
               || current is MemberExpression member && IsUnionValue(member))
        {
            current = current is UnaryExpression u ? u.Operand : ((MemberExpression)current).Expression!;
        }
        return current;
    }

    /// <summary>
    /// Walks a member chain from the outermost member to its root, passing through casts and skipping union
    /// <c>.Value</c> hops. Returns the members outermost-first and the root (the parameter, or whatever ends the chain).
    /// </summary>
    public static (List<MemberExpression> Members, Expression? Root) WalkMembers(Expression expression)
    {
        var members = new List<MemberExpression>();
        var current = StripCastsAndUnionValues(expression);
        while (current is MemberExpression member)
        {
            members.Add(member);
            current = StripCastsAndUnionValues(member.Expression!);
        }
        return (members, current);
    }

    /// <summary>
    /// The case a union-member expression is cast to — <c>(int)x.Ref.Value</c> → <c>int</c> — or null when
    /// <paramref name="expression"/> is not a cast of a union's <c>.Value</c>.
    /// </summary>
    public static Type? CastCaseType(Expression expression)
    {
        // The innermost cast is the one applied to the value; outer ones are boxing the IN builder adds ((object)(int)v).
        Type? caseType = null;
        var current = expression;
        while (current is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
        {
            if (convert.Type != typeof(object))
                caseType = convert.Type;
            current = convert.Operand;
        }
        return caseType != null && current is MemberExpression member && IsUnionValue(member) ? caseType : null;
    }

    /// <summary>
    /// How to test that a stored union value is a <paramref name="caseType"/>: by JSON kind, or — for one of several
    /// object cases — by the presence of a property only that case has.
    /// </summary>
    /// <exception cref="NotSupportedException">The case cannot be told apart from another case in the stored JSON.</exception>
    public static UnionCaseTest CaseTest(Type unionType, Type caseType, JsonSerializerOptions options)
        => caseTests.GetOrAdd((Nullable.GetUnderlyingType(unionType) ?? unionType, caseType, options), static key => BuildCaseTest(key.Union, key.Case, key.Options));

    static UnionCaseTest BuildCaseTest(Type unionType, Type caseType, JsonSerializerOptions options)
    {
        var cases = CaseTypes(unionType);
        var matching = cases.Where(caseType.IsAssignableFrom).ToList();
        if (matching.Count == 0)
            throw new NotSupportedException($"'{caseType.Name}' is not a case of the union '{unionType.Name}'.");

        var kind = KindOf(matching[0], options);
        if (matching.Any(c => KindOf(c, options) != kind))
            throw Ambiguous(unionType, caseType, "its matching cases are stored as different JSON kinds");

        var sameKind = cases.Where(c => !caseType.IsAssignableFrom(c) && KindOf(c, options) == kind).ToList();
        if (sameKind.Count == 0)
            return new UnionCaseTest(kind, null);

        if (kind != JsonValueKind.Object || matching.Count != 1)
            throw Ambiguous(unionType, caseType, $"'{string.Join("', '", sameKind.Select(c => c.Name))}' {(sameKind.Count == 1 ? "is" : "are")} stored as the same JSON kind");

        // A property this case always writes and no other object case has. Value types first: they are never
        // written as null, so "present and not null" is exact for them.
        var others = sameKind.SelectMany(c => JsonProperties(c, options)).Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
        var unique = JsonProperties(caseType, options)
            .Where(p => !others.Contains(p.Name))
            .OrderBy(p => p.PropertyType.IsValueType && Nullable.GetUnderlyingType(p.PropertyType) == null ? 0 : 1)
            .FirstOrDefault()
            ?? throw Ambiguous(unionType, caseType, "it has no JSON property the other object cases lack");

        return new UnionCaseTest(kind, unique.Name);
    }

    /// <summary>
    /// When <paramref name="caseType"/> is a scalar case of a union whose other cases are stored as other JSON kinds,
    /// that kind — a cast to the case must only read values of it (<c>(int)x.Ref.Value</c> must not read the string
    /// "42", nor fail converting "ABC"). Null when no guard is needed.
    /// </summary>
    public static JsonValueKind? ScalarGuardKind(Type unionType, Type caseType, JsonSerializerOptions options)
    {
        var kind = KindOf(Nullable.GetUnderlyingType(caseType) ?? caseType, options);
        if (kind is not (JsonValueKind.String or JsonValueKind.Number or JsonValueKind.True))
            return null;
        return CaseTypes(Nullable.GetUnderlyingType(unionType) ?? unionType).Any(c => KindOf(c, options) != kind) ? kind : null;
    }

    // ── String grammar: the union value, cases by name and by literal kind ──

    /// <summary><c>union.Value</c> — the <c>object</c> a union holds.</summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "A union's Value property (and Nullable<T>.Value) is referenced by every use of the type.")]
    public static Expression ValueOf(Expression union)
    {
        var type = Nullable.GetUnderlyingType(union.Type) ?? union.Type;
        var target = union.Type == type ? union : Expression.Property(union, union.Type.GetProperty("Value")!);
        return Expression.Property(target, type.GetProperty("Value")!);
    }

    /// <summary>The case a filter string names in <c>pet is Dog</c> / <c>ref is int</c>: by type name or C# keyword.</summary>
    public static Type? CaseByName(Type unionType, string name)
        => CaseTypes(Nullable.GetUnderlyingType(unionType) ?? unionType)
            .FirstOrDefault(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase) || KeywordOf(c) == name.ToLowerInvariant());

    /// <summary>
    /// The case a literal compares against in <c>ref == 42</c>: the only case of the literal's JSON kind, or for a
    /// number the case matching the literal's own type (int before long before double…). Null when none fits.
    /// </summary>
    public static Type? CaseForLiteral(Type unionType, JsonValueKind kind, Type naturalType, JsonSerializerOptions options)
    {
        var candidates = CaseTypes(Nullable.GetUnderlyingType(unionType) ?? unionType).Where(c => KindOf(c, options) == kind).ToList();
        return candidates.FirstOrDefault(c => c == naturalType)
               ?? candidates.FirstOrDefault(c => c == typeof(string))
               ?? (candidates.Count == 1 || kind == JsonValueKind.Number ? candidates.FirstOrDefault() : null);
    }

    /// <summary>The case of <paramref name="unionType"/> whose JSON object has a property named <paramref name="name"/>.</summary>
    public static Type? CaseWithProperty(Type unionType, string name, JsonSerializerOptions options)
        => CaseTypes(Nullable.GetUnderlyingType(unionType) ?? unionType)
            .Where(c => KindOf(c, options) == JsonValueKind.Object)
            .FirstOrDefault(c => JsonProperties(c, options).Any(p =>
                p.Name.Equals(name, StringComparison.OrdinalIgnoreCase)
                || p.AttributeProvider is System.Reflection.MemberInfo m && m.Name.Equals(name, StringComparison.OrdinalIgnoreCase)));

    static string? KeywordOf(Type type) => Type.GetTypeCode(type) switch
    {
        TypeCode.Int32 => "int", TypeCode.Int64 => "long", TypeCode.Int16 => "short", TypeCode.Byte => "byte",
        TypeCode.Double => "double", TypeCode.Single => "float", TypeCode.Decimal => "decimal",
        TypeCode.Boolean => "bool", TypeCode.String => "string", TypeCode.Char => "char",
        _ => null
    };

    static NotSupportedException Ambiguous(Type unionType, Type caseType, string why)
        => new($"A query cannot test whether a '{unionType.Name}' holds a '{caseType.Name}': {why}, so the stored JSON does not say which case it is. Compare the value instead (for example a cast: (int)x.Ref.Value == 42).");

    // The compiler gives a union one single-argument constructor per case.
    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "A union's case constructors are referenced by every use of the union.")]
    static IReadOnlyList<Type> CaseTypes(Type unionType)
        => unionType.GetConstructors()
            .Select(c => c.GetParameters())
            .Where(p => p.Length == 1)
            .Select(p => Nullable.GetUnderlyingType(p[0].ParameterType) ?? p[0].ParameterType)
            .Distinct()
            .ToList();

    static IEnumerable<System.Text.Json.Serialization.Metadata.JsonPropertyInfo> JsonProperties(Type type, JsonSerializerOptions options)
    {
        var typeInfo = options.TryGetTypeInfo(type, out var info) ? info : null;
        return typeInfo?.Kind == System.Text.Json.Serialization.Metadata.JsonTypeInfoKind.Object
            ? typeInfo.Properties.Where(p => p.Get != null)
            : [];
    }

    /// <summary>The JSON kind a value of <paramref name="type"/> is stored as. <see cref="JsonValueKind.True"/> stands for a boolean.</summary>
    static JsonValueKind KindOf(Type type, JsonSerializerOptions options)
    {
        if (type == typeof(bool))
            return JsonValueKind.True;
        if (type == typeof(string) || type == typeof(char) || type == typeof(Guid) || type == typeof(DateTime) || type == typeof(DateTimeOffset)
            || type == typeof(DateOnly) || type == typeof(TimeOnly) || type == typeof(TimeSpan) || type == typeof(Uri))
            return JsonValueKind.String;
        if (type.IsEnum)
            return EnumJsonStorage.SerializesAsString(type, options) ? JsonValueKind.String : JsonValueKind.Number;
        if (type.IsPrimitive || type == typeof(decimal))
            return JsonValueKind.Number;
        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type))
            return JsonValueKind.Array;
        return JsonValueKind.Object;
    }
}

/// <summary>
/// How a stored union value is recognized as a given case: its JSON <paramref name="Kind"/>
/// (<see cref="JsonValueKind.True"/> standing for any boolean), plus — for one of several object cases — the JSON name
/// of a property only that case has.
/// </summary>
readonly record struct UnionCaseTest(JsonValueKind Kind, string? DistinctProperty);
