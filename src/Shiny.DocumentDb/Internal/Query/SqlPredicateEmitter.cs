using System.Globalization;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Emits a relational SQL <c>WHERE</c> fragment (plus its bound parameters) from the <see cref="QueryNode"/>
/// IR, using an <see cref="IDatabaseProvider"/> for dialect spelling. Each node renders to a SQL string;
/// parameters are allocated in visit order (<c>@p0</c>, <c>@p1</c>, …) so output matches the original
/// direct-visitor translation exactly.
/// </summary>
sealed class SqlPredicateEmitter
{
    readonly Dictionary<string, object?> parameters = new();
    readonly IDatabaseProvider provider;
    readonly string paramPrefix;
    readonly bool inlineConstants;
    readonly string? tableName;
    int paramIndex;

    SqlPredicateEmitter(IDatabaseProvider provider, string paramPrefix = "@p", bool inlineConstants = false, string? tableName = null)
    {
        this.provider = provider;
        this.paramPrefix = paramPrefix;
        this.inlineConstants = inlineConstants;
        this.tableName = tableName;
    }

    public static (string Sql, Dictionary<string, object?> Parameters) Emit(PredicateNode root, IDatabaseProvider provider, string? tableName = null)
    {
        var emitter = new SqlPredicateEmitter(provider, tableName: tableName);
        var sql = emitter.Predicate(root);
        return (sql, emitter.parameters);
    }

    /// <summary>Emits a single scalar value (for projections) with a custom parameter prefix to avoid
    /// collisions with the WHERE clause's <c>@p</c> parameters.</summary>
    public static (string Sql, Dictionary<string, object?> Parameters) EmitValue(ValueNode root, IDatabaseProvider provider, string paramPrefix)
    {
        var emitter = new SqlPredicateEmitter(provider, paramPrefix);
        var sql = emitter.Value(root);
        return (sql, emitter.parameters);
    }

    /// <summary>Emits a scalar value with constants inlined as SQL literals (no parameters) — for contexts
    /// that cannot bind parameters, such as a generated/computed column definition.</summary>
    public static string EmitValueInline(ValueNode root, IDatabaseProvider provider)
        => new SqlPredicateEmitter(provider, inlineConstants: true).Value(root);

    string Predicate(PredicateNode node) => node switch
    {
        LogicalNode l => $"({this.Predicate(l.Left)}{(l.Op == LogicalOp.And ? " AND " : " OR ")}{this.Predicate(l.Right)})",
        NotNode n => $"NOT ({this.Predicate(n.Operand)})",
        CompareNode c => $"({this.Value(c.Left)}{CompareOpSql(c.Op)}{this.Value(c.Right)})",
        NullCheckRootNode nr => $"({this.provider.JsonNullCheck("Data", nr.JsonPath, nr.IsNull)})",
        NullCheckExprNode ne => $"({this.Value(ne.Target)}{(ne.IsNull ? " IS NULL" : " IS NOT NULL")})",
        LikeNode like => this.Like(like),
        InNode @in => this.In(@in),
        AnyNode any => this.Any(any),
        HasFlagNode hf => this.HasFlag(hf),
        BoolValueNode b => this.Value(b.Value),
        SpatialPredicateNode sp => this.Spatial(sp),
        _ => throw new NotSupportedException($"Predicate node '{node.GetType().Name}' is not supported.")
    };

    string Spatial(SpatialPredicateNode node)
    {
        if (this.tableName is null)
            throw new NotSupportedException("Spatial predicates require a table-scoped query.");

        // Matches of a distance-band query can lie outside the raw envelope, so expand the bbox prefilter.
        var env = node.Meters is { } d
            ? global::Shiny.DocumentDb.Internal.GeoBoundingBoxExtensions.ExpandByMeters(node.Query.GetEnvelope(), d)
            : node.Query.GetEnvelope();
        var geo = this.AddParameter(global::Shiny.DocumentDb.Internal.Spatial.SpatialJson.ToGeoJson(node.Query));
        var wkt = this.AddParameter(global::Shiny.DocumentDb.Internal.Spatial.WktWriter.ToWkt(node.Query));
        var minLat = this.AddParameter(env.MinLatitude);
        var maxLat = this.AddParameter(env.MaxLatitude);
        var minLng = this.AddParameter(env.MinLongitude);
        var maxLng = this.AddParameter(env.MaxLongitude);
        var meters = node.Meters is { } m ? this.AddParameter(m) : null;

        var sql = this.provider.BuildSpatialFilterSql(this.tableName, node.JsonPath, node.Op.ToString(), geo, wkt, minLat, maxLat, minLng, maxLng, meters);
        return sql ?? throw new NotSupportedException(
            $"DocumentFunctions spatial predicates in a LINQ Where are not supported on this provider — use store.Geo{node.Op}(...), or enable native spatial.");
    }

    string Value(ValueNode node) => node switch
    {
        RootFieldNode f => this.provider.JsonExtractTyped("Data", f.JsonPath, f.ClrType),
        ComputedColumnNode c => c.Column,
        ElementFieldNode f => this.provider.JsonExtractElementTyped(f.JsonPath, f.ClrType),
        ElementValueNode => this.provider.JsonEachPrimitiveValue,
        ConstantNode c => this.AddParameter(c.Value),
        ArrayLengthNode a => this.provider.JsonArrayLength("Data", a.JsonPath),
        CountSubqueryNode cs => $"(SELECT COUNT(*) FROM {this.provider.JsonEachFrom("Data", cs.CollectionJsonPath)} WHERE {this.Predicate(cs.Predicate)})",
        ScalarFnNode s => this.provider.TranslateScalar(s.Fn, [.. s.Args.Select(this.Value)], s.ResultType),
        BitAndNode b => this.provider.BitAnd(this.provider.CastInteger(this.Value(b.Left)), this.Value(b.Right)),
        ArithmeticNode a => $"({this.Value(a.Left)} {ArithOpSql(a.Op)} {this.Value(a.Right)})",
        CustomFnNode cf => $"{cf.SqlFunctionName}({string.Join(", ", cf.Args.Select(this.Value))})",
        SpatialDistanceNode sd => this.SpatialDistance(sd),
        _ => throw new NotSupportedException($"Value node '{node.GetType().Name}' is not supported.")
    };

    string SpatialDistance(SpatialDistanceNode node)
    {
        var geo = this.AddParameter(global::Shiny.DocumentDb.Internal.Spatial.SpatialJson.ToGeoJson(node.Query));
        var sql = this.provider.BuildSpatialDistanceSql(node.JsonPath, geo);
        return sql ?? throw new NotSupportedException(
            "DocumentFunctions.Distance is not supported on this provider — enable native spatial, or use store.NearestNeighbors(...).");
    }

    string Like(LikeNode node)
    {
        // Match the original visitor: allocate the argument parameter before emitting the target.
        var paramName = this.AddParameter(node.Argument);
        var target = this.Value(node.Target);
        var op = node.Kind switch
        {
            LikeKind.Contains => $" LIKE {this.provider.ConcatStrings("'%'", paramName, "'%'")}",
            LikeKind.StartsWith => $" LIKE {this.provider.ConcatStrings(paramName, "'%'")}",
            LikeKind.EndsWith => $" LIKE {this.provider.ConcatStrings("'%'", paramName)}",
            _ => throw new NotSupportedException($"LIKE kind '{node.Kind}' is not supported.")
        };
        return $"({target}{op})";
    }

    string In(InNode node)
    {
        // x IN () is invalid SQL; an empty set matches nothing.
        if (node.Values.Count == 0)
            return "(1 = 0)";

        var item = this.Value(node.Item);
        var values = string.Join(", ", node.Values.Select(this.AddParameter));
        return $"({item} IN ({values}))";
    }

    string Any(AnyNode node)
    {
        if (node.Predicate == null)
            return $"{this.provider.JsonArrayLength("Data", node.CollectionJsonPath)} > 0";

        return $"(SELECT COUNT(*) FROM {this.provider.JsonEachFrom("Data", node.CollectionJsonPath)} WHERE {this.Predicate(node.Predicate)}) > 0";
    }

    string HasFlag(HasFlagNode node)
    {
        // Allocate the mask once and reuse it on both sides: (BitAnd(field, mask) = mask).
        var mask = this.Value(node.Mask);
        var field = this.provider.CastInteger(this.Value(node.Field));
        return $"({this.provider.BitAnd(field, mask)} = {mask})";
    }

    string AddParameter(object? value)
    {
        if (this.inlineConstants)
            return SqlLiteral(NormalizeValue(value));

        var name = $"{this.paramPrefix}{this.paramIndex++}";
        this.parameters[name] = this.provider.NormalizeParameterValue(NormalizeValue(value));
        return name;
    }

    static string SqlLiteral(object? value) => value switch
    {
        null => "NULL",
        string s => $"'{s.Replace("'", "''")}'",
        bool b => b ? "1" : "0",
        Guid g => $"'{g}'",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => $"'{value.ToString()!.Replace("'", "''")}'"
    };

    static string ArithOpSql(ArithOp op) => op switch
    {
        ArithOp.Add => "+",
        ArithOp.Subtract => "-",
        ArithOp.Multiply => "*",
        ArithOp.Divide => "/",
        _ => throw new NotSupportedException($"Arithmetic operator '{op}' is not supported.")
    };

    static string CompareOpSql(CompareOp op) => op switch
    {
        CompareOp.Equal => " = ",
        CompareOp.NotEqual => " <> ",
        CompareOp.GreaterThan => " > ",
        CompareOp.GreaterThanOrEqual => " >= ",
        CompareOp.LessThan => " < ",
        CompareOp.LessThanOrEqual => " <= ",
        _ => throw new NotSupportedException($"Compare operator '{op}' is not supported.")
    };

    // ── Value normalization (moved from JsonExpressionVisitor) ──────────────

    static object? NormalizeValue(object? value) => value switch
    {
        DateTime dt => FormatDateTime(dt),
        DateTimeOffset dto => FormatDateTimeOffset(dto),
        // Box enums to their underlying numeric value so providers bind them the same way a
        // compiler-lifted `== EnumValue` constant does, matching the numeric JSON representation.
        Enum e => Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), CultureInfo.InvariantCulture),
        // Bind Guids as the string System.Text.Json wrote into the JSON blob (lowercase "D").
        Guid g => g.ToString(),
        _ => value
    };

    static string FormatDateTime(DateTime dt)
    {
        var fractional = dt.Ticks % TimeSpan.TicksPerSecond;
        var baseFmt = fractional != 0
            ? dt.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF")
            : dt.ToString("yyyy-MM-ddTHH:mm:ss");

        return dt.Kind switch
        {
            DateTimeKind.Utc => baseFmt + "Z",
            DateTimeKind.Local => baseFmt + dt.ToString("zzz"),
            _ => baseFmt
        };
    }

    static string FormatDateTimeOffset(DateTimeOffset dto)
    {
        var fractional = dto.Ticks % TimeSpan.TicksPerSecond;
        var baseFmt = fractional != 0
            ? dto.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF")
            : dto.ToString("yyyy-MM-ddTHH:mm:ss");

        return baseFmt + dto.ToString("zzz");
    }
}
