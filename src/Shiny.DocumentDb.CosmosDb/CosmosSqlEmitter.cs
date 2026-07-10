using System.Globalization;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.CosmosDb;

/// <summary>
/// Emits a Cosmos NoSQL <c>WHERE</c> fragment (plus bound parameters) from the shared <see cref="QueryNode"/>
/// IR — the Cosmos counterpart to the relational <c>SqlPredicateEmitter</c>. Shares recognition
/// (<see cref="ExpressionLowerer"/>) with every other backend; only the emission differs (dotted
/// <c>c.data.*</c> paths, <c>CONTAINS</c>/<c>STARTSWITH</c>, <c>IS_DEFINED</c>, <c>EXISTS … IN</c>).
/// </summary>
sealed class CosmosSqlEmitter
{
    readonly Dictionary<string, object?> parameters = new();
    readonly int paramStart;
    int aliasIndex;
    string elementAlias = "";

    CosmosSqlEmitter(int paramStart) => this.paramStart = paramStart;

    /// <param name="paramStart">First parameter ordinal — lets the caller keep names globally unique across
    /// multiple predicates without post-hoc remapping.</param>
    public static (string Sql, Dictionary<string, object?> Parameters) Emit(PredicateNode root, int paramStart = 0)
    {
        var e = new CosmosSqlEmitter(paramStart);
        return (e.Predicate(root), e.parameters);
    }

    string Predicate(PredicateNode node) => node switch
    {
        LogicalNode l => $"({this.Predicate(l.Left)}{(l.Op == LogicalOp.And ? " AND " : " OR ")}{this.Predicate(l.Right)})",
        NotNode n => $"NOT ({this.Predicate(n.Operand)})",
        CompareNode c => $"({this.Value(c.Left)} {CompareOpSql(c.Op)} {this.Value(c.Right)})",
        NullCheckRootNode nr => this.NullCheck(this.RootPath(nr.JsonPath), nr.IsNull),
        NullCheckExprNode ne => this.NullCheck(this.Value(ne.Target), ne.IsNull),
        LikeNode like => this.Like(like),
        InNode @in => this.In(@in),
        AnyNode any => this.Any(any),
        HasFlagNode hf => this.HasFlag(hf),
        BoolValueNode b => $"({this.Value(b.Value)} = true)",
        SpatialPredicateNode sp => this.Spatial(sp),
        FullTextMatchNode => throw new NotSupportedException(
            "DocumentFunctions.LuceneMatch is not supported inside a CosmosDB query — use store.FullTextSearch(...) for full-text search on CosmosDB."),
        _ => throw new NotSupportedException($"Predicate node '{node.GetType().Name}' is not supported in CosmosDB queries.")
    };

    string Spatial(SpatialPredicateNode node)
    {
        var path = this.RootPath(node.JsonPath);
        var gj = global::Shiny.DocumentDb.Internal.Spatial.SpatialJson.ToGeoJson(node.Query);  // GeoJSON object literal
        return node.Op switch
        {
            SpatialOp.Intersects => $"ST_INTERSECTS({path}, {gj})",
            SpatialOp.Within => $"ST_WITHIN({path}, {gj})",
            SpatialOp.Disjoint => $"(NOT ST_INTERSECTS({path}, {gj}))",
            SpatialOp.WithinDistance => $"ST_DISTANCE({path}, {gj}) <= {(node.Meters ?? 0).ToString(System.Globalization.CultureInfo.InvariantCulture)}",
            _ => throw new NotSupportedException($"DocumentFunctions.{node.Op} in a Where is not translatable on CosmosDB — use store.Geo{node.Op}(...).")
        };
    }

    string Value(ValueNode node) => node switch
    {
        RootFieldNode f => this.RootPath(f.JsonPath),
        ElementFieldNode f => $"{this.elementAlias}.{f.JsonPath}",
        ElementValueNode => this.elementAlias,
        ConstantNode c => this.AddParameter(c.Value),
        ArrayLengthNode a => $"ARRAY_LENGTH({this.RootPath(a.JsonPath)})",
        CountSubqueryNode cs => this.CountSubquery(cs),
        ScalarFnNode s => this.Scalar(s),
        BitAndNode b => $"({this.Value(b.Left)} & {this.Value(b.Right)})",
        CustomFnNode cf => $"{cf.SqlFunctionName}({string.Join(", ", cf.Args.Select(this.Value))})",
        FullTextScoreNode => throw new NotSupportedException(
            "DocumentFunctions.LuceneScore is not supported inside a CosmosDB query — use store.FullTextSearch(...) for ranked full-text on CosmosDB."),
        _ => throw new NotSupportedException($"Value node '{node.GetType().Name}' is not supported in CosmosDB queries.")
    };

    string RootPath(string jsonPath) => $"c.data.{jsonPath}";

    string NullCheck(string path, bool isNull)
        => isNull ? $"(NOT IS_DEFINED({path}) OR IS_NULL({path}))" : $"(IS_DEFINED({path}) AND NOT IS_NULL({path}))";

    string Like(LikeNode node)
    {
        var p = this.AddParameter(node.Argument);
        var target = this.Value(node.Target);
        return node.Kind switch
        {
            LikeKind.Contains => $"CONTAINS({target}, {p})",
            LikeKind.StartsWith => $"STARTSWITH({target}, {p})",
            LikeKind.EndsWith => $"ENDSWITH({target}, {p})",
            _ => throw new NotSupportedException($"LIKE kind '{node.Kind}' is not supported.")
        };
    }

    string In(InNode node)
    {
        if (node.Values.Count == 0)
            return "(1 = 0)";
        var item = this.Value(node.Item);
        var values = string.Join(", ", node.Values.Select(this.AddParameter));
        return $"{item} IN ({values})";
    }

    string Any(AnyNode node)
    {
        if (node.Predicate == null)
            return $"ARRAY_LENGTH({this.RootPath(node.CollectionJsonPath)}) > 0";

        var saved = this.elementAlias;
        this.elementAlias = $"elem{this.aliasIndex++}";
        var inner = this.Predicate(node.Predicate);
        var sql = $"EXISTS(SELECT VALUE 1 FROM {this.elementAlias} IN {this.RootPath(node.CollectionJsonPath)} WHERE {inner})";
        this.elementAlias = saved;
        return sql;
    }

    string CountSubquery(CountSubqueryNode node)
    {
        var saved = this.elementAlias;
        this.elementAlias = $"elem{this.aliasIndex++}";
        var inner = this.Predicate(node.Predicate);
        var sql = $"(SELECT VALUE COUNT(1) FROM {this.elementAlias} IN {this.RootPath(node.CollectionJsonPath)} WHERE {inner})";
        this.elementAlias = saved;
        return sql;
    }

    string HasFlag(HasFlagNode node)
    {
        var mask = this.Value(node.Mask);
        var field = this.Value(node.Field);
        return $"(({field} & {mask}) = {mask})";
    }

    string Scalar(ScalarFnNode node)
    {
        var a = node.Args.Select(this.Value).ToList();
        return node.Fn switch
        {
            ScalarFn.Lower => $"LOWER({a[0]})",
            ScalarFn.Upper => $"UPPER({a[0]})",
            ScalarFn.Length => $"LENGTH({a[0]})",
            ScalarFn.Trim => $"TRIM({a[0]})",
            ScalarFn.TrimStart => $"LTRIM({a[0]})",
            ScalarFn.TrimEnd => $"RTRIM({a[0]})",
            // Cosmos SUBSTRING is 0-based (matches .NET) and requires a length.
            ScalarFn.Substring => a.Count == 3 ? $"SUBSTRING({a[0]}, {a[1]}, {a[2]})" : $"SUBSTRING({a[0]}, {a[1]}, LENGTH({a[0]}))",
            ScalarFn.Replace => $"REPLACE({a[0]}, {a[1]}, {a[2]})",
            ScalarFn.IndexOf => $"INDEX_OF({a[0]}, {a[1]})",
            ScalarFn.Concat => $"CONCAT({string.Join(", ", a)})",
            ScalarFn.Abs => $"ABS({a[0]})",
            ScalarFn.Ceiling => $"CEILING({a[0]})",
            ScalarFn.Floor => $"FLOOR({a[0]})",
            ScalarFn.Round => $"ROUND({a[0]})",
            ScalarFn.Sqrt => $"SQRT({a[0]})",
            ScalarFn.Pow => $"POWER({a[0]}, {a[1]})",
            ScalarFn.Sign => $"SIGN({a[0]})",
            ScalarFn.Year => $"DateTimePart(\"yyyy\", {a[0]})",
            ScalarFn.Month => $"DateTimePart(\"mm\", {a[0]})",
            ScalarFn.Day => $"DateTimePart(\"dd\", {a[0]})",
            ScalarFn.Hour => $"DateTimePart(\"hh\", {a[0]})",
            ScalarFn.Minute => $"DateTimePart(\"mi\", {a[0]})",
            ScalarFn.Second => $"DateTimePart(\"ss\", {a[0]})",
            _ => throw new NotSupportedException($"Scalar function '{node.Fn}' is not supported in CosmosDB queries.")
        };
    }

    string AddParameter(object? value)
    {
        var name = $"@p{this.paramStart + this.parameters.Count}";
        this.parameters[name] = NormalizeValue(value);
        return name;
    }

    static object? NormalizeValue(object? value) => value switch
    {
        Enum e => Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()), CultureInfo.InvariantCulture),
        _ => value
    };

    static string CompareOpSql(CompareOp op) => op switch
    {
        CompareOp.Equal => "=",
        CompareOp.NotEqual => "!=",
        CompareOp.GreaterThan => ">",
        CompareOp.GreaterThanOrEqual => ">=",
        CompareOp.LessThan => "<",
        CompareOp.LessThanOrEqual => "<=",
        _ => throw new NotSupportedException($"Compare operator '{op}' is not supported.")
    };
}
