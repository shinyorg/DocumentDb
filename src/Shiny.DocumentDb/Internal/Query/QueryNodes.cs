namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Backend-agnostic intermediate representation for a <c>Where</c> predicate. The recognition layer
/// (<see cref="ExpressionLowerer"/>) lowers a LINQ expression tree — or, in future, a parsed filter
/// string — into this node set; each backend emits from it (relational SQL via
/// <see cref="SqlPredicateEmitter"/>, and later Cosmos SQL / Mongo BSON / in-memory interpretation).
/// </summary>
/// <remarks>
/// All JSON paths carried by <see cref="RootFieldNode"/> / <see cref="ElementFieldNode"/> are already
/// resolved (naming policy + nested type info applied), so emitters stay stateless walks that only
/// allocate parameters in visit order.
/// </remarks>
abstract record QueryNode;

// ── Values (produce a scalar SQL fragment) ──────────────────────────────────

abstract record ValueNode : QueryNode;

/// <summary>A document property: <c>json_extract(Data, '$.path')</c>, typed by <see cref="ClrType"/>.</summary>
sealed record RootFieldNode(string JsonPath, Type ClrType) : ValueNode;

/// <summary>An object-element property inside a <c>json_each</c> body: <c>json_extract(value, '$.path')</c>.</summary>
sealed record ElementFieldNode(string JsonPath, Type ClrType) : ValueNode;

/// <summary>The scalar value of a primitive-collection element inside a <c>json_each</c> body.</summary>
sealed record ElementValueNode : ValueNode;

/// <summary>A captured/literal value, bound as a parameter at emit time (value normalized then).</summary>
sealed record ConstantNode(object? Value) : ValueNode;

/// <summary>Collection length: <c>json_array_length(Data, '$.path')</c> — from <c>.Count</c>/<c>.Length</c>/<c>.Count()</c>.</summary>
sealed record ArrayLengthNode(string JsonPath) : ValueNode;

/// <summary>Filtered collection count: <c>(SELECT COUNT(*) FROM json_each(...) WHERE pred)</c> — from <c>.Count(pred)</c>.</summary>
sealed record CountSubqueryNode(string CollectionJsonPath, PredicateNode Predicate) : ValueNode;

/// <summary>A scalar function applied to argument values (string/math/date/phonetic), rendered via the provider dialect.</summary>
sealed record ScalarFnNode(ScalarFn Fn, IReadOnlyList<ValueNode> Args, Type ResultType) : ValueNode;

/// <summary>Bitwise AND of two integer-valued operands — backs flag-enum tests.</summary>
sealed record BitAndNode(ValueNode Left, ValueNode Right) : ValueNode;

/// <summary>A materialized computed property: a real generated/computed column referenced by name.</summary>
sealed record ComputedColumnNode(string Column, Type ClrType) : ValueNode;

enum ArithOp { Add, Subtract, Multiply, Divide }

/// <summary>Numeric arithmetic (<c>+ - * /</c>) over two value operands, rendered as a SQL infix expression.</summary>
sealed record ArithmeticNode(ArithOp Op, ValueNode Left, ValueNode Right, Type ResultType) : ValueNode;

/// <summary>A user-registered custom function, emitted as <c>SqlFunctionName(arg0, …)</c>.</summary>
sealed record CustomFnNode(string SqlFunctionName, IReadOnlyList<ValueNode> Args) : ValueNode;

// ── Predicates (produce a boolean SQL fragment) ─────────────────────────────

abstract record PredicateNode : QueryNode;

enum LogicalOp { And, Or }

sealed record LogicalNode(LogicalOp Op, PredicateNode Left, PredicateNode Right) : PredicateNode;

sealed record NotNode(PredicateNode Operand) : PredicateNode;

enum CompareOp { Equal, NotEqual, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual }

sealed record CompareNode(CompareOp Op, ValueNode Left, ValueNode Right) : PredicateNode;

/// <summary>Root-property null test, emitted via the provider's JSON null check.</summary>
sealed record NullCheckRootNode(string JsonPath, bool IsNull) : PredicateNode;

/// <summary>Null test on an arbitrary value expression: <c>(expr IS [NOT] NULL)</c>.</summary>
sealed record NullCheckExprNode(ValueNode Target, bool IsNull) : PredicateNode;

enum LikeKind { Contains, StartsWith, EndsWith }

sealed record LikeNode(ValueNode Target, object? Argument, LikeKind Kind) : PredicateNode;

/// <summary>Membership test lowered to native <c>IN (...)</c>. An empty value set emits <c>(1 = 0)</c>.</summary>
sealed record InNode(ValueNode Item, IReadOnlyList<object?> Values) : PredicateNode;

/// <summary>
/// <c>Enumerable.Any</c> over a collection. <see cref="Predicate"/> null ⇒ non-empty test
/// (<c>json_array_length(...) &gt; 0</c>); otherwise <c>(SELECT COUNT(*) FROM json_each(...) WHERE pred) &gt; 0</c>.
/// </summary>
sealed record AnyNode(string CollectionJsonPath, PredicateNode? Predicate) : PredicateNode;

/// <summary>A value used directly as a boolean predicate (a bare bool member / constant).</summary>
sealed record BoolValueNode(ValueNode Value) : PredicateNode;

/// <summary>
/// Flag-enum "has all bits" test (<c>Enum.HasFlag</c> / <c>(x &amp; f) == f</c>). Emitted as
/// <c>(BitAnd(field, mask) = mask)</c> on relational/Cosmos and <c>$bitsAllSet</c> on Mongo.
/// </summary>
sealed record HasFlagNode(ValueNode Field, ValueNode Mask) : PredicateNode;

enum SpatialOp { Intersects, Disjoint, Contains, Within, Covers, CoveredBy, Touches, Crosses, Overlaps, Equals, WithinDistance }

/// <summary>
/// A spatial predicate between a mapped geometry field (<see cref="JsonPath"/>) and a constant query
/// <see cref="Geometry"/>. Emitted by the provider's spatial dialect seam — a native <c>ST_*</c>/<c>SDO</c>
/// predicate or an R*Tree-pruned UDF refine (SQLite). Throws on providers without spatial support.
/// </summary>
sealed record SpatialPredicateNode(SpatialOp Op, string JsonPath, Geometry Query, double? Meters) : PredicateNode;
