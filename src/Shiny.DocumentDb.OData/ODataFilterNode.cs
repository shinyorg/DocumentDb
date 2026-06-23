namespace Shiny.DocumentDb.OData;

/// <summary>
/// Provider-neutral node kinds for a parsed OData <c>$filter</c> tree. Both the engine's own
/// lightweight string parser and the ASP.NET host's Microsoft-parser adapter produce this same tree,
/// so there is exactly one <see cref="FilterExpressionBuilder"/> consuming it.
/// </summary>
public enum ODataNodeKind
{
    /// <summary>A binary comparison or logical/arithmetic operator (see <see cref="ODataBinaryOperator"/>).</summary>
    Binary,

    /// <summary>A unary operator (<c>not</c> / negation).</summary>
    Unary,

    /// <summary>A property path reference such as <c>Name</c> or <c>Address/City</c>.</summary>
    Property,

    /// <summary>A literal constant (string, number, bool, null).</summary>
    Constant,

    /// <summary>A function call such as <c>contains</c>, <c>startswith</c>, <c>tolower</c>.</summary>
    Function
}

/// <summary>Binary operators across comparison, logical, and arithmetic categories.</summary>
public enum ODataBinaryOperator
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo
}

/// <summary>Unary operators.</summary>
public enum ODataUnaryOperator
{
    Not,
    Negate
}

/// <summary>
/// A node in the provider-neutral <c>$filter</c> tree. Immutable; constructed via the static factory
/// helpers so callers don't have to remember which slots each kind populates.
/// </summary>
public sealed class ODataFilterNode
{
    ODataFilterNode(ODataNodeKind kind) => this.Kind = kind;

    public ODataNodeKind Kind { get; private init; }

    // Binary
    public ODataBinaryOperator BinaryOperator { get; private init; }
    public ODataFilterNode? Left { get; private init; }
    public ODataFilterNode? Right { get; private init; }

    // Unary
    public ODataUnaryOperator UnaryOperator { get; private init; }
    public ODataFilterNode? Operand { get; private init; }

    // Property
    public string? PropertyPath { get; private init; }

    // Constant
    public object? Value { get; private init; }

    // Function
    public string? FunctionName { get; private init; }
    public IReadOnlyList<ODataFilterNode> Arguments { get; private init; } = Array.Empty<ODataFilterNode>();

    public static ODataFilterNode Binary(ODataBinaryOperator op, ODataFilterNode left, ODataFilterNode right)
        => new(ODataNodeKind.Binary) { BinaryOperator = op, Left = left, Right = right };

    public static ODataFilterNode Unary(ODataUnaryOperator op, ODataFilterNode operand)
        => new(ODataNodeKind.Unary) { UnaryOperator = op, Operand = operand };

    public static ODataFilterNode Property(string path)
        => new(ODataNodeKind.Property) { PropertyPath = path };

    public static ODataFilterNode Constant(object? value)
        => new(ODataNodeKind.Constant) { Value = value };

    public static ODataFilterNode Function(string name, params ODataFilterNode[] args)
        => new(ODataNodeKind.Function) { FunctionName = name, Arguments = args };
}
