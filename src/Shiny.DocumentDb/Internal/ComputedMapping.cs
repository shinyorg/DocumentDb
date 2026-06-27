using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Registration record produced by <c>MapComputedProperty&lt;T&gt;</c>. A computed property is a derived
/// value defined by <see cref="Definition"/> (e.g. <c>o =&gt; o.Quantity * o.UnitPrice</c>) that is
/// <b>not</b> stored in the document JSON. On the relational providers the definition is lowered to SQL and
/// inlined wherever the property is referenced in a query (filter / sort / projection); on read the value is
/// recomputed and written back onto the live object via <see cref="SetValue"/>.
/// </summary>
/// <remarks>
/// Public because <see cref="ExpressionLowerer"/> and the query layer read <see cref="Definition"/> to lower
/// the expression — there is no behavior here for consumers to override.
/// </remarks>
public sealed class ComputedMapping
{
    public required Type DocumentType { get; init; }

    /// <summary>The CLR property the value is exposed as — typically a <c>[JsonIgnore]</c> property.</summary>
    public required string PropertyName { get; init; }

    /// <summary>The naming-policy-applied key used to resolve the property from string/OData queries — set lazily.</summary>
    public string JsonName { get; set; } = null!;

    /// <summary>The CLR type of the computed value.</summary>
    public required Type ResultType { get; init; }

    /// <summary>The value definition, lowered to SQL for relational queries.</summary>
    public required LambdaExpression Definition { get; init; }

    /// <summary>Computes the value from a live document (compile-free) for read-back and in-memory providers.</summary>
    public required Func<object, object?> Compute { get; init; }

    /// <summary>Writes a computed value back onto a live document on read.</summary>
    public required Action<object, object?> SetValue { get; init; }

    /// <summary>When true, the value should be materialized as a native generated/computed column where the provider supports it.</summary>
    public bool Materialize { get; init; }

    /// <summary>When true, an index is created over the materialized column (implies <see cref="Materialize"/>).</summary>
    public bool Indexed { get; init; }

    /// <summary>The deterministic column name used when the value is materialized (e.g. <c>cc_Order_Total</c>).</summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// Set during store initialization to the materialized column name when the provider actually created it;
    /// null means alias mode (the definition is inlined into each query). Mutated per store, like the
    /// lazily-resolved JSON paths on the other mappings.
    /// </summary>
    public string? MaterializedColumn { get; set; }
}
