using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// A document field addressed by JSON path rather than by CLR member. This is what a schema-free
/// (name-keyed) collection's field binder produces: there is no CLR type to hang an
/// <see cref="Expression.Property(Expression, string)"/> off, so the path travels through the expression
/// tree as an extension node and <see cref="ExpressionLowerer"/> lowers it straight to a
/// <see cref="RootFieldNode"/>.
/// </summary>
/// <remarks>
/// <see cref="ClrType"/> is the type the SQL extraction is cast to (<c>JsonExtractTyped</c>). It is inferred
/// while parsing — from the other operand of a comparison, from a function's required argument type, or from
/// an explicit <c>path:type</c> hint — and defaults to <see cref="string"/>, which every dialect emits as the
/// plain untyped extract.
/// </remarks>
sealed class DocumentFieldExpression(string jsonPath, Type clrType) : Expression
{
    public string JsonPath { get; } = jsonPath;
    public Type ClrType { get; } = clrType;

    public override ExpressionType NodeType => ExpressionType.Extension;
    public override Type Type => this.ClrType;
    public override bool CanReduce => false;

    // MANDATORY. Without it, ExpressionVisitor.VisitExtension falls through to Expression.VisitChildren,
    // which throws "must be reducible" for a node that cannot reduce. Nothing hits that on a single Where —
    // it only fires once a visitor walks the tree, i.e. on the SECOND Where (DocumentQuery.CombinePredicates
    // runs ParameterReplacer over every predicate after the first), so it is easy to miss.
    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    /// <summary>Returns this node retyped to <paramref name="t"/>, or itself when already that type.</summary>
    public DocumentFieldExpression Retype(Type t) => t == this.ClrType ? this : new(this.JsonPath, t);

    // Stable and type-inclusive: DocumentQueryBase's cursor pagination hashes the ordering's string form to
    // derive the cursor shape, so two orderings that extract the same path as different types must not
    // produce interchangeable cursors.
    public override string ToString() => $"${this.JsonPath}:{this.ClrType.Name}";
}
