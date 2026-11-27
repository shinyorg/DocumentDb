using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>Swaps one lambda parameter for another — re-targets a base-type predicate at a derived type.</summary>
sealed class ParameterRetargeter(ParameterExpression from, ParameterExpression to) : ExpressionVisitor
{
    protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : base.VisitParameter(node);
}
