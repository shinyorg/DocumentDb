using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A registered global query filter. <see cref="Name"/> is <c>null</c> for unnamed filters.
/// <see cref="Predicate"/> is an <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c> stored as a
/// <see cref="LambdaExpression"/> so the registry can hold filters for many types.
/// </summary>
public sealed record QueryFilter(string? Name, LambdaExpression Predicate);
