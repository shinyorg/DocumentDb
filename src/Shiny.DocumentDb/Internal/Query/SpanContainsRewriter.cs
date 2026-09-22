using System.Collections;
using System.Linq.Expressions;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Undoes C# 14's first-class span binding for query predicates. <c>values.Contains(x.Age)</c> over an array now binds
/// to <c>MemoryExtensions.Contains(ReadOnlySpan&lt;T&gt;, T)</c> through an implicit array→span conversion, a shape no
/// translator knows (and the in-memory interpreter cannot invoke — a span can't be boxed). When the collection is a
/// constant or captured value, the call is rewritten to the canonical <see cref="InExpressionBuilder"/> <c>IN</c> form
/// every provider already translates, so arrays behave exactly like lists again.
/// </summary>
/// <remarks>
/// Applied where a predicate enters the query layer (the <c>Where</c> builders and query filters) and at the
/// translator/interpreter entry points, so store-level filter parameters are covered too. A span over a document member
/// (<c>x.Tags.Contains("a")</c> on an array property) is left alone — collection membership over the document itself was
/// never a supported <c>Contains</c> form, and its error still surfaces from the translator.
/// </remarks>
static class SpanContainsRewriter
{
    public static Expression<TDelegate> Rewrite<TDelegate>(Expression<TDelegate> predicate)
        => Rewriter.Instance.VisitAndConvert(predicate, nameof(Rewrite));

    public static Expression Rewrite(Expression body) => Rewriter.Instance.Visit(body);

    sealed class Rewriter : ExpressionVisitor
    {
        public static readonly Rewriter Instance = new();

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(MemoryExtensions)
                && node.Method.Name == nameof(MemoryExtensions.Contains)
                && node.Arguments.Count == 2
                && TryUnwrapSpan(node.Arguments[0], out var source)
                && TryEvaluate(source, out var values))
                return InExpressionBuilder.Build(this.Visit(node.Arguments[1]), values, NullHandling.Raw);

            return base.VisitMethodCall(node);
        }

        // The array→span conversion appears as a call to the span's op_Implicit (or a Convert that uses it), with the
        // array itself — sometimes wrapped in a no-op Convert — as its operand.
        static bool TryUnwrapSpan(Expression expression, out Expression source)
        {
            source = expression switch
            {
                MethodCallExpression { Method.Name: "op_Implicit", Arguments.Count: 1 } call when IsSpan(call.Type) => call.Arguments[0],
                UnaryExpression { NodeType: ExpressionType.Convert } convert when IsSpan(convert.Type) => convert.Operand,
                _ => null!
            };
            if (source == null)
                return false;

            source = ClosureValueExtractor.StripConvert(source);
            return true;
        }

        static bool IsSpan(Type type)
            => type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>) || type.GetGenericTypeDefinition() == typeof(Span<>));

        // Only a collection fixed before the query runs becomes an IN list: a literal, a captured variable, or an
        // inline array of those. Anything reading the document stays as it was.
        static bool TryEvaluate(Expression source, out IEnumerable values)
        {
            values = Array.Empty<object>();
            if (!TryConstant(source, out var value)
                && !(source is NewArrayExpression { NodeType: ExpressionType.NewArrayInit } array && array.Expressions.All(e => TryConstant(ClosureValueExtractor.StripConvert(e), out _))))
                return false;

            value ??= ExpressionInterpreter.EvaluateClosed(source);
            if (value is not IEnumerable enumerable || value is string)
                return false;

            values = enumerable;
            return true;
        }

        static bool TryConstant(Expression expression, out object? value)
        {
            value = null;
            if (expression is ConstantExpression constant)
            {
                value = constant.Value;
                return true;
            }
            return expression is MemberExpression member && ClosureValueExtractor.TryExtractCapturedValue(member, out value);
        }
    }
}
