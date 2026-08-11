using System.Linq.Expressions;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Hosting;

/// <summary>
/// Turns a document predicate into a delegate that can be run against an in-memory instance.
/// </summary>
/// <remarks>
/// <para>
/// This exists for packages that <b>host</b> DocumentDb over a transport (HTTP, MCP, a message pump). A host
/// has to answer "is this document inside the caller's scope?" for a document that has not been stored yet —
/// an incoming POST/PUT body — where there is nothing to query against, so the same predicate it pushes into
/// <see cref="IDocumentQuery{T}.Where(Expression{Func{T, bool}})"/> must also be evaluable in memory.
/// </para>
/// <para>
/// <b>The contract is that it never uses <c>Reflection.Emit</c>.</b> That is why a host calls this instead of
/// <see cref="Expression{TDelegate}.Compile()"/>: emitting IL at runtime forfeits the trimmed/Native-AOT
/// guarantee of the app doing the hosting. The tree-walk behind it is an implementation detail; the absence of
/// runtime code generation is not.
/// </para>
/// <para>
/// It is also the same evaluator the query layer's in-memory paths use, so a host cannot end up with the
/// pushed-down predicate and the in-memory check disagreeing about what a scope means — which would be a
/// scope bypass, not a cosmetic difference.
/// </para>
/// </remarks>
public static class DocumentPredicate
{
    /// <summary>
    /// Builds a delegate equivalent to <c>predicate.Compile()</c> without generating any IL.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="predicate">The predicate — the same expression that would be handed to <c>Where</c>.</param>
    /// <returns>A delegate that evaluates <paramref name="predicate"/> against an instance.</returns>
    /// <exception cref="NotSupportedException">
    /// Thrown when the delegate runs and the expression contains a node the interpreter does not handle.
    /// Building the delegate never inspects the tree, so an unsupported node surfaces on first evaluation.
    /// </exception>
    public static Func<T, bool> Compile<T>(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        return ExpressionInterpreter.Interpret(predicate);
    }
}
