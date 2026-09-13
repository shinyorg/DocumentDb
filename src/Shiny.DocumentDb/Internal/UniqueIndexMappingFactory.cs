using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text;
using Shiny.DocumentDb.Internal.Query;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Builds <see cref="UniqueIndexMapping"/>s from the <c>MapUniqueIndex</c> / <c>Unique</c> surface. The key
/// selector is <c>x =&gt; x.Email</c> for one part or an anonymous type (<c>x =&gt; new { x.Region, x.Email }</c>)
/// for several; each part must be a plain property chain on the document. The filter is interpreted, never
/// compiled, so the feature stays AOT/trim-safe.
/// </summary>
static class UniqueIndexMappingFactory
{
    // PostgreSQL truncates identifiers at 63 bytes; MySQL rejects anything over 64. The storage name adds a 9-character
    // table suffix (UniqueIndexMapping.GetStorageName), so capping here at 51 keeps the name the backend reports in a
    // constraint error identical to the one we compare against.
    const int MaxNameLength = 51;

    public static UniqueIndexMapping Create<T>(
        string typeName,
        Expression<Func<T, object?>> properties,
        Expression<Func<T, bool>>? filter,
        string? name) where T : class
    {
        ArgumentNullException.ThrowIfNull(properties);
        if (name != null)
            ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var parameter = properties.Parameters[0];
        var body = StripConvert(properties.Body);
        IReadOnlyList<Expression> parts = body is NewExpression { Arguments.Count: > 0 } anonymous
            ? anonymous.Arguments
            : [body];

        var selectors = new List<Expression>(parts.Count);
        var chains = new List<IReadOnlyList<string>>(parts.Count);
        foreach (var part in parts)
        {
            var selector = StripConvert(part);
            var chain = MemberChain(selector, parameter)
                ?? throw new ArgumentException(
                    $"Unique index key parts on '{typeof(T).Name}' must be property accesses on the document (x => x.Email, or x => new {{ x.Region, x.Email }}); '{part}' is not.",
                    nameof(properties));

            if (chains.Any(c => c.SequenceEqual(chain)))
                throw new ArgumentException($"'{string.Join('.', chain)}' appears more than once in the unique index key on '{typeof(T).Name}'.", nameof(properties));

            selectors.Add(selector);
            chains.Add(chain);
        }

        Func<object, bool>? predicate = null;
        if (filter != null)
        {
            var interpreted = ExpressionInterpreter.Interpret(filter);
            predicate = document => interpreted((T)document);
        }

        var label = name ?? string.Join("__", chains.Select(c => string.Join('_', c)));
        var indexName = BuildName(typeName, label);
        return new UniqueIndexMapping(typeof(T), indexName, parameter, selectors, chains, filter, predicate);
    }

    static string BuildName(string typeName, string label)
    {
        var full = $"uq_{IDatabaseProvider.SanitizeTypeSuffix(typeName)}_{IDatabaseProvider.SanitizeTypeSuffix(label)}";
        if (full.Length <= MaxNameLength)
            return full;

        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(full))).ToLowerInvariant()[..8];
        return full[..(MaxNameLength - 9)] + "_" + hash;
    }

    static List<string>? MemberChain(Expression expression, ParameterExpression parameter)
    {
        var chain = new List<string>();
        var current = expression;
        while (current is MemberExpression member)
        {
            chain.Insert(0, member.Member.Name);
            current = member.Expression;
        }
        return chain.Count > 0 && current == parameter ? chain : null;
    }

    static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
            expression = convert.Operand;
        return expression;
    }
}
