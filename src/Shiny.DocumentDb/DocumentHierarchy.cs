using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Inheritance: a base document type and the types derived from it, stored together under the base type and told
/// apart by a string discriminator property on the base.
/// <code>
/// options.ConfigureDocument&lt;Payment&gt;(cfg => cfg.MapDerivedTypes(x => x.Kind, d => d
///     .Add&lt;CardPayment&gt;("card")
///     .Add&lt;CashPayment&gt;("cash")));
///
/// await store.Insert(new CardPayment { Id = "p1", Amount = 10, Last4 = "4242" });   // Kind is set to "card"
/// var all   = await store.Query&lt;Payment&gt;().ToList();         // CardPayment and CashPayment instances
/// var cards = await store.Query&lt;CardPayment&gt;().ToList();     // only the cards
/// </code>
/// <para>
/// Every write serializes the document by its runtime type and sets the discriminator, so <c>Insert&lt;Payment&gt;(card)</c>
/// keeps the card's own properties. Every read looks at the discriminator and returns that type.
/// <c>Query&lt;CardPayment&gt;()</c> (and <c>Get&lt;CardPayment&gt;</c>) adds a named query filter
/// (<see cref="FilterName"/>) on the discriminator, and inherits the query filters, id property and version property
/// mapped on the base. Like upcasters, the hierarchy is a property of the CLR types and applies process-wide.
/// </para>
/// </summary>
public static class DocumentHierarchy
{
    /// <summary>The name of the query filter that narrows a derived type's queries to its discriminator values.</summary>
    public const string FilterName = "discriminator";
}

/// <summary>Collects the derived types of a <c>MapDerivedTypes</c> call.</summary>
/// <typeparam name="T">The base document type.</typeparam>
public sealed class DerivedTypeBuilder<T> where T : class
{
    internal DerivedTypeBuilder() { }

    internal List<(Type Type, string Value, Func<Expression, ParameterExpression, IReadOnlyList<string>, LambdaExpression> Filter, Func<LambdaExpression, LambdaExpression> Retarget)> Types { get; } = new();

    /// <summary>Maps <typeparamref name="TDerived"/> under <paramref name="discriminator"/>. A derived type may itself be
    /// derived from another mapped type; querying the middle type returns both.</summary>
    public DerivedTypeBuilder<T> Add<TDerived>(string discriminator) where TDerived : class, T
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(discriminator);
        if (typeof(TDerived) == typeof(T))
            throw new ArgumentException($"'{typeof(T).Name}' is the base type; give it a value with MapDerivedTypes(..., baseValue: \"...\") instead.");
        this.Types.Add((typeof(TDerived), discriminator, BuildFilter<TDerived>, Retarget<TDerived>));
        return this;
    }

    // Generic so the lambdas are built without MakeGenericType — AOT-safe.
    static LambdaExpression BuildFilter<TDerived>(Expression body, ParameterExpression parameter, IReadOnlyList<string> values)
        => Expression.Lambda<Func<TDerived, bool>>(body, parameter);

    static LambdaExpression Retarget<TDerived>(LambdaExpression predicate)
    {
        var parameter = Expression.Parameter(typeof(TDerived), predicate.Parameters[0].Name);
        var body = new ParameterRetargeter(predicate.Parameters[0], parameter).Visit(predicate.Body);
        return Expression.Lambda<Func<TDerived, bool>>(body, parameter);
    }
}

/// <summary>Maps a document type's derived types.</summary>
public static class DocumentHierarchyOptionsExtensions
{
    /// <summary>
    /// Stores the types derived from <typeparamref name="T"/> under <typeparamref name="T"/>, told apart by the string
    /// property <paramref name="discriminator"/>. See <see cref="DocumentHierarchy"/>.
    /// </summary>
    /// <param name="cfg">The base type's configuration.</param>
    /// <param name="discriminator">A settable string property on the base type, e.g. <c>x =&gt; x.Kind</c>. The store sets it on every write.</param>
    /// <param name="configure">Adds the derived types and their discriminator values.</param>
    /// <param name="baseValue">The base type's own value, when it is not abstract. A document with a missing or unknown
    /// discriminator is read as the base type.</param>
    public static DocumentTypeBuilder<T> MapDerivedTypes<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        this DocumentTypeBuilder<T> cfg,
        Expression<Func<T, string?>> discriminator,
        Action<DerivedTypeBuilder<T>> configure,
        string? baseValue = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(cfg);
        ArgumentNullException.ThrowIfNull(discriminator);
        ArgumentNullException.ThrowIfNull(configure);

        if (discriminator.Body is not MemberExpression { Member: PropertyInfo property } member || member.Expression is not ParameterExpression)
            throw new ArgumentException($"The discriminator must be a top-level property of '{typeof(T).Name}' (e.g. x => x.Kind).", nameof(discriminator));
        if (!property.CanWrite)
            throw new ArgumentException($"The discriminator '{typeof(T).Name}.{property.Name}' needs a setter: the store sets it on every write.", nameof(discriminator));

        var derived = new DerivedTypeBuilder<T>();
        configure(derived);
        if (derived.Types.Count == 0)
            throw new ArgumentException($"MapDerivedTypes on '{typeof(T).Name}' needs at least one derived type.", nameof(configure));

        DocumentShapes.RegisterHierarchy(typeof(T), property, baseValue);
        foreach (var (type, value, _, retarget) in derived.Types)
            DocumentShapes.RegisterDerived(typeof(T), type, value, retarget);

        // Each derived type's queries are narrowed to its own value and those of the mapped types below it.
        foreach (var (type, _, filter, _) in derived.Types)
        {
            var parameter = Expression.Parameter(type, "x");
            var values = DocumentShapes.DiscriminatorValuesFor(typeof(T), type);
            var body = values
                .Select(v => (Expression)Expression.Equal(Expression.Property(parameter, property), Expression.Constant(v, typeof(string))))
                .Aggregate(Expression.OrElse);
            cfg.Mappings.AddQueryFilter(type, DocumentHierarchy.FilterName, filter(body, parameter, values));
        }
        return cfg;
    }
}
