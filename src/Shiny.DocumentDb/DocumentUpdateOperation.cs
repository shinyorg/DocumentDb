namespace Shiny.DocumentDb;

/// <summary>What a <see cref="DocumentUpdateOperation"/> does to the JSON value at its path.</summary>
public enum DocumentUpdateKind
{
    /// <summary>Adds a number to the value at the path. A missing or null value counts as zero.</summary>
    Increment,

    /// <summary>Removes the property from the document.</summary>
    Unset,

    /// <summary>Appends an item to the array at the path, creating the array when it is missing or null.</summary>
    Push,

    /// <summary>Removes every element of the array at the path that equals the item.</summary>
    Pull,

    /// <summary>Appends an item to the array at the path unless an equal element is already there.</summary>
    AddToSet
}

/// <summary>
/// A set-based update that is more than "set this path to this value": an increment, a property removal, or an
/// array push / pull / add-to-set. Built by <see cref="IDocumentUpdateBuilder{T}"/>, or directly through the
/// factory methods for the JSON-path overloads:
/// <code>
/// await store.Collection("orders").Query().Where("status == 'open'")
///     .ExecuteUpdate(new Dictionary&lt;string, object?&gt; { ["retries"] = DocumentUpdateOperation.Increment(1) });
/// </code>
/// <para>
/// An operation travels in the <c>Value</c> slot of an update assignment, so a bulk interceptor sees it in
/// <see cref="DocumentBulkContext.Assignments"/> next to plain values. Array elements compare by JSON value:
/// two objects are equal when they serialize to the same properties and values.
/// </para>
/// </summary>
public sealed class DocumentUpdateOperation
{
    DocumentUpdateOperation(DocumentUpdateKind kind, object? operand)
    {
        this.Kind = kind;
        this.Operand = operand;
    }

    /// <summary>The kind of operation.</summary>
    public DocumentUpdateKind Kind { get; }

    /// <summary>The number to add (<see cref="DocumentUpdateKind.Increment"/>), the array item
    /// (<see cref="DocumentUpdateKind.Push"/> / <see cref="DocumentUpdateKind.Pull"/> /
    /// <see cref="DocumentUpdateKind.AddToSet"/>), or null (<see cref="DocumentUpdateKind.Unset"/>).</summary>
    public object? Operand { get; }

    /// <summary>Adds <paramref name="amount"/> to the number at the path. Pass a negative amount to subtract.</summary>
    public static DocumentUpdateOperation Increment(object amount)
    {
        ArgumentNullException.ThrowIfNull(amount);
        if (!IsNumber(amount))
            throw new ArgumentException($"Increment needs a number; '{amount.GetType().Name}' is not one.", nameof(amount));
        if (amount is double d && !double.IsFinite(d) || amount is float f && !float.IsFinite(f))
            throw new ArgumentException("Increment needs a finite number.", nameof(amount));
        return new(DocumentUpdateKind.Increment, amount);
    }

    /// <summary>Removes the property at the path.</summary>
    public static DocumentUpdateOperation Unset() => new(DocumentUpdateKind.Unset, null);

    /// <summary>Appends <paramref name="item"/> to the array at the path.</summary>
    public static DocumentUpdateOperation Push(object? item) => new(DocumentUpdateKind.Push, item);

    /// <summary>Removes every element equal to <paramref name="item"/> from the array at the path.</summary>
    public static DocumentUpdateOperation Pull(object? item) => new(DocumentUpdateKind.Pull, item);

    /// <summary>Appends <paramref name="item"/> to the array at the path unless an equal element is already there.</summary>
    public static DocumentUpdateOperation AddToSet(object? item) => new(DocumentUpdateKind.AddToSet, item);

    /// <inheritdoc />
    public override string ToString() => this.Kind == DocumentUpdateKind.Unset ? "Unset" : $"{this.Kind}({this.Operand})";

    internal static bool IsNumber(object value) => value is int or long or short or byte or sbyte or uint or ulong or ushort or decimal or double or float;
}
