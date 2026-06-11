namespace Shiny.DocumentDb;

/// <summary>
/// Teaches the document store how a custom Id CLR type round-trips to the string that every
/// provider already stores (the SQL <c>Id</c> column, MongoDB <c>_id</c>/<c>id</c>, Cosmos <c>id</c>).
/// Register an implementation with <c>MapIdType</c> on the store options to use Id types beyond the
/// built-in <see cref="System.Guid"/>, <see cref="int"/>, <see cref="long"/>, and <see cref="string"/>.
/// </summary>
/// <typeparam name="TId">The Id property's CLR type (e.g. <c>Ulid</c> or a strongly-typed wrapper).</typeparam>
public abstract class DocumentIdConverter<TId>
{
    /// <summary>Converts an Id value to the canonical string written to storage.</summary>
    public abstract string ToStorageString(TId id);

    /// <summary>Converts a stored string back to the Id value. Must round-trip <see cref="ToStorageString"/>.</summary>
    public abstract TId FromStorageString(string id);

    /// <summary>
    /// Returns <c>true</c> when the Id is "unset" and should trigger auto-generation on Insert.
    /// Defaults to the type's <c>default</c> value (e.g. <c>Guid.Empty</c>, <c>0</c>, <c>null</c>).
    /// </summary>
    public virtual bool IsDefault(TId id) => EqualityComparer<TId>.Default.Equals(id, default!);

    /// <summary>
    /// Optionally generates a new Id on Insert when <see cref="IsDefault"/> is <c>true</c>.
    /// Return <c>false</c> (the default) to require the caller to assign the Id explicitly.
    /// </summary>
    public virtual bool TryGenerate(out TId id)
    {
        id = default!;
        return false;
    }
}
