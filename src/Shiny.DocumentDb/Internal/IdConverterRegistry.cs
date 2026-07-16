using System.Diagnostics.CodeAnalysis;

namespace Shiny.DocumentDb.Internal;

/// <summary>Non-generic runtime face of a <see cref="DocumentIdConverter{TId}"/>.</summary>
public interface IIdConverter
{
    Type IdType { get; }
    string ToStorageString(object id);
    object FromStorageString(string id);
    bool IsDefault(object? id);
    bool TryGenerate(out object id);
}

/// <summary>Boxes a strongly-typed <see cref="DocumentIdConverter{TId}"/> behind <see cref="IIdConverter"/>.</summary>
internal sealed class IdConverterAdapter<TId> : IIdConverter
{
    readonly DocumentIdConverter<TId> inner;

    public IdConverterAdapter(DocumentIdConverter<TId> inner) => this.inner = inner;

    public Type IdType => typeof(TId);
    public string ToStorageString(object id) => this.inner.ToStorageString((TId)id);
    public object FromStorageString(string id) => this.inner.FromStorageString(id)!;
    public bool IsDefault(object? id) => this.inner.IsDefault(id is null ? default! : (TId)id);

    public bool TryGenerate(out object id)
    {
        if (this.inner.TryGenerate(out var value))
        {
            id = value!;
            return true;
        }
        id = null!;
        return false;
    }
}

/// <summary>A <see cref="DocumentIdConverter{TId}"/> built from inline delegates (the <c>MapIdType(toString, parse, …)</c> overload).</summary>
public sealed class DelegateIdConverter<TId> : DocumentIdConverter<TId>
{
    readonly Func<TId, string> toStorageString;
    readonly Func<string, TId> fromStorageString;
    readonly Func<TId, bool>? isDefault;
    readonly Func<TId>? generate;

    public DelegateIdConverter(
        Func<TId, string> toStorageString,
        Func<string, TId> fromStorageString,
        Func<TId, bool>? isDefault,
        Func<TId>? generate)
    {
        this.toStorageString = toStorageString;
        this.fromStorageString = fromStorageString;
        this.isDefault = isDefault;
        this.generate = generate;
    }

    public override string ToStorageString(TId id) => this.toStorageString(id);
    public override TId FromStorageString(string id) => this.fromStorageString(id);
    public override bool IsDefault(TId id) => this.isDefault?.Invoke(id) ?? base.IsDefault(id);

    public override bool TryGenerate(out TId id)
    {
        if (this.generate != null)
        {
            id = this.generate();
            return true;
        }
        id = default!;
        return false;
    }
}

/// <summary>Maps an Id CLR type to its registered <see cref="IIdConverter"/>. Empty by default — built-in Guid/int/long/string need no entry.</summary>
public sealed class IdConverterRegistry
{
    readonly Dictionary<Type, IIdConverter> converters = new();

    public void Register<TId>(DocumentIdConverter<TId> converter)
        => this.converters[typeof(TId)] = new IdConverterAdapter<TId>(converter);

    public bool TryGet(Type idType, [NotNullWhen(true)] out IIdConverter? converter)
        => this.converters.TryGetValue(idType, out converter);

    public bool IsEmpty => this.converters.Count == 0;
}
