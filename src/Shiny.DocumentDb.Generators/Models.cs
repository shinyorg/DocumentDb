using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace Shiny.DocumentDb.Generators;

sealed record ContextModel(
    string? Namespace,
    string Name,
    string Accessibility,
    bool HasStoreConstructor,
    bool CanEmit,
    bool EmitDi,
    EquatableArray<DocumentDecl> Documents,
    EquatableArray<MetaType> GeneratedMetadata,
    EquatableArray<DiagInfo> Diagnostics
)
{
    public string HintName =>
        (this.Namespace == null ? this.Name : this.Namespace + "." + this.Name) + ".DocumentContext.g.cs";

    public bool HasGenerated => this.GeneratedMetadata.Count > 0;

    public string ResolverName => this.Name + "GeneratedResolver";
}

sealed record DocumentDecl(
    string TypeFullName,
    string SetName,
    string? Table,
    string? IdProperty,
    string? JsonContextFullName
);

enum MetaKind { Object, Value, List, Array }

// One entry the generated IJsonTypeInfoResolver must produce metadata for.
sealed record MetaType(
    MetaKind Kind,
    string FullName,          // global:: type used in typeof(...) / generic args
    string SafeName,          // identifier-safe suffix for Create_<SafeName>
    string? ConverterExpr,    // Value: the JsonMetadataServices converter expression
    string? ElementFullName,  // List/Array: element type
    string? ObjectCreator,    // Object: "new global::T()" or null
    EquatableArray<MetaProp> Properties
);

sealed record MetaProp(
    string ClrName,
    string? JsonName,         // explicit [JsonPropertyName], else null (naming policy applies)
    string PropTypeFullName,  // TProp generic for Prop<>
    string OwnerFullName
);

sealed record DiagInfo
{
    public DiagInfo(DiagnosticDescriptor descriptor, Location location, string? arg0 = null, string? arg1 = null)
    {
        this.Descriptor = descriptor;
        this.Location = location;
        this.Arg0 = arg0;
        this.Arg1 = arg1;
    }

    public DiagnosticDescriptor Descriptor { get; }
    public Location Location { get; }
    public string? Arg0 { get; }
    public string? Arg1 { get; }

    public Diagnostic ToDiagnostic()
    {
        var args = (this.Arg0, this.Arg1) switch
        {
            (null, _) => Array.Empty<object?>(),
            (_, null) => new object?[] { this.Arg0 },
            _ => new object?[] { this.Arg0, this.Arg1 }
        };
        return Diagnostic.Create(this.Descriptor, this.Location, args);
    }
}

/// <summary>Value-equatable wrapper over <see cref="ImmutableArray{T}"/> so incremental models cache correctly.</summary>
readonly struct EquatableArray<T> : IEquatable<EquatableArray<T>> where T : IEquatable<T>
{
    readonly ImmutableArray<T> array;

    public EquatableArray(ImmutableArray<T> array) => this.array = array;

    public int Count => this.array.IsDefault ? 0 : this.array.Length;

    public ImmutableArray<T>.Enumerator GetEnumerator() =>
        (this.array.IsDefault ? ImmutableArray<T>.Empty : this.array).GetEnumerator();

    public bool Equals(EquatableArray<T> other)
    {
        if (this.array.IsDefault || other.array.IsDefault)
            return this.array.IsDefault && other.array.IsDefault;

        if (this.array.Length != other.array.Length)
            return false;

        for (var i = 0; i < this.array.Length; i++)
        {
            if (!EqualityComparer<T>.Default.Equals(this.array[i], other.array[i]))
                return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => obj is EquatableArray<T> other && this.Equals(other);

    public override int GetHashCode()
    {
        if (this.array.IsDefault)
            return 0;

        var hash = 17;
        foreach (var item in this.array)
            hash = hash * 31 + (item?.GetHashCode() ?? 0);
        return hash;
    }
}
