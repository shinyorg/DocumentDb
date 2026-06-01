namespace Shiny.DocumentDb.Internal;

/// <summary>
/// A provider-level change record produced by a native change feed before it is mapped to a typed
/// <see cref="DocumentChange{T}"/> by the store. <see cref="Json"/> carries the document body when
/// the underlying mechanism makes it cheaply available (it is <c>null</c> for deletes and for
/// notification-only mechanisms such as PostgreSQL <c>NOTIFY</c>).
/// </summary>
public readonly record struct RawDocumentChange(DocumentChangeType ChangeType, string Id, string? Json);
