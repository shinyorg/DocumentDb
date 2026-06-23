using Aspire.Hosting.ApplicationModel;

namespace Shiny.DocumentDb.Aspire.Hosting;

/// <summary>
/// An Aspire resource that represents a DocumentDb store layered on top of a backing database
/// resource. It forwards the backing resource's connection string (so <c>WithReference(store)</c>
/// injects <c>ConnectionStrings:&lt;name&gt;</c>) and carries the <see cref="DocumentProviderKind"/>
/// the client integration reads to pick the matching provider.
/// </summary>
public sealed class DocumentStoreResource : Resource, IResourceWithConnectionString, IResourceWithParent
{
    readonly IResourceWithConnectionString backing;

    public DocumentStoreResource(string name, IResourceWithConnectionString backing, DocumentProviderKind kind)
        : base(name)
    {
        this.backing = backing;
        this.Kind = kind;
    }

    /// <summary>The backing database provider this store is configured against.</summary>
    public DocumentProviderKind Kind { get; }

    /// <summary>The underlying database resource that supplies the connection string.</summary>
    public IResourceWithConnectionString Backing => this.backing;

    public IResource Parent => this.backing;

    public ReferenceExpression ConnectionStringExpression => this.backing.ConnectionStringExpression;
}
