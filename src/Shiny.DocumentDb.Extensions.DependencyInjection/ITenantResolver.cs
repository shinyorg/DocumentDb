namespace Shiny.DocumentDb;

/// <summary>
/// Implement this interface to provide the current tenant identifier.
/// Register your implementation with the DI container before calling
/// AddMultiTenantDocumentStore or AddDocumentStore with multiTenant: true.
/// </summary>
public interface ITenantResolver
{
    /// <summary>
    /// Returns the current tenant identifier.
    /// Throws if no tenant context is available.
    /// </summary>
    string GetCurrentTenant();
}
