namespace Shiny.DocumentDb.OData;

/// <summary>
/// Thrown when a request uses an OData feature the engine intentionally does not support — e.g.
/// <c>$expand</c> (no document relationships), a complex <c>$apply</c> transform, or a spatial
/// filter against a non-spatial provider. The host maps this to HTTP <c>501 Not Implemented</c>.
/// Capability gating always surfaces an error rather than silently dropping a predicate, which would
/// be a correctness / data-leak hazard.
/// </summary>
public sealed class ODataNotSupportedException : Exception
{
    public ODataNotSupportedException(string message) : base(message) { }
}
