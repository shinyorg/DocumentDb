namespace Shiny.DocumentDb.OData;

/// <summary>
/// Thrown when a parsed <see cref="ODataQueryModel"/> violates a governance <see cref="ODataQueryPolicy"/>
/// — a disallowed query option, a property outside an allowlist, a too-large <c>$top</c>/<c>$skip</c>, or
/// an over-complex filter. The ASP.NET host maps this to <c>400 Bad Request</c> (the request is
/// well-formed but not permitted), distinct from <see cref="ODataNotSupportedException"/> which is a
/// <c>501</c> (the engine cannot express the request at all).
/// </summary>
public sealed class ODataQueryPolicyException : Exception
{
    public ODataQueryPolicyException(string message) : base(message) { }
}
