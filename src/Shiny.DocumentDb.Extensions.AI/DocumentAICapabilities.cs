namespace Shiny.DocumentDb.Extensions.AI;

[Flags]
public enum DocumentAICapabilities
{
    None      = 0,
    Get       = 1 << 0,
    Query     = 1 << 1,
    Count     = 1 << 2,
    Aggregate = 1 << 3,
    Insert    = 1 << 4,
    Update    = 1 << 5,
    Delete    = 1 << 6,
    ReadOnly  = Get | Query | Count | Aggregate,
    All       = ReadOnly | Insert | Update | Delete
}
