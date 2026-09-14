namespace Shiny.DocumentDb;

/// <summary>How a join treats a left document that has no matching right document.</summary>
public enum JoinKind
{
    /// <summary>Only pairs where both sides match are returned.</summary>
    Inner,

    /// <summary>Every left document is returned; its right side is <c>null</c> when nothing matched.</summary>
    Left
}
