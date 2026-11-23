namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// Points equality and <c>IN</c> tests on the document's id property at the envelope <c>Id</c> column. The body and
/// the column hold the same string for a string id, so the result is unchanged, but the column is the leading key of
/// the primary key: <c>WhereIn(x =&gt; x.Id, ids)</c> becomes index seeks instead of a scan of every row's JSON.
/// </summary>
/// <remarks>
/// Relational only (the emitter is the sole consumer of <see cref="IdColumnNode"/>). Ordering and range comparisons
/// are left on the body: the column is text, so they would order differently for anything but plain strings.
/// </remarks>
static class IdColumnRewriter
{
    public static PredicateNode Rewrite(PredicateNode node, string idJsonPath) => node switch
    {
        LogicalNode l => l with { Left = Rewrite(l.Left, idJsonPath), Right = Rewrite(l.Right, idJsonPath) },
        NotNode n => n with { Operand = Rewrite(n.Operand, idJsonPath) },
        CompareNode c when c.Op is CompareOp.Equal or CompareOp.NotEqual => RewriteCompare(c, idJsonPath),
        InNode i when IsIdField(i.Item, idJsonPath) && i.Values.All(v => v is string) => i with { Item = new IdColumnNode() },
        _ => node
    };

    static CompareNode RewriteCompare(CompareNode node, string idJsonPath)
    {
        if (IsIdField(node.Left, idJsonPath) && node.Right is ConstantNode { Value: string })
            return node with { Left = new IdColumnNode() };

        if (IsIdField(node.Right, idJsonPath) && node.Left is ConstantNode { Value: string })
            return node with { Right = new IdColumnNode() };

        return node;
    }

    // Only the root document's own field: a joined side (Source set) or a nested property of the same name is not the key.
    static bool IsIdField(ValueNode node, string idJsonPath)
        => node is RootFieldNode { Source: null } f && f.ClrType == typeof(string) && f.JsonPath == idJsonPath;
}
