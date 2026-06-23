namespace Shiny.DocumentDb.OData;

/// <summary>
/// Governance limits applied to a parsed <see cref="ODataQueryModel"/> before it executes — the security
/// surface a public OData endpoint needs: result-size caps, allowed system query options, per-property
/// allowlists, and filter-complexity limits. <see cref="Apply"/> validates a model (throwing
/// <see cref="ODataQueryPolicyException"/> on a violation) and clamps the effective page size.
/// <para>
/// Provider-neutral and dependency-free: it operates only on the engine's own model, so the same policy
/// works under the ASP.NET host and any other front-end. All defaults are permissive (no limits, every
/// option/property/function allowed) so an unconfigured policy changes nothing.
/// </para>
/// </summary>
public sealed class ODataQueryPolicy
{
    /// <summary>Page size applied when the request omits <c>$top</c>. Bounds otherwise-unbounded reads.</summary>
    public int? DefaultPageSize { get; set; }

    /// <summary>Largest permitted <c>$top</c>. A larger <c>$top</c> is rejected; the effective page size is also clamped to this.</summary>
    public int? MaxTop { get; set; }

    /// <summary>Largest permitted <c>$skip</c>. A larger <c>$skip</c> is rejected.</summary>
    public int? MaxSkip { get; set; }

    /// <summary>Maximum number of nodes in the <c>$filter</c> tree (complexity / DoS guard).</summary>
    public int? MaxFilterNodeCount { get; set; }

    /// <summary>Maximum number of <c>$orderby</c> terms.</summary>
    public int? MaxOrderByNodeCount { get; set; }

    /// <summary>When false, any <c>$filter</c> is rejected.</summary>
    public bool AllowFilter { get; set; } = true;

    /// <summary>When false, any <c>$orderby</c> is rejected.</summary>
    public bool AllowOrderBy { get; set; } = true;

    /// <summary>When false, any <c>$select</c> is rejected.</summary>
    public bool AllowSelect { get; set; } = true;

    /// <summary>When false, <c>$count=true</c> is rejected.</summary>
    public bool AllowCount { get; set; } = true;

    /// <summary>When false, arithmetic operators (<c>add sub mul div mod</c>) in <c>$filter</c> are rejected.</summary>
    public bool AllowArithmetic { get; set; } = true;

    /// <summary>Allowed <c>$filter</c> function names (e.g. <c>contains</c>, <c>startswith</c>). Empty = all allowed.</summary>
    public ISet<string> AllowedFunctions { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>Properties a client may <c>$filter</c> on (matched on the root path segment). Empty = all allowed.</summary>
    public ISet<string> FilterableProperties { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>Properties a client may <c>$orderby</c> on (root segment). Empty = all allowed.</summary>
    public ISet<string> SortableProperties { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>Properties a client may <c>$select</c>. Empty = all allowed.</summary>
    public ISet<string> SelectableProperties { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    public ODataQueryPolicy() { }

    /// <summary>Copy constructor — used to clone a shared default policy before applying per-entity-set overrides.</summary>
    public ODataQueryPolicy(ODataQueryPolicy other)
    {
        ArgumentNullException.ThrowIfNull(other);
        this.DefaultPageSize = other.DefaultPageSize;
        this.MaxTop = other.MaxTop;
        this.MaxSkip = other.MaxSkip;
        this.MaxFilterNodeCount = other.MaxFilterNodeCount;
        this.MaxOrderByNodeCount = other.MaxOrderByNodeCount;
        this.AllowFilter = other.AllowFilter;
        this.AllowOrderBy = other.AllowOrderBy;
        this.AllowSelect = other.AllowSelect;
        this.AllowCount = other.AllowCount;
        this.AllowArithmetic = other.AllowArithmetic;
        this.AllowedFunctions = new HashSet<string>(other.AllowedFunctions, StringComparer.OrdinalIgnoreCase);
        this.FilterableProperties = new HashSet<string>(other.FilterableProperties, StringComparer.OrdinalIgnoreCase);
        this.SortableProperties = new HashSet<string>(other.SortableProperties, StringComparer.OrdinalIgnoreCase);
        this.SelectableProperties = new HashSet<string>(other.SelectableProperties, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Validates <paramref name="model"/> against this policy and clamps its <c>$top</c> to the effective
    /// page size (<c>$top</c> ?? <see cref="DefaultPageSize"/> ?? <see cref="MaxTop"/>, never above
    /// <see cref="MaxTop"/>). Throws <see cref="ODataQueryPolicyException"/> on the first violation.
    /// </summary>
    public void Apply(ODataQueryModel model)
    {
        ArgumentNullException.ThrowIfNull(model);

        if (!this.AllowFilter && model.Filter != null)
            throw new ODataQueryPolicyException("The $filter query option is not allowed on this entity set.");
        if (!this.AllowOrderBy && model.OrderBy.Count > 0)
            throw new ODataQueryPolicyException("The $orderby query option is not allowed on this entity set.");
        if (!this.AllowSelect && !string.IsNullOrWhiteSpace(model.Select))
            throw new ODataQueryPolicyException("The $select query option is not allowed on this entity set.");
        if (!this.AllowCount && model.Count)
            throw new ODataQueryPolicyException("The $count query option is not allowed on this entity set.");

        if (this.MaxSkip is { } maxSkip && model.Skip is { } skip && skip > maxSkip)
            throw new ODataQueryPolicyException($"The $skip value '{skip}' exceeds the maximum allowed value '{maxSkip}'.");
        if (this.MaxTop is { } maxTop && model.Top is { } top && top > maxTop)
            throw new ODataQueryPolicyException($"The $top value '{top}' exceeds the maximum allowed value '{maxTop}'.");
        if (this.MaxOrderByNodeCount is { } maxOrder && model.OrderBy.Count > maxOrder)
            throw new ODataQueryPolicyException($"The $orderby has {model.OrderBy.Count} terms, exceeding the maximum of {maxOrder}.");

        if (model.Filter != null)
            this.ValidateFilter(model.Filter);

        if (this.SortableProperties.Count > 0)
        {
            foreach (var item in model.OrderBy)
            {
                if (!this.SortableProperties.Contains(RootSegment(item.PropertyPath)))
                    throw new ODataQueryPolicyException($"Ordering by property '{item.PropertyPath}' is not allowed.");
            }
        }

        if (this.SelectableProperties.Count > 0 && !string.IsNullOrWhiteSpace(model.Select))
        {
            foreach (var field in model.Select!.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (!this.SelectableProperties.Contains(field))
                    throw new ODataQueryPolicyException($"Selecting property '{field}' is not allowed.");
            }
        }

        // Clamp the effective page size. $top was already validated against MaxTop above.
        var effectiveTop = model.Top ?? this.DefaultPageSize ?? this.MaxTop;
        if (effectiveTop is { } e)
            model.Top = this.MaxTop is { } cap ? Math.Min(e, cap) : e;
    }

    void ValidateFilter(ODataFilterNode root)
    {
        var nodeCount = 0;
        var stack = new Stack<ODataFilterNode>();
        stack.Push(root);
        while (stack.Count > 0)
        {
            var node = stack.Pop();
            nodeCount++;
            if (this.MaxFilterNodeCount is { } max && nodeCount > max)
                throw new ODataQueryPolicyException($"The $filter is too complex ({max} node maximum).");

            switch (node.Kind)
            {
                case ODataNodeKind.Property when this.FilterableProperties.Count > 0
                                                 && !this.FilterableProperties.Contains(RootSegment(node.PropertyPath!)):
                    throw new ODataQueryPolicyException($"Filtering on property '{node.PropertyPath}' is not allowed.");

                case ODataNodeKind.Function when this.AllowedFunctions.Count > 0
                                                 && !this.AllowedFunctions.Contains(node.FunctionName!):
                    throw new ODataQueryPolicyException($"The $filter function '{node.FunctionName}' is not allowed.");

                case ODataNodeKind.Binary when !this.AllowArithmetic && IsArithmetic(node.BinaryOperator):
                    throw new ODataQueryPolicyException($"Arithmetic operators are not allowed in $filter.");
            }

            if (node.Left != null) stack.Push(node.Left);
            if (node.Right != null) stack.Push(node.Right);
            if (node.Operand != null) stack.Push(node.Operand);
            foreach (var arg in node.Arguments)
                stack.Push(arg);
        }
    }

    static bool IsArithmetic(ODataBinaryOperator op) => op
        is ODataBinaryOperator.Add or ODataBinaryOperator.Subtract or ODataBinaryOperator.Multiply
        or ODataBinaryOperator.Divide or ODataBinaryOperator.Modulo;

    // Allowlists match the root EDM property: "Address/City" → "Address".
    static string RootSegment(string path)
    {
        var slash = path.IndexOfAny(['/', '.']);
        return slash < 0 ? path : path[..slash];
    }
}
