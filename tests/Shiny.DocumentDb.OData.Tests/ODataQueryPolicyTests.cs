using Xunit;

namespace Shiny.DocumentDb.OData.Tests;

/// <summary>
/// Governance enforcement on a parsed <see cref="ODataQueryModel"/>: result caps, allowed system query
/// options, per-property allowlists, and filter-complexity limits. The ASP.NET host maps a thrown
/// <see cref="ODataQueryPolicyException"/> to <c>400</c>.
/// </summary>
public class ODataQueryPolicyTests
{
    static ODataQueryModel Model(string query) => ODataQueryModelParser.ParseQueryString(query);

    static void Allows(ODataQueryPolicy policy, string query) => policy.Apply(Model(query));

    static ODataQueryPolicyException Rejects(ODataQueryPolicy policy, string query)
        => Assert.Throws<ODataQueryPolicyException>(() => policy.Apply(Model(query)));

    // ── Result-size caps ──────────────────────────────────────────────────────

    [Fact]
    public void MaxTop_RejectsLargerTop()
    {
        var p = new ODataQueryPolicy { MaxTop = 50 };
        Rejects(p, "$top=51");
        Allows(p, "$top=50");
    }

    [Fact]
    public void DefaultPageSize_AppliedWhenTopOmitted()
    {
        var m = Model("$filter=Country eq 'CA'");
        new ODataQueryPolicy { DefaultPageSize = 25 }.Apply(m);
        Assert.Equal(25, m.Top);
    }

    [Fact]
    public void EffectiveTop_ClampedToMaxTop_WhenTopOmitted()
    {
        // No $top, no default → falls back to MaxTop so an omitted $top is still bounded.
        var m = Model("");
        new ODataQueryPolicy { MaxTop = 100 }.Apply(m);
        Assert.Equal(100, m.Top);
    }

    [Fact]
    public void ExplicitTop_Preserved_WhenWithinMax()
    {
        var m = Model("$top=10");
        new ODataQueryPolicy { MaxTop = 100, DefaultPageSize = 25 }.Apply(m);
        Assert.Equal(10, m.Top);
    }

    [Fact]
    public void MaxSkip_RejectsLargerSkip()
    {
        var p = new ODataQueryPolicy { MaxSkip = 1000 };
        Rejects(p, "$skip=1001");
        Allows(p, "$skip=1000");
    }

    // ── Allowed query options ─────────────────────────────────────────────────

    [Fact]
    public void DisallowedOptions_Rejected()
    {
        Rejects(new ODataQueryPolicy { AllowFilter = false }, "$filter=Age gt 1");
        Rejects(new ODataQueryPolicy { AllowOrderBy = false }, "$orderby=Age");
        Rejects(new ODataQueryPolicy { AllowSelect = false }, "$select=Name");
        Rejects(new ODataQueryPolicy { AllowCount = false }, "$count=true");
    }

    // ── Per-property allowlists ───────────────────────────────────────────────

    [Fact]
    public void FilterableProperties_Allowlist()
    {
        var p = new ODataQueryPolicy();
        p.FilterableProperties.Add("Country");
        Allows(p, "$filter=Country eq 'CA'");
        Assert.Contains("Age", Rejects(p, "$filter=Age gt 30").Message);
    }

    [Fact]
    public void SortableProperties_Allowlist()
    {
        var p = new ODataQueryPolicy();
        p.SortableProperties.Add("Name");
        Allows(p, "$orderby=Name desc");
        Rejects(p, "$orderby=Age");
    }

    [Fact]
    public void SelectableProperties_Allowlist()
    {
        var p = new ODataQueryPolicy();
        p.SelectableProperties.Add("Name");
        p.SelectableProperties.Add("Country");
        Allows(p, "$select=Name,Country");
        Rejects(p, "$select=Name,Email");
    }

    [Fact]
    public void Allowlists_MatchRootSegmentOfNestedPath()
    {
        var p = new ODataQueryPolicy();
        p.FilterableProperties.Add("Address");
        Allows(p, "$filter=Address/City eq 'Toronto'");
        Rejects(p, "$filter=Secret/Field eq 'x'");
    }

    // ── Filter-complexity limits ──────────────────────────────────────────────

    [Fact]
    public void MaxFilterNodeCount_RejectsComplexFilter()
    {
        var p = new ODataQueryPolicy { MaxFilterNodeCount = 3 };
        Allows(p, "$filter=Age gt 30");                       // 3 nodes: gt, Age, 30
        Rejects(p, "$filter=Age gt 30 and Country eq 'CA'");  // 7 nodes
    }

    [Fact]
    public void MaxOrderByNodeCount_RejectsTooManyTerms()
    {
        var p = new ODataQueryPolicy { MaxOrderByNodeCount = 1 };
        Allows(p, "$orderby=Age");
        Rejects(p, "$orderby=Age,Name");
    }

    [Fact]
    public void AllowedFunctions_RestrictsFunctions()
    {
        var p = new ODataQueryPolicy();
        p.AllowedFunctions.Add("startswith");
        Allows(p, "$filter=startswith(Name,'A')");
        Assert.Contains("contains", Rejects(p, "$filter=contains(Name,'A')").Message);
    }

    [Fact]
    public void AllowArithmetic_False_RejectsArithmetic()
    {
        var p = new ODataQueryPolicy { AllowArithmetic = false };
        Allows(p, "$filter=Age gt 30");
        Rejects(p, "$filter=Age add 1 gt 30");
    }

    // ── Permissive default + per-set clone ────────────────────────────────────

    [Fact]
    public void DefaultPolicy_IsPermissive()
    {
        var m = Model("$filter=contains(Name,'A')&$orderby=Age desc&$select=Name&$count=true&$top=9999&$skip=9999");
        new ODataQueryPolicy().Apply(m); // no throw
        Assert.Equal(9999, m.Top);
    }

    [Fact]
    public void CopyConstructor_ClonesAllSettings_Independently()
    {
        var baseline = new ODataQueryPolicy { MaxTop = 50 };
        baseline.FilterableProperties.Add("Country");

        var clone = new ODataQueryPolicy(baseline) { MaxTop = 10 };
        clone.FilterableProperties.Add("Age");

        Assert.Equal(50, baseline.MaxTop);
        Assert.Equal(10, clone.MaxTop);
        Assert.DoesNotContain("Age", baseline.FilterableProperties); // clone's set is independent
        Assert.Contains("Country", clone.FilterableProperties);      // base values copied in
    }
}
