using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Pulling field references out of a filter expression, which is what lets the console say "this query
/// touches <c>customer.name</c> and nothing indexes it".
/// </summary>
/// <remarks>
/// A false positive here costs a suggestion the user declines; a false negative costs a suggestion they
/// never see. Neither writes anything — creating the index is a separate, explicit click — so these
/// tests pin the shape rather than demanding the precision of a real parser.
/// </remarks>
public class FilterPathsTests
{
    [Fact]
    public void ASimpleComparison_YieldsItsField()
        => Assert.Equal(["status"], FilterPaths.Extract("status == 'Shipped'"));

    [Fact]
    public void NestedPaths_AreKeptWhole()
        => Assert.Equal(["customer.name"], FilterPaths.Extract("customer.name == 'bob'"));

    [Fact]
    public void TypeHints_AreStrippedFromThePath()
        => Assert.Equal(["total"], FilterPaths.Extract("total:number > 100"));

    [Fact]
    public void CombinedConditions_YieldEveryField()
        => Assert.Equal(
            ["status", "total", "customer.tier"],
            FilterPaths.Extract("status == 'Shipped' and total:number > 100 and customer.tier == 'gold'"));

    [Fact]
    public void StringLiteralsAreNotFields()
    {
        // 'Shipped' looks exactly like an identifier once the quotes are gone.
        var paths = FilterPaths.Extract("status == 'Shipped'");

        Assert.DoesNotContain("Shipped", paths);
    }

    [Fact]
    public void LiteralsContainingSpaces_DoNotLeakWords()
    {
        var paths = FilterPaths.Extract("note == 'urgent and overdue'");

        Assert.Equal(["note"], paths);
    }

    [Fact]
    public void GrammarKeywords_AreNotFields()
    {
        var paths = FilterPaths.Extract("a == 1 and b == 2 or not c == 3");

        Assert.Equal(["a", "b", "c"], paths);
    }

    [Fact]
    public void FunctionNames_AreNotFields_ButTheirArgumentsAre()
    {
        var paths = FilterPaths.Extract("lower(email) == 'bob@x.com'");

        Assert.Contains("email", paths);
        Assert.DoesNotContain("lower", paths);
    }

    [Fact]
    public void ADottedPathStartingWithAKeywordWord_IsStillAField()
    {
        // "date" is a type hint and a function name, but "date.created" can only be a path.
        Assert.Equal(["date.created"], FilterPaths.Extract("date.created > '2026-01-01'"));
    }

    [Fact]
    public void OrderingClauses_YieldTheirKeys()
        => Assert.Equal(["total", "name"], FilterPaths.Extract("total:number desc, name asc"));

    [Fact]
    public void NumbersAreNotFields()
        => Assert.Equal(["total"], FilterPaths.Extract("total > 100.5"));

    [Fact]
    public void RepeatedFields_AppearOnce_InFirstSeenOrder()
        => Assert.Equal(["b", "a"], FilterPaths.Extract("b == 1 and a == 2 and b == 3"));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void NothingToRead_IsAnEmptyList(string? expression)
        => Assert.Empty(FilterPaths.Extract(expression));
}

/// <summary>Index naming, which has to match the library's byte for byte or it builds a duplicate.</summary>
public class IndexNamingTests
{
    [Fact]
    public void SinglePath_MatchesTheLibraryConvention()
        => Assert.Equal("idx_json_Order_status", DocumentAdminService.BuildIndexName("Order", "status"));

    [Fact]
    public void DottedPath_HasItsDotsFlattened()
        => Assert.Equal("idx_json_Order_customer_name", DocumentAdminService.BuildIndexName("Order", "customer.name"));

    [Fact]
    public void DottedTypeName_HasItsDotsFlattened()
        => Assert.Equal("idx_json_MyApp_Order_status", DocumentAdminService.BuildIndexName("MyApp.Order", "status"));

    [Fact]
    public void CompositePaths_AreJoinedWithADoubleUnderscore()
        => Assert.Equal(
            "idx_json_Order_status__total",
            DocumentAdminService.BuildIndexName("Order", ["status", "total"]));

    [Fact]
    public void CompositeWithDottedPaths_FlattensEachThenJoins()
        => Assert.Equal(
            "idx_json_Order_customer_tier__total",
            DocumentAdminService.BuildIndexName("Order", ["customer.tier", "total"]));

    [Fact]
    public void NoPaths_IsRejected()
        => Assert.Throws<ArgumentException>(() => DocumentAdminService.BuildIndexName("Order", []));
}
