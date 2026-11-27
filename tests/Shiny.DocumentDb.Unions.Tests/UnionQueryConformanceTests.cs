using System.Text.Json;
using System.Text.Json.Serialization;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Unions.Tests;

public record UCat(string Name, int Lives);
public record UDog(string Name, bool GoodBoy);

// Two object cases: System.Text.Json needs the structural classifier to tell them apart on read.
[JsonUnion(TypeClassifier = typeof(JsonUnionTypeStructuralClassifier))]
public union UPet(UCat, UDog);

// Cases of different JSON kinds: no classifier needed.
public union URef(int, string);

public class UOwner
{
    public string Id { get; set; } = "";
    public URef Ref { get; set; }
    public UPet Pet { get; set; }
}

/// <summary>C# union members in stored documents and in queries, asserted identically on every provider.</summary>
public abstract class UnionQueryConformanceTestsBase(IDocumentStoreFixture fixture)
{
    protected readonly IDocumentStoreFixture Fixture = fixture;

    async Task<IDocumentStore> SeededAsync()
    {
        var store = this.Fixture.CreateStore($"un{Guid.NewGuid():N}");
        await store.Insert(new UOwner { Id = "o1", Ref = 42, Pet = new UDog("Rex", true) });
        await store.Insert(new UOwner { Id = "o2", Ref = "ABC", Pet = new UCat("Tom", 9) });
        await store.Insert(new UOwner { Id = "o3", Ref = 7, Pet = new UDog("Fido", false) });
        await store.Insert(new UOwner { Id = "o4", Ref = "42", Pet = new UCat("Kit", 3) });
        return store;
    }

    static async Task<string[]> Ids(IDocumentQuery<UOwner> query)
        => (await query.ToList()).Select(x => x.Id).OrderBy(x => x).ToArray();

    [Fact]
    public async Task Unions_RoundTrip()
    {
        var store = await SeededAsync();
        var o1 = (await store.Get<UOwner>("o1"))!;
        var o2 = (await store.Get<UOwner>("o2"))!;

        Assert.Equal(42, Assert.IsType<int>(o1.Ref.Value));
        Assert.Equal("Rex", Assert.IsType<UDog>(o1.Pet.Value).Name);
        Assert.Equal("ABC", Assert.IsType<string>(o2.Ref.Value));
        Assert.Equal(9, Assert.IsType<UCat>(o2.Pet.Value).Lives);
    }

    [Fact]
    public async Task CastToACase_ComparesAsThatCase()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where(x => (int)x.Ref.Value! == 42)));
        Assert.Equal(["o4"], await Ids(store.Query<UOwner>().Where(x => (string)x.Ref.Value! == "42")));
        Assert.Equal(["o2"], await Ids(store.Query<UOwner>().Where(x => (string)x.Ref.Value! == "ABC")));
    }

    [Fact]
    public async Task CastToACase_OrdersAsThatCase()
    {
        var store = await SeededAsync();
        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where(x => (int)x.Ref.Value! > 10)));
    }

    [Fact]
    public async Task CaseTest_ScalarCases_ByJsonKind()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1", "o3"], await Ids(store.Query<UOwner>().Where(x => x.Ref.Value is int)));
        Assert.Equal(["o2", "o4"], await Ids(store.Query<UOwner>().Where(x => x.Ref.Value is string)));
        Assert.Equal(2, await store.Query<UOwner>().Where(x => !(x.Ref.Value is string)).Count());
    }

    [Fact]
    public async Task CaseTest_ObjectCases_ByADistinctProperty()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1", "o3"], await Ids(store.Query<UOwner>().Where(x => x.Pet.Value is UDog)));
        Assert.Equal(["o2", "o4"], await Ids(store.Query<UOwner>().Where(x => x.Pet.Value is UCat)));
    }

    [Fact]
    public async Task CaseMembers_ThroughACast()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where(x => x.Pet.Value is UDog && ((UDog)x.Pet.Value!).GoodBoy)));
        Assert.Equal(["o2"], await Ids(store.Query<UOwner>().Where(x => ((UCat)x.Pet.Value!).Lives > 5)));
        Assert.Equal(["o2", "o4"], await Ids(store.Query<UOwner>().Where(x => ((UCat)x.Pet.Value!).Name.StartsWith("T") || x.Pet.Value is UCat)));
    }

    [Fact]
    public async Task StringGrammar_ComparesTheCaseTheLiteralNames()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where("ref == 42")));
        Assert.Equal(["o4"], await Ids(store.Query<UOwner>().Where("ref == '42'")));
        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where("ref > 10")));
        Assert.Equal(["o1", "o3"], await Ids(store.Query<UOwner>().Where("ref in (7, 42)")));
    }

    [Fact]
    public async Task StringGrammar_Interpolated()
    {
        var store = await SeededAsync();
        var number = 42;
        var text = "ABC";

        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where($"ref == {number}")));
        Assert.Equal(["o2"], await Ids(store.Query<UOwner>().Where($"ref == {text}")));
    }

    [Fact]
    public async Task StringGrammar_CaseTests()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1", "o3"], await Ids(store.Query<UOwner>().Where("ref is int")));
        Assert.Equal(["o1", "o3"], await Ids(store.Query<UOwner>().Where("ref is not string")));
        Assert.Equal(["o1", "o3"], await Ids(store.Query<UOwner>().Where("pet is UDog")));
        Assert.Equal(["o2", "o4"], await Ids(store.Query<UOwner>().Where("not (pet is UDog)")));
    }

    [Fact]
    public async Task StringGrammar_CaseMembers()
    {
        var store = await SeededAsync();

        Assert.Equal(["o1"], await Ids(store.Query<UOwner>().Where("pet is UDog and pet.goodBoy == true")));
        Assert.Equal(["o2"], await Ids(store.Query<UOwner>().Where("pet.lives > 5")));
    }

    [Fact]
    public async Task StringGrammar_MatchesTheLinqForm()
    {
        var store = await SeededAsync();
        Assert.Equal(
            await Ids(store.Query<UOwner>().Where(x => x.Ref.Value is int && (int)x.Ref.Value! < 10 && x.Pet.Value is UDog)),
            await Ids(store.Query<UOwner>().Where("ref is int and ref < 10 and pet is UDog")));
    }

    [Fact]
    public async Task StringGrammar_UnknownCase_Throws()
    {
        var store = await SeededAsync();
        Assert.ThrowsAny<ArgumentException>(() => store.Query<UOwner>().Where("pet is Fish"));
        Assert.ThrowsAny<ArgumentException>(() => store.Query<UOwner>().Where("pet.fins == 2"));
        await Task.CompletedTask;
    }

    [Fact]
    public async Task CaseTests_CombineWithOtherPredicates()
    {
        var store = await SeededAsync();
        Assert.Equal(["o3"], await Ids(store.Query<UOwner>().Where(x => x.Ref.Value is int && x.Pet.Value is UDog && (int)x.Ref.Value! < 10)));
    }
}

public class UnionSupportUnitTests
{
    public record Alpha(string Name);
    public record Beta(string Name);

    [JsonUnion(TypeClassifier = typeof(JsonUnionTypeStructuralClassifier))]
    public union Lookalike(Alpha, Beta);

    public union Dates(DateTime, string);

    static readonly JsonSerializerOptions Options = new(JsonSerializerOptions.Default) { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    [Fact]
    public void Unions_AreRecognized()
    {
        Assert.True(Internal.UnionSupport.IsUnion(typeof(UPet)));
        Assert.True(Internal.UnionSupport.IsUnion(typeof(UPet?)));
        Assert.False(Internal.UnionSupport.IsUnion(typeof(UOwner)));
    }

    [Fact]
    public void ObjectCases_UseADistinctProperty()
    {
        var test = Internal.UnionSupport.CaseTest(typeof(UPet), typeof(UDog), Options);
        Assert.Equal(JsonValueKind.Object, test.Kind);
        Assert.Equal("goodBoy", test.DistinctProperty);
    }

    [Fact]
    public void ScalarCases_UseTheJsonKind()
    {
        var test = Internal.UnionSupport.CaseTest(typeof(URef), typeof(int), Options);
        Assert.Equal(JsonValueKind.Number, test.Kind);
        Assert.Null(test.DistinctProperty);
    }

    [Fact]
    public void IndistinguishableObjectCases_Throw()
        => Assert.Throws<NotSupportedException>(() => Internal.UnionSupport.CaseTest(typeof(Lookalike), typeof(Alpha), Options));

    [Fact]
    public void CasesOfTheSameJsonKind_Throw()
        => Assert.Throws<NotSupportedException>(() => Internal.UnionSupport.CaseTest(typeof(Dates), typeof(string), Options));
}
