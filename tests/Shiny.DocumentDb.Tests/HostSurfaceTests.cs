using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Shiny.DocumentDb.Hosting;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The public host surface (<c>Shiny.DocumentDb.Hosting</c>) — what a package hosting DocumentDb over a
/// transport is allowed to depend on, in place of an <c>InternalsVisibleTo</c> grant. These tests exist for the
/// contract, not the mechanics: the interpreter's node coverage lives in <see cref="InterpreterTests"/>, and the
/// grammar's in <c>WhereStringTests</c>.
/// </summary>
[Collection("SQLite")]
public class HostSurfaceTests(SqliteDatabaseFixture fx) : IDisposable
{
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    readonly IDocumentStore store = fx.CreateStore($"t{Guid.NewGuid():N}", o => o.SerializerOptions = ctx.Options);

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync()
    {
        await this.store.Insert(new User { Id = "u1", Name = "Alice", Age = 25, Email = "alice@test.com" }, ctx.User);
        await this.store.Insert(new User { Id = "u2", Name = "Bob", Age = 35 }, ctx.User);
        await this.store.Insert(new User { Id = "u3", Name = "Charlie", Age = 30, Email = "c@test.com" }, ctx.User);
    }

    static string[] Ids(IEnumerable<User> users) => users.Select(u => u.Id).OrderBy(x => x).ToArray();

    public static TheoryData<string, Expression<Func<User, bool>>> Predicates => new()
    {
        { "numeric", x => x.Age > 28 },
        { "equality", x => x.Name == "Alice" },
        { "conjunction", x => x.Age >= 25 && x.Name != "Bob" },
        { "disjunction", x => x.Age < 26 || x.Name == "Charlie" },
        { "negation", x => !(x.Age == 35) },
        { "null-check", x => x.Email == null },
        { "string-fn", x => x.Name.StartsWith("A") },
        { "case-fold", x => x.Name.ToLower() == "bob" },
        { "arithmetic", x => x.Age + 5 > 35 },
        { "deny-all", x => false }
    };

    /// <summary>
    /// The whole reason a host compiles a predicate rather than only pushing it down: the two enforcement paths
    /// have to agree. A scope that the query filters on but the in-memory check waves through is a scope bypass,
    /// so this equivalence is the contract — not an implementation detail of either side.
    /// </summary>
    [Theory]
    [MemberData(nameof(Predicates))]
    public async Task Compile_AgreesWithWhere(string _, Expression<Func<User, bool>> predicate)
    {
        await this.SeedAsync();

        var pushedDown = await this.store.Query(ctx.User).Where(predicate).ToList();
        var all = await this.store.Query(ctx.User).ToList();
        var inMemory = all.Where(DocumentPredicate.Compile(predicate));

        Assert.Equal(Ids(pushedDown), Ids(inMemory));
    }

    [Fact]
    public void Compile_RejectsNull()
        => Assert.Throws<ArgumentNullException>(() => DocumentPredicate.Compile<User>(null!));

    // ── DocumentFilter ──────────────────────────────────────────────────

    [Fact]
    public async Task Parse_AgreesWithStringWhere()
    {
        await this.SeedAsync();
        const string filter = "Age > 28 and Name != 'Bob'";

        var pushedDown = await this.store.Query(ctx.User).Where(filter, ctx.User).ToList();
        var all = await this.store.Query(ctx.User).ToList();
        var inMemory = all.Where(DocumentPredicate.Compile(DocumentFilter.Parse(filter, ctx.User)));

        Assert.Equal(["u3"], Ids(pushedDown));
        Assert.Equal(Ids(pushedDown), Ids(inMemory));
    }

    /// <summary>
    /// A computed property is <c>[JsonIgnore]</c>'d and never stored, so a host has to be able to filter on one
    /// without knowing that. The parsed expression reads the CLR property — which the query pushes down to the
    /// materialized column, and which the store populates on read-back — so both sides of a host's enforcement
    /// see the same value.
    /// </summary>
    [Fact]
    public async Task Parse_ResolvesComputedProperties()
    {
        var computed = fx.CreateStore($"t{Guid.NewGuid():N}", o =>
        {
            o.SerializerOptions = ctx.Options;
            o.ConfigureDocument<ComputedSale>(cfg => cfg.MapComputedProperty<string>(s => s.FullName, s => s.First + " " + s.Last));
        });
        using var _ = computed as IDisposable;
        await computed.Insert(new ComputedSale { Id = "s1", First = "Jane", Last = "Doe" }, ctx.ComputedSale);
        await computed.Insert(new ComputedSale { Id = "s2", First = "John", Last = "Roe" }, ctx.ComputedSale);

        var predicate = DocumentFilter.Parse("FullName == 'Jane Doe'", ctx.ComputedSale);
        var pushedDown = await computed.Query(ctx.ComputedSale).Where(predicate).ToList();

        Assert.Equal(["s1"], pushedDown.Select(s => s.Id));
        Assert.True(DocumentPredicate.Compile(predicate)(pushedDown[0]));
    }

    [Fact]
    public void ParseJson_SchemaFree_TakesTypeHints()
    {
        var predicate = DocumentPredicate.Compile(DocumentFilter.ParseJson("weight:number > 5"));

        Assert.True(predicate(JsonNode.Parse("""{"weight":10}""")!.AsObject()));
        Assert.False(predicate(JsonNode.Parse("""{"weight":1}""")!.AsObject()));
    }

    [Fact]
    public void ParseJson_WithTypeInfo_ResolvesJsonNames()
    {
        var predicate = DocumentPredicate.Compile(DocumentFilter.ParseJson("Age > 28", ctx.User));

        Assert.True(predicate(JsonNode.Parse("""{"age":35}""")!.AsObject()));
        Assert.False(predicate(JsonNode.Parse("""{"age":25}""")!.AsObject()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_RejectsEmptyFilter(string filter)
    {
        Assert.ThrowsAny<ArgumentException>(() => DocumentFilter.Parse(filter, ctx.User));
        Assert.ThrowsAny<ArgumentException>(() => DocumentFilter.ParseJson(filter));
    }

    // ── DocumentStoreAccessor ───────────────────────────────────────────

    [Fact]
    public void GetVersionMapping_ReadsTheMappedProperty()
    {
        var versioned = fx.CreateStore($"t{Guid.NewGuid():N}", o =>
            o.ConfigureDocument<VersionedUser>(cfg => cfg.MapVersionProperty(x => x.Version)));
        using var _ = versioned as IDisposable;

        var mapping = versioned.GetVersionMapping(typeof(VersionedUser));

        Assert.NotNull(mapping);
        Assert.Equal(nameof(VersionedUser.Version), mapping.PropertyName);
        Assert.Equal(7, mapping.GetVersion(new VersionedUser { Version = 7 }));
    }

    [Fact]
    public void GetVersionMapping_IsNullForAnUnmappedType()
    {
        Assert.NotNull(this.store.GetMappings());
        Assert.Null(this.store.GetVersionMapping(typeof(User)));
    }

    [Fact]
    public void Accessors_RejectNull()
    {
        Assert.Throws<ArgumentNullException>(() => ((IDocumentStore)null!).GetMappings());
        Assert.Throws<ArgumentNullException>(() => this.store.GetVersionMapping(null!));
    }
}

/// <summary>
/// The accessors have to work on the non-relational providers too — those do not implement the relational
/// query executor, so they reach their registry through <see cref="DocumentProviderBase"/> instead. LiteDB
/// stands in for that whole family here because it needs no container.
/// </summary>
[Collection("LiteDB")]
public class HostSurfaceProviderTests(LiteDbDatabaseFixture fx)
{
    [Fact]
    public void GetMappings_WorksOnANonRelationalProvider()
    {
        var store = fx.CreateStore($"t{Guid.NewGuid():N}", o =>
            o.ConfigureDocument<VersionedUser>(cfg => cfg.MapVersionProperty(x => x.Version)));
        using var _ = store as IDisposable;

        Assert.NotNull(store.GetMappings());
        Assert.Equal(nameof(VersionedUser.Version), store.GetVersionMapping(typeof(VersionedUser))?.PropertyName);
        Assert.Null(store.GetVersionMapping(typeof(User)));
    }
}

/// <summary>
/// Pins the shape of the host surface. Everything here is load-bearing for packages built outside this repo, so
/// a rename or a signature change has to be a deliberate breaking change with a release note — not something a
/// refactor does quietly and discovers at a downstream runtime.
/// </summary>
public class HostSurfaceApiTests
{
    static IEnumerable<Type> HostTypes => typeof(IDocumentStore).Assembly
        .GetExportedTypes()
        .Where(t => t.Namespace == "Shiny.DocumentDb.Hosting");

    [Fact]
    public void TheNamespaceContainsExactlyTheseTypes()
        => Assert.Equal(
            ["DocumentFilter", "DocumentPredicate", "DocumentStoreAccessor"],
            HostTypes.Select(t => t.Name).OrderBy(x => x, StringComparer.Ordinal));

    [Fact]
    public void TheSignaturesAreUnchanged()
        => Assert.Equal(
        [
            "System.Linq.Expressions.Expression`1[System.Func`2[T,System.Boolean]] Parse[T](System.String, System.Text.Json.Serialization.Metadata.JsonTypeInfo`1[T])",
            "System.Linq.Expressions.Expression`1[System.Func`2[System.Text.Json.Nodes.JsonObject,System.Boolean]] ParseJson(System.String, System.Text.Json.Serialization.Metadata.JsonTypeInfo)",
            "System.Func`2[T,System.Boolean] Compile[T](System.Linq.Expressions.Expression`1[System.Func`2[T,System.Boolean]])",
            "DocumentMappingRegistry GetMappings(IDocumentStore)",
            "VersionMapping GetVersionMapping(IDocumentStore, System.Type)"
        ],
        HostTypes
            .OrderBy(t => t.Name, StringComparer.Ordinal)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .Select(m => $"{Name(m.ReturnType)} {m.Name}{(m.IsGenericMethodDefinition ? $"[{string.Join(", ", m.GetGenericArguments().Select(a => a.Name))}]" : "")}" +
                         $"({string.Join(", ", m.GetParameters().Select(p => Name(p.ParameterType)))})"));

    // Full names for everything outside the assembly under test, short names for our own — the point is to catch
    // a change to what a host writes, and a host writes the short name.
    static string Name(Type t)
        => t.Assembly == typeof(IDocumentStore).Assembly || t.IsGenericParameter ? t.Name : t.ToString();
}
