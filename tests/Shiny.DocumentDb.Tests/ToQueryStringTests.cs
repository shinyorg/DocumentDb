using System.Text.Json;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public abstract class ToQueryStringTestsBase : IDisposable
{
    protected readonly IDatabaseFixture Fixture;
    protected readonly IDocumentStore store;
    static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    protected ToQueryStringTestsBase(IDatabaseFixture fixture)
    {
        this.Fixture = fixture;
        this.store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    [Fact]
    public void Where_Linq_ProducesParameterizedSql()
    {
        var qs = this.store.Query(ctx.User)
            .Where(u => u.Age > 28)
            .ToQueryString();

        Assert.Contains("SELECT Data FROM", qs.Sql);
        Assert.Contains("TypeName = @typeName", qs.Sql);
        Assert.Contains("json_extract", qs.Sql);
        Assert.Contains("@p0", qs.Sql);

        Assert.Equal("User", qs.Parameters["@typeName"]);
        Assert.Equal(28, Convert.ToInt32(qs.Parameters["@p0"]));
    }

    [Fact]
    public void Where_StringExpression_ProducesSameSqlAsLinq()
    {
        var linq = this.store.Query(ctx.User).Where(u => u.Age > 28).ToQueryString();
        var str = this.store.Query(ctx.User).Where("Age > 28", ctx.User).ToQueryString();

        Assert.Equal(linq.Sql, str.Sql);
        Assert.Equal(linq.Parameters["@typeName"], str.Parameters["@typeName"]);
        Assert.Equal(Convert.ToInt32(linq.Parameters["@p0"]), Convert.ToInt32(str.Parameters["@p0"]));
    }

    [Fact]
    public void NoFilter_ProducesBareSelect()
    {
        var qs = this.store.Query(ctx.User).ToQueryString();

        Assert.Contains("TypeName = @typeName", qs.Sql);
        Assert.DoesNotContain("AND (", qs.Sql);
        Assert.Single(qs.Parameters);
        Assert.Equal("User", qs.Parameters["@typeName"]);
    }

    [Fact]
    public void OrderByAndPaginate_AreIncluded()
    {
        var qs = this.store.Query(ctx.User)
            .OrderByDescending(u => u.Age)
            .Paginate(10, 5)
            .ToQueryString();

        Assert.Contains("ORDER BY", qs.Sql);
        Assert.Contains("DESC", qs.Sql);
        // SQLite renders pagination as LIMIT/OFFSET.
        Assert.Contains("LIMIT", qs.Sql.ToUpperInvariant());
    }

    [Fact]
    public void ToString_RendersParametersAsCommentHeader()
    {
        var rendered = this.store.Query(ctx.User)
            .Where(u => u.Age > 28)
            .ToQueryString()
            .ToString();

        Assert.Contains("-- @typeName='User'", rendered);
        Assert.Contains("-- @p0=28", rendered);
        Assert.Contains("SELECT Data FROM", rendered);
        // The comment header precedes the SQL.
        Assert.True(rendered.IndexOf("-- @typeName", StringComparison.Ordinal) < rendered.IndexOf("SELECT", StringComparison.Ordinal));
    }

    [Fact]
    public void Select_Projection_ProducesSql()
    {
        var qs = this.store.Query(ctx.User)
            .Where(u => u.Age == 25)
            .Select(u => new UserSummary { Name = u.Name, Email = u.Email }, ctx.UserSummary)
            .ToQueryString();

        Assert.Contains("TypeName = @typeName", qs.Sql);
        Assert.Contains("json_extract", qs.Sql);
        Assert.Equal("User", qs.Parameters["@typeName"]);
    }

    [Fact]
    public void Project_StringFields_ProducesSql()
    {
        var qs = this.store.Query(ctx.User)
            .Project("name, age", ctx.User)
            .ToQueryString();

        Assert.Contains("TypeName = @typeName", qs.Sql);
        Assert.Contains("json_extract", qs.Sql);
        Assert.Equal("User", qs.Parameters["@typeName"]);
    }
}
