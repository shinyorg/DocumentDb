using System.Text.Json;
using Shiny.DocumentDb.DuckDb;
using Shiny.DocumentDb.LiteDb;
using Shiny.DocumentDb.Sqlite;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

public class DuckDbCursorPaginationTests : CursorPaginationTestsBase
{
    protected override IDocumentStore CreateStore()
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new DuckDbDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
}

public class SqliteCursorPaginationTests : CursorPaginationTestsBase
{
    protected override IDocumentStore CreateStore()
        => new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            JsonSerializerOptions = ctx.Options,
            TableName = $"t{Guid.NewGuid():N}"
        });
}

public class LiteDbCursorPaginationTests : CursorPaginationTestsBase
{
    protected override IDocumentStore CreateStore()
        => new LiteDbDocumentStore(new LiteDbDocumentStoreOptions
        {
            ConnectionString = $"Filename={Path.GetTempFileName()};Connection=direct",
            CollectionName = $"t{Guid.NewGuid():N}"
        });
}

public abstract class CursorPaginationTestsBase : IDisposable
{
    protected static readonly TestJsonContext ctx = new(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
    readonly IDocumentStore store;

    protected CursorPaginationTestsBase() => this.store = this.CreateStore();

    protected abstract IDocumentStore CreateStore();

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    async Task SeedAsync(int count, Func<int, DateTimeOffset>? createdAt = null, Func<int, string>? status = null)
    {
        createdAt ??= i => new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(i);
        for (var i = 0; i < count; i++)
            await this.store.Insert(new Sale
            {
                Id = $"s{i:D3}",
                Status = status?.Invoke(i) ?? "open",
                Region = "west",
                Total = 100m + i,
                Quantity = i,
                CreatedAt = createdAt(i)
            }, ctx.Sale);
    }

    async Task<List<string>> WalkAsync(Func<IDocumentQuery<Sale>> queryFactory, int take)
    {
        var ids = new List<string>();
        string? cursor = null;
        var pages = 0;
        do
        {
            var page = await queryFactory().ToCursorPage(cursor, take);
            ids.AddRange(page.Items.Select(s => s.Id));
            cursor = page.NextCursor;
            if (++pages > 1000)
                throw new InvalidOperationException("Runaway cursor walk — did NextCursor never go null?");
        }
        while (cursor != null);
        return ids;
    }

    [Fact]
    public async Task ForwardWalk_DescendingDate_CoversAllNoDupes()
    {
        await this.SeedAsync(25);
        var expected = (await this.store.Query(ctx.Sale).OrderByDescending(s => s.CreatedAt).ToList())
            .Select(s => s.Id).ToList();

        var walked = await this.WalkAsync(() => this.store.Query(ctx.Sale).OrderByDescending(s => s.CreatedAt), take: 10);

        Assert.Equal(expected, walked);
        Assert.Equal(walked.Count, walked.Distinct().Count());
    }

    [Fact]
    public async Task SingleKey_Ascending_And_Descending()
    {
        await this.SeedAsync(13);

        var asc = await this.WalkAsync(() => this.store.Query(ctx.Sale).OrderBy(s => s.Total), take: 5);
        var desc = await this.WalkAsync(() => this.store.Query(ctx.Sale).OrderByDescending(s => s.Total), take: 5);

        var ascExpected = Enumerable.Range(0, 13).Select(i => $"s{i:D3}").ToList();
        Assert.Equal(ascExpected, asc);
        Assert.Equal(Enumerable.Reverse(ascExpected).ToList(), desc);
    }

    [Fact]
    public async Task MultiKey_StatusAsc_DateDesc()
    {
        await this.SeedAsync(20, status: i => i % 2 == 0 ? "a" : "b");

        var expected = (await this.store.Query(ctx.Sale)
                .OrderBy(s => s.Status)
                .OrderByDescending(s => s.CreatedAt)
                .ToList())
            .Select(s => s.Id).ToList();

        var walked = await this.WalkAsync(() => this.store.Query(ctx.Sale)
            .OrderBy(s => s.Status)
            .OrderByDescending(s => s.CreatedAt), take: 7);

        Assert.Equal(expected, walked);
    }

    [Fact]
    public async Task SameValueRun_TiebreakerNoDupeOrSkip()
    {
        // Every row shares one CreatedAt so the sort key is entirely non-unique; the Id tiebreaker must
        // carry the ordering across page boundaries that fall inside the run.
        var fixed_ = new DateTimeOffset(2026, 5, 1, 12, 0, 0, TimeSpan.Zero);
        await this.SeedAsync(30, createdAt: _ => fixed_);

        var walked = await this.WalkAsync(() => this.store.Query(ctx.Sale).OrderByDescending(s => s.CreatedAt), take: 7);

        var expected = Enumerable.Range(0, 30).Select(i => $"s{i:D3}").ToList(); // Id asc tiebreaker
        Assert.Equal(expected, walked);
    }

    [Fact]
    public async Task NoOrderBy_OrdersById()
    {
        await this.SeedAsync(15);

        var walked = await this.WalkAsync(() => this.store.Query(ctx.Sale), take: 6);

        Assert.Equal(Enumerable.Range(0, 15).Select(i => $"s{i:D3}").ToList(), walked);
    }

    [Fact]
    public async Task ToCursorStream_YieldsFullSet()
    {
        await this.SeedAsync(23);

        var streamed = new List<string>();
        await foreach (var s in this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorStream(pageSize: 5))
            streamed.Add(s.Id);

        Assert.Equal(Enumerable.Range(0, 23).Select(i => $"s{i:D3}").ToList(), streamed);
    }

    [Fact]
    public async Task LastPage_HasNullCursor_And_HasMoreFalse()
    {
        await this.SeedAsync(10);
        var page1 = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(null, 4);
        Assert.True(page1.HasMore);
        Assert.NotNull(page1.NextCursor);

        var page2 = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(page1.NextCursor, 4);
        var page3 = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(page2.NextCursor, 4);
        Assert.Equal(2, page3.Items.Count);
        Assert.Null(page3.NextCursor);
        Assert.False(page3.HasMore);
    }

    [Fact]
    public async Task NullInOrderedColumn_WalksEntireSetNoDrops()
    {
        // Regression: a NULL in an ordered column made the keyset seek predicate (col > @v / col = @v with
        // @v bound to NULL) evaluate to UNKNOWN in SQL, so once a page boundary landed on a NULL row the
        // predicate matched zero rows and every later row was silently dropped. The walk must cover the whole
        // set (no drops) and never duplicate, in both directions, with NULLs pinned last.
        for (var i = 0; i < 12; i++)
            await this.store.Insert(new Sale
            {
                Id = $"s{i:D3}",
                Status = i % 3 == 0 ? null! : $"st{i % 4}",   // ~1/3 of rows have a JSON-null status
                Region = "west",
                Total = 100m + i,
                Quantity = i,
                CreatedAt = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(i)
            }, ctx.Sale);

        var all = Enumerable.Range(0, 12).Select(i => $"s{i:D3}").OrderBy(x => x, StringComparer.Ordinal).ToList();

        var asc = await this.WalkAsync(() => this.store.Query(ctx.Sale).OrderBy(s => s.Status), take: 5);
        Assert.Equal(all, asc.OrderBy(x => x, StringComparer.Ordinal).ToList());   // nothing dropped
        Assert.Equal(asc.Count, asc.Distinct().Count());                           // nothing duplicated

        var desc = await this.WalkAsync(() => this.store.Query(ctx.Sale).OrderByDescending(s => s.Status), take: 5);
        Assert.Equal(all, desc.OrderBy(x => x, StringComparer.Ordinal).ToList());
        Assert.Equal(desc.Count, desc.Distinct().Count());
    }

    [Fact]
    public async Task EmptyResultSet_FirstPageHasNoCursor()
    {
        var page = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(null, 5);
        Assert.Empty(page.Items);
        Assert.Null(page.NextCursor);
        Assert.False(page.HasMore);
    }

    [Fact]
    public async Task StableUnderConcurrentInsertBeforeCursor()
    {
        await this.SeedAsync(10, createdAt: i => new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(i * 10));

        var page1 = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(null, 4);
        var firstFour = page1.Items.Select(s => s.Id).ToList();

        // Insert a row that sorts BEFORE the current cursor position — offset paging would shift and dup a
        // row; keyset is anchored to the last row's key so it is unaffected.
        await this.store.Insert(new Sale
        {
            Id = "s000-early",
            CreatedAt = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero)
        }, ctx.Sale);

        var rest = new List<string>();
        string? cursor = page1.NextCursor;
        while (cursor != null)
        {
            var p = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(cursor, 4);
            rest.AddRange(p.Items.Select(s => s.Id));
            cursor = p.NextCursor;
        }

        var all = firstFour.Concat(rest).ToList();
        Assert.Equal(all.Count, all.Distinct().Count());        // no dupes despite the concurrent insert
        Assert.DoesNotContain("s000-early", all);               // it sorted before the anchor, so not re-surfaced
    }

    [Fact]
    public async Task Cursor_FromDifferentOrdering_Throws()
    {
        await this.SeedAsync(10);
        var page = await this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(null, 4);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            this.store.Query(ctx.Sale).OrderByDescending(s => s.Total).ToCursorPage(page.NextCursor, 4));
    }

    [Fact]
    public async Task Cursor_Corrupt_Throws()
    {
        await this.SeedAsync(5);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage("not-a-valid-cursor!!", 4));
    }

    [Fact]
    public async Task AfterSelect_Throws_NotSupported()
    {
        await this.SeedAsync(5);
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            this.store.Query(ctx.Sale)
                .Select(s => new StatusRollup { Status = s.Status }, ctx.StatusRollup)
                .ToCursorPage(null, 4));
    }

    [Fact]
    public async Task AfterProject_Throws_NotSupported()
    {
        await this.SeedAsync(5);
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            this.store.Query(ctx.Sale).Project("status").ToCursorPage(null, 4));
    }

    [Fact]
    public async Task TakeZeroOrNegative_Throws()
    {
        await this.SeedAsync(3);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(null, 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            this.store.Query(ctx.Sale).OrderBy(s => s.CreatedAt).ToCursorPage(null, -5));
    }

    [Fact]
    public async Task Filter_And_Cursor_Compose()
    {
        await this.SeedAsync(20, status: i => i % 2 == 0 ? "open" : "closed");

        var walked = new List<string>();
        string? cursor = null;
        do
        {
            var page = await this.store.Query(ctx.Sale)
                .Where(s => s.Status == "open")
                .OrderBy(s => s.CreatedAt)
                .ToCursorPage(cursor, 3);
            walked.AddRange(page.Items.Select(s => s.Id));
            cursor = page.NextCursor;
        }
        while (cursor != null);

        var expected = Enumerable.Range(0, 20).Where(i => i % 2 == 0).Select(i => $"s{i:D3}").ToList();
        Assert.Equal(expected, walked);
    }

    [Fact]
    public async Task GuidId_Tiebreaker_Walks()
    {
        for (var i = 0; i < 15; i++)
            await this.store.Insert(new GuidIdModel { Id = Guid.NewGuid(), Name = "same" }, ctx.GuidIdModel);

        // All share Name; ordering falls entirely to the Guid Id tiebreaker.
        var walked = new List<Guid>();
        string? cursor = null;
        do
        {
            var page = await this.store.Query(ctx.GuidIdModel).OrderBy(m => m.Name).ToCursorPage(cursor, 4);
            walked.AddRange(page.Items.Select(m => m.Id));
            cursor = page.NextCursor;
        }
        while (cursor != null);

        Assert.Equal(15, walked.Count);
        Assert.Equal(15, walked.Distinct().Count());
    }
}
