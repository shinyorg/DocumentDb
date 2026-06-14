using Microsoft.EntityFrameworkCore;

namespace Shiny.DocumentDb.Benchmarks;

/// <summary>
/// EF Core context used as the benchmark contender. Reads go through pre-compiled,
/// no-tracking async queries to give EF Core its best showing ("a fighting chance").
/// </summary>
public class BenchDbContext : DbContext
{
    readonly string connectionString;

    public DbSet<EfUser> Users => Set<EfUser>();
    public DbSet<EfOrder> Orders => Set<EfOrder>();

    BenchDbContext(string connectionString) => this.connectionString = connectionString;

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseSqlite($"Data Source={connectionString}");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EfUser>(e =>
        {
            e.HasKey(u => u.Id);
            e.Property(u => u.Id).ValueGeneratedOnAdd();
        });

        modelBuilder.Entity<EfOrder>(e =>
        {
            e.HasKey(o => o.Id);
            e.Property(o => o.Id).ValueGeneratedOnAdd();
            e.OwnsOne(o => o.ShippingAddress);
            e.HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId).OnDelete(DeleteBehavior.Cascade);
            e.HasMany(o => o.Tags).WithOne().HasForeignKey(t => t.OrderId).OnDelete(DeleteBehavior.Cascade);
        });
        modelBuilder.Entity<EfOrderLine>().HasKey(l => l.Id);
        modelBuilder.Entity<EfOrderTag>().HasKey(t => t.Id);
    }

    public static BenchDbContext Create(string connectionString)
    {
        var ctx = new BenchDbContext(connectionString);
        ctx.Database.EnsureCreated();
        return ctx;
    }

    // --- Compiled async queries (flat) ---

    public static readonly Func<BenchDbContext, int, Task<EfUser?>> GetUserById =
        EF.CompileAsyncQuery((BenchDbContext ctx, int id) =>
            ctx.Users.AsNoTracking().FirstOrDefault(u => u.Id == id));

    public static readonly Func<BenchDbContext, IAsyncEnumerable<EfUser>> GetAllUsers =
        EF.CompileAsyncQuery((BenchDbContext ctx) => ctx.Users.AsNoTracking());

    public static readonly Func<BenchDbContext, string, IAsyncEnumerable<EfUser>> QueryUsersByName =
        EF.CompileAsyncQuery((BenchDbContext ctx, string name) =>
            ctx.Users.AsNoTracking().Where(u => u.Name == name));

    // --- Compiled async queries (nested order graph) ---

    public static readonly Func<BenchDbContext, int, Task<EfOrder?>> GetOrderById =
        EF.CompileAsyncQuery((BenchDbContext ctx, int id) =>
            ctx.Orders.AsNoTracking().Include(o => o.Lines).Include(o => o.Tags).FirstOrDefault(o => o.Id == id));

    public static readonly Func<BenchDbContext, IAsyncEnumerable<EfOrder>> GetAllOrders =
        EF.CompileAsyncQuery((BenchDbContext ctx) =>
            ctx.Orders.AsNoTracking().Include(o => o.Lines).Include(o => o.Tags));

    public static readonly Func<BenchDbContext, string, IAsyncEnumerable<EfOrder>> QueryOrdersByStatus =
        EF.CompileAsyncQuery((BenchDbContext ctx, string status) =>
            ctx.Orders.AsNoTracking().Include(o => o.Lines).Include(o => o.Tags).Where(o => o.Status == status));
}
