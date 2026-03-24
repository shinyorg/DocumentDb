using Microsoft.EntityFrameworkCore;

namespace Shiny.DocumentDb.Benchmarks.Providers;

public class BenchmarkDbContext : DbContext
{
    readonly string connectionString;
    readonly string providerName;

    public DbSet<EfUser> Users => Set<EfUser>();

    BenchmarkDbContext(string connectionString, string providerName)
    {
        this.connectionString = connectionString;
        this.providerName = providerName;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        switch (providerName)
        {
            case "SQLite":
                options.UseSqlite(connectionString);
                break;
            case "PostgreSQL":
                options.UseNpgsql(connectionString);
                break;
            case "SqlServer":
                options.UseSqlServer(connectionString);
                break;
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EfUser>(e =>
        {
            e.HasKey(u => u.Id);
            e.Property(u => u.Id).ValueGeneratedOnAdd();
            e.Property(u => u.Name).HasMaxLength(200);
            e.Property(u => u.Email).HasMaxLength(200);
        });
    }

    public static BenchmarkDbContext Create(string provider, string connectionString)
    {
        var ctx = new BenchmarkDbContext(connectionString, provider);
        ctx.Database.EnsureCreated();
        return ctx;
    }
}
