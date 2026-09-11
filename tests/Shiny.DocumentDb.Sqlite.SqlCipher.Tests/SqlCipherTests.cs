using System.Text;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Sqlite.SqlCipher.Tests;

public class SqlCipherTests : IDisposable
{
    const string Password = "s3cret-key";
    const string PlainHeader = "SQLite format 3";

    readonly List<string> files = new();

    public class Person
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    string NewPath()
    {
        var path = Path.Combine(Path.GetTempPath(), $"cipher_{Guid.NewGuid():N}.db");
        this.files.Add(path);
        return path;
    }

    static bool IsPlaintext(string path)
    {
        var bytes = File.ReadAllBytes(path);
        return bytes.Length >= PlainHeader.Length
            && Encoding.ASCII.GetString(bytes, 0, PlainHeader.Length) == PlainHeader;
    }

    static async Task Seed(string path, string password)
    {
        using var store = new SqlCipherDocumentStore(path, password);
        await store.Insert(new Person { Id = "p1", Name = "Alice", Age = 30 });
        await store.Insert(new Person { Id = "p2", Name = "Bob", Age = 17 });
    }

    [Fact]
    public void Engine_IsSqlCipher()
    {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        Assert.Equal("e_sqlcipher", SQLitePCL.raw.GetNativeLibraryName());

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA cipher_version;";
        Assert.False(String.IsNullOrWhiteSpace(cmd.ExecuteScalar() as string));
    }

    [Fact]
    public async Task Store_RoundTrips_AndIsEncryptedAtRest()
    {
        var path = NewPath();
        await Seed(path, Password);

        using (var store = new SqlCipherDocumentStore(path, Password))
        {
            var alice = await store.Get<Person>("p1");
            Assert.NotNull(alice);
            Assert.Equal("Alice", alice.Name);

            var adults = await store.Query<Person>().Where(p => p.Age >= 18).ToList();
            Assert.Single(adults);
            Assert.Equal("p1", adults[0].Id);
        }

        SqliteConnection.ClearAllPools();
        Assert.False(IsPlaintext(path));
    }

    [Fact]
    public async Task WrongPassword_CannotRead()
    {
        var path = NewPath();
        await Seed(path, Password);
        SqliteConnection.ClearAllPools();

        using var store = new SqlCipherDocumentStore(path, "not-the-key");
        await Assert.ThrowsAsync<SqliteException>(() => store.Get<Person>("p1"));
    }

    [Fact]
    public async Task Rekey_SwitchesTheKey()
    {
        var path = NewPath();
        await Seed(path, Password);

        using (var store = new SqlCipherDocumentStore(path, Password))
            await store.RekeyAsync("new-key");

        SqliteConnection.ClearAllPools();

        using (var rekeyed = new SqlCipherDocumentStore(path, "new-key"))
            Assert.Equal("Alice", (await rekeyed.Get<Person>("p1"))!.Name);

        using var old = new SqlCipherDocumentStore(path, Password);
        await Assert.ThrowsAsync<SqliteException>(() => old.Get<Person>("p1"));
    }

    [Fact]
    public async Task Backup_IsEncryptedWithTheSameKey()
    {
        var path = NewPath();
        var backup = NewPath();
        await Seed(path, Password);

        using (var store = new SqlCipherDocumentStore(path, Password))
            await store.Backup(backup);

        SqliteConnection.ClearAllPools();
        Assert.False(IsPlaintext(backup));

        using var restored = new SqlCipherDocumentStore(backup, Password);
        Assert.Equal("Bob", (await restored.Get<Person>("p2"))!.Name);
    }

    [Fact]
    public async Task PlainSqliteProvider_StillWritesUnencryptedFiles()
    {
        var path = NewPath();

        using (var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={path}")
        }))
        {
            await store.Insert(new Person { Id = "p1", Name = "Alice", Age = 30 });
            Assert.Equal("Alice", (await store.Get<Person>("p1"))!.Name);
        }

        SqliteConnection.ClearAllPools();
        Assert.True(IsPlaintext(path));
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in this.files.Where(File.Exists))
            File.Delete(file);
    }
}
