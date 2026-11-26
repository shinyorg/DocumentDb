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
    public void Engine_IsSqlite3MultipleCiphers()
    {
        using var conn = new SqliteConnection("Data Source=:memory:");
        conn.Open();
        Assert.Equal(SqlCipherEngine.NativeLibraryName, SQLitePCL.raw.GetNativeLibraryName());

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sqlite3mc_version();";
        Assert.False(String.IsNullOrWhiteSpace(cmd.ExecuteScalar() as string));
    }

    // e_sqlite3's batteries_v2 outranks SQLite3MC's in the build output, so a later Batteries_V2.Init()
    // (Microsoft.Data.Sqlite's, SqliteVec's) would install an engine without encryption if not frozen
    [Fact]
    public void Engine_IsFrozen_AgainstBatteriesInit()
    {
        SQLitePCL.Batteries_V2.Init();
        SqlCipherEngine.Init();
        Assert.Equal(SqlCipherEngine.NativeLibraryName, SQLitePCL.raw.GetNativeLibraryName());
    }

    // a database written by the old e_sqlcipher engine (SQLCipher 4.5.2) opens, reads and takes writes
    [Fact]
    public async Task LegacySqlCipherDatabase_StillOpens()
    {
        var path = NewPath();
        File.Copy(Path.Combine(AppContext.BaseDirectory, "Fixtures", "legacy-sqlcipher4.db"), path);

        using (var store = new SqlCipherDocumentStore(path, "legacy-key"))
        {
            Assert.Equal("Alice", (await store.Get<Person>("p1"))!.Name);
            var minors = await store.Query<Person>().Where(p => p.Age < 18).ToList();
            Assert.Equal("p2", Assert.Single(minors).Id);

            await store.Insert(new Person { Id = "p3", Name = "Carol", Age = 41 });
        }

        SqliteConnection.ClearAllPools();

        using var reopened = new SqlCipherDocumentStore(path, "legacy-key");
        Assert.Equal(3, (await reopened.Query<Person>().ToList()).Count);
    }

    // new files are SQLCipher 4, not SQLite3MC's ChaCha20 default, so SQLCipher tooling can still open them
    [Fact]
    public async Task NewDatabase_UsesTheSqlCipher4Format()
    {
        var path = NewPath();
        await Seed(path, Password);
        SqliteConnection.ClearAllPools();

        static long Count(string connectionString)
        {
            using var conn = new SqliteConnection(connectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT count(*) FROM sqlite_master;";
            return (long)cmd.ExecuteScalar()!;
        }

        var sqlCipher4 = new SqliteConnectionStringBuilder { DataSource = $"file:{path}?cipher=sqlcipher&legacy=4", Password = Password, Pooling = false };
        Assert.True(Count(sqlCipher4.ToString()) > 0);

        var chacha = new SqliteConnectionStringBuilder { DataSource = $"file:{path}?cipher=chacha20", Password = Password, Pooling = false };
        Assert.Throws<SqliteException>(() => Count(chacha.ToString()));
    }

    // the path rides in a SQLite URI, where '%', '#' and '?' are reserved and must be escaped
    [Fact]
    public async Task PathWithUriReservedCharacters_RoundTrips()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"cipher dir #1 100% {Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "my data.db");
        try
        {
            await Seed(path, Password);
            using (var store = new SqlCipherDocumentStore(path, Password))
                Assert.Equal("Bob", (await store.Get<Person>("p2"))!.Name);

            SqliteConnection.ClearAllPools();
            Assert.True(File.Exists(path));
            Assert.False(IsPlaintext(path));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(dir, true);
        }
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

    // the read lane opens its own connections from the provider's connection string, which carries the key
    [Fact]
    public async Task ConcurrentReads_read_the_encrypted_database_beside_an_open_transaction()
    {
        var path = NewPath();
        using var store = new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqlCipherDatabaseProvider(path, Password) { ConcurrentReads = true }
        });
        await store.Insert(new Person { Id = "p1", Name = "Alice", Age = 30 });

        var session = store.OpenSession();
        var tx = await session.BeginTransaction();
        session.Update(new Person { Id = "p1", Name = "Changed", Age = 30 });
        await session.SaveChanges();
        try
        {
            // a read on the writer would wait for the transaction; a read-lane read returns the committed row
            var get = store.Get<Person>("p1");
            var finished = await Task.WhenAny(get, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.Same(get, finished);
            Assert.Equal("Alice", (await get)!.Name);
            Assert.Single(await store.Query<Person>().ToList());
        }
        finally
        {
            await tx.Commit();
            await session.DisposeAsync();
        }

        Assert.Equal("Changed", (await store.Get<Person>("p1"))!.Name);
        SqliteConnection.ClearAllPools();
        Assert.False(IsPlaintext(path));
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in this.files.Where(File.Exists))
            File.Delete(file);
    }
}
