using System.Reflection;
using Microsoft.Data.Sqlite;

namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// Builds the demo instance's SQLite sample from SQL embedded in the image.
/// </summary>
/// <remarks>
/// <para>
/// This is what lets demo mode be one flag: the sample travels inside the image, so bringing a
/// playground up is running the container rather than orchestrating a seeding sidecar that has to
/// agree with it about paths and ordering.
/// </para>
/// <para>
/// <b>Written once.</b> If the file is already there it is left completely alone - not compared, not
/// topped up, not migrated. A visitor cannot ask for a rebuild because nothing offers one, and an
/// image update never silently discards a database someone is looking at. Changing the sample means
/// deleting the file deliberately.
/// </para>
/// </remarks>
public static class DemoDatabaseBuilder
{
    /// <summary>
    /// The embedded scripts, in load order: the sample creates the tables the volume inserts into.
    /// </summary>
    static readonly string[] Scripts =
    [
        "ShinyDocDbMyAdmin.Demo.seed-demo.sql",
        "ShinyDocDbMyAdmin.Demo.seed-demo-volume.sql"
    ];

    /// <summary>
    /// Creates the sample at <paramref name="databasePath"/> if it is not already there.
    /// </summary>
    /// <returns>True when a database was written, false when one already existed.</returns>
    public static bool EnsureCreated(string databasePath, ILogger logger)
    {
        if (File.Exists(databasePath))
        {
            logger.LogInformation("Demo database already present at {Path}; leaving it as it is.", databasePath);
            return false;
        }

        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);

        // Built beside the target and moved into place, so a failure part-way through cannot leave a
        // half-written database that the next start would then find and accept as complete.
        var staging = databasePath + ".building";
        DeleteQuietly(staging);

        try
        {
            using (var connection = new SqliteConnection($"Data Source={staging}"))
            {
                connection.Open();

                foreach (var name in Scripts)
                {
                    var sql = ReadResource(name);
                    using var command = connection.CreateCommand();
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                    logger.LogInformation("Applied {Script} ({Length:N0} characters).", name, sql.Length);
                }

                // The provider runs `PRAGMA journal_mode=WAL` on every connection, including the
                // read-only one this file is about to be served through. Against a database already
                // in WAL that is a no-op read; otherwise it must rewrite the header, which a
                // read-only open refuses with "attempt to write a readonly database".
                using (var pragma = connection.CreateCommand())
                {
                    pragma.CommandText = "PRAGMA journal_mode=WAL;";
                    pragma.ExecuteScalar();
                }

                // And a leftover -wal sidecar would fail the read-only open on its own.
                using (var checkpoint = connection.CreateCommand())
                {
                    checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
                    checkpoint.ExecuteNonQuery();
                }
            }

            SqliteConnection.ClearAllPools();
            DeleteQuietly(staging + "-wal");
            DeleteQuietly(staging + "-shm");

            File.Move(staging, databasePath);
            logger.LogInformation("Built the demo database at {Path}.", databasePath);
            return true;
        }
        catch
        {
            SqliteConnection.ClearAllPools();
            DeleteQuietly(staging);
            DeleteQuietly(staging + "-wal");
            DeleteQuietly(staging + "-shm");
            throw;
        }
    }

    static string ReadResource(string name)
    {
        var assembly = Assembly.GetExecutingAssembly();
        using var stream = assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException(
                $"'{name}' is not embedded in this build, so demo mode has nothing to seed from. " +
                $"Available: {string.Join(", ", assembly.GetManifestResourceNames())}");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    static void DeleteQuietly(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
            // Best effort: the caller is already either throwing or about to overwrite.
        }
    }
}
