using SQLitePCL;

namespace Shiny.DocumentDb.Sqlite.SqlCipher;

/// <summary>
/// Selects SQLite3 Multiple Ciphers as the process-wide SQLite engine and locks it in.
/// </summary>
/// <remarks>
/// <para>
/// SQLitePCLRaw has one engine per process. An app that references both
/// <c>Shiny.DocumentDb.Sqlite</c> and this package carries two <c>SQLitePCLRaw.batteries_v2</c>
/// assemblies (e_sqlite3's and SQLite3MC's) and the build keeps whichever has the higher version -
/// e_sqlite3's - so <c>Batteries_V2.Init()</c> alone would pick an engine without encryption. This
/// sets the SQLite3MC provider explicitly and freezes it, so later <c>Batteries_V2.Init()</c> calls
/// (Microsoft.Data.Sqlite's own, or <c>SqliteVec</c>'s) cannot swap it back.
/// </para>
/// <para>
/// SQLite3MC opens unencrypted databases as well, so plain SQLite stores in the same process keep
/// working. Call <see cref="Init"/> at startup, before anything opens a SQLite connection: a
/// connection opened on one engine must not be used after the engine is swapped. Constructing a
/// <see cref="SqlCipherDatabaseProvider"/> or <see cref="SqlCipherDocumentStore"/> calls it for you.
/// </para>
/// </remarks>
public static class SqlCipherEngine
{
    /// <summary>The native library name SQLitePCLRaw reports once this engine is active.</summary>
    public const string NativeLibraryName = "sqlite3mc";

    static readonly object gate = new();
    static bool initialized;

    /// <summary>Installs and freezes the SQLite3MC provider. Idempotent and thread-safe.</summary>
    public static void Init()
    {
        if (initialized)
            return;

        lock (gate)
        {
            if (initialized)
                return;

            raw.SetProvider(new SQLite3Provider_sqlite3mc());
            raw.FreezeProvider();
            initialized = true;
        }
    }
}
