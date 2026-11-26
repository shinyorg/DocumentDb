using System.Runtime.CompilerServices;
using Shiny.DocumentDb.Sqlite.SqlCipher;

namespace ShinyDocDbMyAdmin.Tui.Tests;

static class SqliteEngineSetup
{
    // The admin tools run every SQLite connection - plain and encrypted - on SQLite3 Multiple Ciphers.
    // Pin it before any test opens a connection, so no SQLCipher connection swaps the engine underneath
    // an open e_sqlite3 one.
    [ModuleInitializer]
    internal static void Init() => SqlCipherEngine.Init();
}
