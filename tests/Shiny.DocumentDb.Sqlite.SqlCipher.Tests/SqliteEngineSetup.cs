using System.Runtime.CompilerServices;

namespace Shiny.DocumentDb.Sqlite.SqlCipher.Tests;

static class SqliteEngineSetup
{
    // What an app does at startup: pin SQLite3 Multiple Ciphers before the first connection opens, so the
    // plain-SQLite tests here never leave an e_sqlite3 connection in the pool for a later swap to strand.
    [ModuleInitializer]
    internal static void Init() => SqlCipherEngine.Init();
}
