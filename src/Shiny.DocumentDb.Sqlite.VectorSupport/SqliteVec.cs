using System.Runtime.InteropServices;
using Shiny.DocumentDb.Sqlite;

namespace Shiny.DocumentDb.Sqlite.VectorSupport;

/// <summary>
/// Registers the bundled <c>sqlite-vec</c> native binary as a SQLite auto-extension so the
/// <c>vec0</c> virtual table is available on every connection — without calling
/// <see cref="Microsoft.Data.Sqlite.SqliteConnection.LoadExtension(string)"/>. This is the only
/// approach that works on iOS (Apple forbids <c>dlopen</c> of loose libraries) and the simplest
/// uniform approach on Android and desktop.
/// </summary>
public static partial class SqliteVec
{
    const string VecInitSymbol = "sqlite3_vec_init";

    // sqlite3_auto_extension lives in the SQLite engine. On iOS/tvOS the engine (SQLitePCLRaw's
    // e_sqlite3) is statically linked into the app image, reached via "__Internal" — the same way
    // SQLitePCLRaw binds its own sqlite3_* calls. On Mac Catalyst / desktop / Android it's the
    // dynamic e_sqlite3 library.
    [LibraryImport("__Internal", EntryPoint = "sqlite3_auto_extension")]
    private static partial int AutoExtensionInternal(IntPtr entryPoint);

    [LibraryImport("e_sqlite3", EntryPoint = "sqlite3_auto_extension")]
    private static partial int AutoExtensionDynamic(IntPtr entryPoint);

    // Statically-linked sqlite-vec entry point (iOS/tvOS). Declaring this P/Invoke does two jobs:
    // it binds the symbol the same way SQLitePCLRaw binds sqlite3_*, AND it anchors it against the
    // native linker's -dead_strip (-force_load pulls the .a object in, but with no reference the
    // symbol would still be stripped). The trampoline below gives us an address for auto_extension
    // without depending on the symbol being exported for dlsym.
    [LibraryImport("__Internal", EntryPoint = "sqlite3_vec_init")]
    private static partial int VecInitInternal(IntPtr db, IntPtr pzErrMsg, IntPtr pApi);

    [UnmanagedCallersOnly]
    private static int VecInitTrampoline(IntPtr db, IntPtr pzErrMsg, IntPtr pApi)
        => VecInitInternal(db, pzErrMsg, pApi);

    static readonly object gate = new();
    static bool registered;

    // Real iOS/tvOS apps statically link BOTH the SQLite engine and sqlite-vec into the main image.
    // Everywhere else they're dynamic libraries: desktop/Android, and Mac Catalyst too
    // (OperatingSystem.IsIOS() returns true there, but it ships e_sqlite3 and sqlite-vec as dylibs,
    // so it must be treated as dynamic — hence the explicit Catalyst exclusion).
    static bool StaticallyLinked =>
        (OperatingSystem.IsIOS() || OperatingSystem.IsTvOS()) && !OperatingSystem.IsMacCatalyst();

    /// <summary>
    /// Registers <c>sqlite-vec</c> so <c>vec0</c> is available on every connection opened afterward.
    /// Call once at startup, before constructing the <c>DocumentStore</c>. Idempotent and thread-safe.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The native binary or the SQLite engine could not be located, or the SQLite engine rejected
    /// the registration.
    /// </exception>
    public static void RegisterAutoExtension()
    {
        if (registered)
            return;

        lock (gate)
        {
            if (registered)
                return;

            // Ensure the SQLitePCLRaw provider is bootstrapped before we touch sqlite3_* symbols.
            SQLitePCL.Batteries_V2.Init();

            int rc;
            if (StaticallyLinked)
            {
                // The statically-linked entry point is reached through a trampoline whose address we
                // can take in managed code (a raw P/Invoke target has no usable address here).
                unsafe
                {
                    delegate* unmanaged<IntPtr, IntPtr, IntPtr, int> entry = &VecInitTrampoline;
                    rc = AutoExtensionInternal((IntPtr)entry);
                }
            }
            else
            {
                rc = CallDynamicAutoExtension(ResolveDynamicVecInit());
            }

            if (rc != 0) // SQLITE_OK
                throw new InvalidOperationException(
                    $"sqlite3_auto_extension(sqlite3_vec_init) returned {rc}. The SQLite engine rejected the sqlite-vec registration.");

            registered = true;
        }
    }

    /// <summary>
    /// Calls <see cref="RegisterAutoExtension"/> and returns a <see cref="SqliteDatabaseProvider"/>
    /// configured with <see cref="SqliteDatabaseProvider.VectorExtensionPreloaded"/> set — ready to
    /// pass to <c>DocumentStoreOptions.DatabaseProvider</c>.
    /// </summary>
    public static SqliteDatabaseProvider CreateProvider(string connectionString)
    {
        RegisterAutoExtension();
        return new SqliteDatabaseProvider(connectionString) { VectorExtensionPreloaded = true };
    }

    // Desktop / Android / Mac Catalyst: the engine is the dynamic e_sqlite3 library. Fall back to
    // "__Internal" just in case a toolchain links it statically.
    static int CallDynamicAutoExtension(IntPtr entryPoint)
    {
        try
        {
            return AutoExtensionDynamic(entryPoint);
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            return AutoExtensionInternal(entryPoint);
        }
    }

    // Loads the bundled loadable sqlite-vec binary (vec0.dylib/.so/.dll or libsqlite_vec0.so) and
    // returns the sqlite3_vec_init address. Used everywhere except statically-linked iOS/tvOS.
    static IntPtr ResolveDynamicVecInit()
    {
        foreach (var name in new[] { "sqlite_vec0", "vec0" })
        {
            if (NativeLibrary.TryLoad(name, typeof(SqliteVec).Assembly, null, out var handle) &&
                NativeLibrary.TryGetExport(handle, VecInitSymbol, out var export))
                return export;
        }

        // Last resort: it may already be in the main image.
        try
        {
            if (NativeLibrary.TryGetExport(NativeLibrary.GetMainProgramHandle(), VecInitSymbol, out var p))
                return p;
        }
        catch
        {
            // ignore — fall through to the error below
        }

        throw new InvalidOperationException(
            "Could not load the bundled sqlite-vec native binary. Ensure the " +
            "Shiny.DocumentDb.Sqlite.VectorSupport package shipped it for this platform, or load the " +
            "extension yourself via SqliteDatabaseProvider.EnableVectorExtension.");
    }
}
