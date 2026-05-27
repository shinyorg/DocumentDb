using System.Runtime.InteropServices.JavaScript;

namespace Shiny.DocumentDb.IndexedDb;

/// <summary>
/// Source-generated JS interop bindings for the IndexedDB provider. Uses
/// [JSImport] from System.Runtime.InteropServices.JavaScript so this library
/// is fully AOT/reflection-free — no IJSRuntime serialization, no Object[]
/// envelope, nothing that requires JsonSerializerIsReflectionEnabledByDefault.
///
/// Complex types (DocumentRecord, DocumentRecord[]) are pre-serialized to JSON
/// strings on the C# side using the source-generated IndexedDbInteropJsonContext
/// and JSON.parse'd on the JS side. Results that include complex types come
/// back as JSON strings and are deserialized via the same context.
/// </summary>
internal static partial class IndexedDbJsInterop
{
    public const string ModuleName = "shiny-indexeddb";
    // Path is resolved relative to dotnet.js (which lives in /_framework/), not the document base href.
    // Hence "../_content/..." to step up out of /_framework/ and into the static-web-asset _content/ root.
    public const string ModulePath = "../_content/Shiny.DocumentDb.IndexedDb/shiny-indexeddb.js";

    public static Task ImportAsync() => JSHost.ImportAsync(ModuleName, ModulePath);

    [JSImport("initialize", ModuleName)]
    public static partial Task Initialize(string databaseName, int version, string[] storeNames);

    [JSImport("get", ModuleName)]
    public static partial Task<string?> Get(string storeName, string key);

    [JSImport("put", ModuleName)]
    public static partial Task Put(string storeName, string recordJson);

    [JSImport("remove", ModuleName)]
    public static partial Task<bool> Remove(string storeName, string key);

    [JSImport("getAllByTypeName", ModuleName)]
    public static partial Task<string> GetAllByTypeName(string storeName, string typeName);

    [JSImport("countByTypeName", ModuleName)]
    public static partial Task<int> CountByTypeName(string storeName, string typeName);

    [JSImport("clearByTypeName", ModuleName)]
    public static partial Task<int> ClearByTypeName(string storeName, string typeName);

    [JSImport("batchPut", ModuleName)]
    public static partial Task BatchPut(string storeName, string recordsJson);

    [JSImport("batchDelete", ModuleName)]
    public static partial Task BatchDelete(string storeName, string[] keys);
}
