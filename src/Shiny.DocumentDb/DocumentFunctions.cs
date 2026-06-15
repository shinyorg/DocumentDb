using Shiny.DocumentDb.Internal;

namespace Shiny.DocumentDb;

/// <summary>
/// Query-translatable functions with no direct BCL equivalent. Used inside a <c>Where</c> predicate or
/// projection — e.g. <c>store.Query&lt;Person&gt;().Where(p =&gt; DocumentFunctions.Soundex(p.Name) == DocumentFunctions.Soundex("Smith"))</c>.
/// Each method has a real implementation, so the in-memory providers (LiteDB, IndexedDB) execute it
/// directly while the pushdown providers translate it to a native / registered function.
/// </summary>
public static class DocumentFunctions
{
    /// <summary>
    /// Classic American Soundex phonetic code. Translates to native <c>SOUNDEX()</c> (SQL Server, MySQL,
    /// Oracle), the <c>fuzzystrmatch</c> <c>soundex()</c> (PostgreSQL), or a registered connection UDF
    /// (SQLite, DuckDB). Not translatable on Cosmos/Mongo — use a computed stored field there.
    /// </summary>
    public static string Soundex(string value) => Phonetics.Soundex(value);
}
