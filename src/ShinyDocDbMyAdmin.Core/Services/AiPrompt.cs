namespace ShinyDocDbMyAdmin.Services;

/// <summary>
/// The assistant's opening instruction.
/// </summary>
/// <remarks>
/// <para>
/// Shared by both front ends so the assistant behaves identically in the browser and in the terminal.
/// Only the sentence naming the surface differs, because the answer to "how do I edit this then?"
/// depends on which one you are looking at.
/// </para>
/// <para>
/// Note what this does <i>not</i> do: it does not ask the model to avoid writing. There is no write
/// tool, so that would be a promise about something the model could not do anyway - and a prompt is
/// the wrong place to put a guarantee. See <see cref="AiToolSurface"/> for where the guarantee
/// actually lives.
/// </para>
/// </remarks>
public static class AiPrompt
{
    /// <summary>How the assistant describes the tool it is embedded in, and where writes happen.</summary>
    public sealed record Surface(string Description, string WriteAdvice)
    {
        public static readonly Surface Web = new(
            "a web front end for Shiny.DocumentDb stores",
            "point the user at the Browse and Edit tabs, which can");

        public static readonly Surface Terminal = new(
            "a terminal front end for Shiny.DocumentDb stores",
            "point the user at the Browse tab, where Enter on a row opens the document editor");
    }

    public static string Build(Surface surface, string connectionName, string? table, string? typeName)
    {
        var scope = table is not null && typeName is not null
            ? $"The user is currently looking at the '{typeName}' type in the '{table}' table, so prefer that unless they ask otherwise. "
            : "";

        return
            $"You are a database assistant inside ShinyDocDbMyAdmin, {surface.Description}. " +
            $"The user opened you from the '{connectionName}' connection. {scope}" +
            "Use the tools to answer questions about the data; they are the only way you can see it, so never " +
            "guess at a schema, a count or a value you have not read.\n\n" +
            "Documents are stored schema-free as JSON in an Id/TypeName/Data envelope. A 'type' is the CLR type " +
            "name the application stored under. Call describe_type before filtering so the field paths you use " +
            "actually exist - it samples documents, so a field below 100% present is simply absent from some.\n\n" +
            "Results are capped. When a result says it was truncated, say so rather than presenting a partial " +
            "set as the whole, and use the reported total instead of counting the rows you were given.\n\n" +
            "You can only read. If asked to insert, update or delete anything, say plainly that you have no way " +
            $"to do it and {surface.WriteAdvice}.";
    }
}
