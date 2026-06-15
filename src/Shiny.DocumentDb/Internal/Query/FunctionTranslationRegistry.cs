using System.Reflection;

namespace Shiny.DocumentDb.Internal.Query;

/// <summary>
/// User-registered custom function translations (the Level-A extensibility surface). Maps a CLR
/// <see cref="MethodInfo"/> — captured from an expression exemplar, so it stays trim/AOT-safe — to a raw
/// SQL function name the <see cref="ExpressionLowerer"/> emits as <c>NAME(arg0, …)</c> across the
/// relational providers.
/// </summary>
sealed class FunctionTranslationRegistry
{
    readonly Dictionary<MethodInfo, string> functions = new();

    public bool IsEmpty => this.functions.Count == 0;

    public void Add(MethodInfo method, string sqlFunctionName) => this.functions[method] = sqlFunctionName;

    public bool TryGet(MethodInfo method, out string sqlFunctionName)
        => this.functions.TryGetValue(method, out sqlFunctionName!);
}
