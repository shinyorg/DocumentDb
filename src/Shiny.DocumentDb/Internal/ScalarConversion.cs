using System.Globalization;

namespace Shiny.DocumentDb.Internal;

/// <summary>
/// Converts a raw scalar a provider handed back (from <c>ExecuteScalar</c>) to the CLR type the caller asked for.
/// </summary>
static class ScalarConversion
{
    /// <summary>
    /// Like <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>, except that a <see cref="double"/> or
    /// <see cref="float"/> headed for <see cref="decimal"/> goes through its shortest round-trip text. A database that
    /// sums JSON numbers as REAL returns <c>49.97</c> as the nearest double; .NET 11's exact double-to-decimal
    /// conversion turns that into <c>49.969999999999998863…</c>, while the round-trip text gives back <c>49.97</c>
    /// on every runtime.
    /// </summary>
    public static object ChangeType(object value, Type targetType)
    {
        if (targetType == typeof(decimal))
        {
            if (value is double d && double.IsFinite(d))
                return decimal.Parse(d.ToString("R", CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture);

            if (value is float f && float.IsFinite(f))
                return decimal.Parse(f.ToString("R", CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture);
        }
        return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
    }
}
