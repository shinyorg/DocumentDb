using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// The demo-mode switch. The feature has to be genuinely absent, not merely unlinked - a public demo
/// instance showing a "configure AI" box invites a stranger to paste their own API key into it.
/// </summary>
public sealed class AiAvailabilityTests
{
    static AiAvailability Build(string? disable)
    {
        var values = new Dictionary<string, string?>();
        if (disable is not null)
            values[AiAvailability.DisableKey] = disable;

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        return new AiAvailability(configuration, NullLogger<AiAvailability>.Instance);
    }

    [Fact]
    public void EnabledByDefault()
        => Assert.True(Build(null).IsEnabled);

    [Theory]
    [InlineData("true")]
    [InlineData("True")]
    [InlineData("TRUE")]
    public void DisableFlag_TurnsItOff(string value)
        => Assert.False(Build(value).IsEnabled);

    [Fact]
    public void ExplicitFalse_LeavesItOn()
        => Assert.True(Build("false").IsEnabled);

    [Fact]
    public void AssertEnabled_ThrowsWhenOff()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Build("true").AssertEnabled());
        Assert.Contains("disabled", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AssertEnabled_IsSilentWhenOn()
        => Build(null).AssertEnabled();

    [Fact]
    public void TheKeyIsTheDocumentedOne()
        // Named in the readme, the docs and the UI's "switched off" screens; renaming it silently
        // would leave a demo instance serving an assistant it thought it had turned off.
        => Assert.Equal("ShinyDocDbMyAdmin:DisableAi", AiAvailability.DisableKey);
}
