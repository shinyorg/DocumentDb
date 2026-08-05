using System.Text.Json.Nodes;
using ShinyDocDbMyAdmin.Services;

namespace ShinyDocDbMyAdmin.Tests;

/// <summary>
/// Recognising an envelope in a stored body, with no key and no database.
/// </summary>
public class EncryptedFieldsTests
{
    static JsonNode Body(string json) => JsonNode.Parse(json)!;

    [Fact]
    public void PathsIn_FindsTopLevelAndOneLevelOfNesting()
    {
        var body = Body("""
            {
              "id": "p1",
              "ssn": "enc:1:k1:AAECAwQF",
              "name": "Ada",
              "contact": { "email": "enc:1:k1:BBECAwQF", "city": "London" }
            }
            """);

        Assert.Equal(["ssn", "contact.email"], EncryptedFields.PathsIn(body));
    }

    [Fact]
    public void ANumericArray_IsStillAVectorSummary_NotAnEnvelope()
    {
        // Both render as "not the raw value", and the two must not be confused: a vector is a container of
        // numbers, an envelope is a string. Only the string branch may claim encryption.
        var vector = new JsonArray([.. Enumerable.Range(0, 40).Select(i => JsonValue.Create(i * 0.1))]);

        Assert.False(JsonDisplay.TryEncryptedSummary(vector, out _));
        Assert.True(JsonDisplay.TryVectorSummary(vector, out var summary));
        Assert.Equal(summary, JsonDisplay.Cell(vector));
    }

    [Fact]
    public void TheProfileSecretPrefix_IsNotAnEnvelope()
    {
        // SecretProtector wraps this tool's own stored connection strings with "enc:v1:". Nothing separates
        // the two formats except that "v1" does not parse as an integer, so it is worth pinning here too.
        var wrapped = JsonValue.Create("enc:v1:c29tZS1jaXBoZXJ0ZXh0");

        Assert.False(EncryptedFields.TryRead(wrapped, out _));
        Assert.Equal("enc:v1:c29tZS1jaXBoZXJ0ZXh0", JsonDisplay.Cell(wrapped));
    }

    [Fact]
    public void ACellShowsWhatTheValueIs_NotAWallOfBase64()
    {
        var value = JsonValue.Create("enc:1:k2:" + new string('A', 300));

        Assert.Equal("encrypted · key k2", JsonDisplay.Cell(value));
    }

    [Fact]
    public void Read_WalksADottedPath_AndReturnsNullForAMissingOne()
    {
        var body = Body("""{"contact": {"email": "e"}}""");

        Assert.Equal("e", EncryptedFields.Read(body, "contact.email")?.GetValue<string>());
        Assert.Null(EncryptedFields.Read(body, "contact.phone"));
        Assert.Null(EncryptedFields.Read(body, "nothing.at.all"));
    }

    // ── The downgrade diff ───────────────────────────────────────────────

    [Fact]
    public void AnUntouchedEnvelope_IsNotADowngrade()
    {
        var stored = Body("""{"ssn": "enc:1:k1:AAECAwQF"}""");

        Assert.Empty(DocumentAdminService.FindEncryptionDowngrades(stored, stored.DeepClone(), []));
    }

    [Fact]
    public void ReplacingAnEnvelopeWithPlaintext_IsADowngrade()
    {
        var stored = Body("""{"ssn": "enc:1:k1:AAECAwQF"}""");
        var submitted = Body("""{"ssn": "867-53-0900"}""");

        Assert.Equal(["ssn"], DocumentAdminService.FindEncryptionDowngrades(stored, submitted, []));
    }

    [Fact]
    public void RemovingTheFieldEntirely_IsNotADowngrade()
    {
        // Deleting a value is an ordinary edit. It is exposing one that is not.
        var stored = Body("""{"ssn": "enc:1:k1:AAECAwQF", "name": "Ada"}""");

        Assert.Empty(DocumentAdminService.FindEncryptionDowngrades(stored, Body("""{"name": "Ada"}"""), []));
        Assert.Empty(DocumentAdminService.FindEncryptionDowngrades(stored, Body("""{"ssn": null}"""), []));
    }

    [Fact]
    public void ANewDocumentWithPlaintextInAPathTheTypeProtects_IsADowngrade()
    {
        // No stored body to diff against - the type's other documents are what says this path is protected.
        var submitted = Body("""{"id": "p9", "ssn": "867-53-0900"}""");

        Assert.Equal(["ssn"], DocumentAdminService.FindEncryptionDowngrades(null, submitted, ["ssn"]));
    }

    [Fact]
    public void ANewDocumentThatCarriesAnEnvelope_IsFine()
    {
        var submitted = Body("""{"id": "p9", "ssn": "enc:1:k1:AAECAwQF"}""");

        Assert.Empty(DocumentAdminService.FindEncryptionDowngrades(null, submitted, ["ssn"]));
    }

    [Fact]
    public void ANestedEncryptedPath_IsCaughtToo()
    {
        var stored = Body("""{"contact": {"email": "enc:1:k1:AAECAwQF"}}""");
        var submitted = Body("""{"contact": {"email": "ada@example.com"}}""");

        Assert.Equal(["contact.email"], DocumentAdminService.FindEncryptionDowngrades(stored, submitted, []));
    }

    [Fact]
    public void TheExceptionNamesThePathsAndSaysWhatHappens()
    {
        var ex = new EncryptedFieldDowngradeException(["ssn"]);

        Assert.Equal(["ssn"], ex.Paths);
        Assert.Contains("clear text", ex.Message);
        Assert.Contains("no longer protected", ex.Message);
    }
}
