using System.Text;
using Shiny.DocumentDb.Sqlite;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// The envelope as a <b>read-only contract</b> — what tooling that inspects stored documents without the key
/// ring is allowed to rely on.
/// </summary>
/// <remarks>
/// Public for the same reason the envelope is a JSON string in the first place: backups carry it verbatim,
/// replicas copy it, and an operator scanning a table sees it. A second implementation of the shape test in
/// every tool that reads stored bodies would drift from this one, and the failure mode of that drift is
/// treating an envelope as plaintext.
/// </remarks>
public class DocumentEncryptionFormatTests
{
    public class Secretive
    {
        public string Id { get; set; } = "";
        public string? Ssn { get; set; }
        public string? Note { get; set; }
    }

    [Fact]
    public void AnEnvelope_ReportsItsVersionKeyAndPayload()
    {
        Assert.True(DocumentEncryptionFormat.TryParse("enc:1:k1:AAECAwQF", out var info));

        Assert.Equal(1, info.Version);
        Assert.Equal("k1", info.KeyId);
        Assert.Equal("AAECAwQF".Length, info.PayloadLength);
    }

    [Fact]
    public void TheAdminToolsOwnSecretPrefix_IsNotAnEnvelope()
    {
        // ShinyDocDbMyAdmin wraps its stored connection strings with its own, unrelated "enc:v1:" prefix.
        // Nothing disambiguates the two except this: the version segment has to parse as an integer.
        Assert.False(DocumentEncryptionFormat.IsEnvelope("enc:v1:c29tZS1jaXBoZXJ0ZXh0"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("enc:")]
    [InlineData("enc:1")]
    [InlineData("enc:1:k1")]                // a key id but no payload
    [InlineData("enc:one:k1:AAEC")]         // non-numeric version
    [InlineData("encrypted at rest")]
    [InlineData("enc:1::AAEC")]             // an empty key id is not addressable
    public void ThingsThatAreNotEnvelopes(string? value)
        => Assert.False(DocumentEncryptionFormat.IsEnvelope(value));

    [Fact]
    public async Task AValueThatMerelyStartsWithEncColon_RoundTripsUntouched()
    {
        // The converter treats a non-envelope as pre-encryption plaintext, which is what lets encryption be
        // switched on for a populated store. A document whose text happens to start with "enc:" must survive it.
        var options = new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider("Data Source=:memory:"),
            TableName = $"t{Guid.NewGuid():N}"
        };
        options.UseEncryptor(new AesGcmDocumentEncryptor("k1", AesGcmDocumentEncryptor.GenerateKey()));
        options.ConfigureDocument<Secretive>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));

        using var store = new DocumentStore(options);
        await store.Insert(new Secretive { Id = "s1", Ssn = "enc:oder von hier", Note = "plain" });

        var back = await store.Get<Secretive>("s1");
        Assert.Equal("enc:oder von hier", back!.Ssn);
    }

    [Fact]
    public void WhatTheLibraryWrites_IsWhatTheFormatParses()
    {
        // The claim the whole public contract rests on: no separate spelling of the envelope.
        var encryptor = new AesGcmDocumentEncryptor("keyring_2026", AesGcmDocumentEncryptor.GenerateKey());
        var envelope = encryptor.Encrypt("867-53-0900"u8, EncryptionMode.Randomized);

        Assert.True(DocumentEncryptionFormat.TryParse(envelope, out var info));
        Assert.Equal(1, info.Version);
        Assert.Equal("keyring_2026", info.KeyId);
    }

    [Fact]
    public void EveryCodecEncodesAsUtf8Text_WhichIsWhyToolingCanRenderWithoutTheClrType()
    {
        // The finding this method exists for: decryption needs the key, not the property's type. Without it
        // a tool showing a decrypted value would have to guess whether the bytes were a Guid or a decimal.
        var encryptor = new AesGcmDocumentEncryptor("k1", AesGcmDocumentEncryptor.GenerateKey());

        foreach (var value in (string[])["867-53-0900", "42", "3.5", "true", Guid.NewGuid().ToString(), "2026-08-04T00:00:00"])
        {
            var envelope = encryptor.Encrypt(Encoding.UTF8.GetBytes(value), EncryptionMode.Randomized);

            Assert.True(DocumentEncryptionFormat.TryRenderPlaintext(encryptor.Decrypt(envelope), out var text));
            Assert.Equal(value, text);
        }
    }

    [Fact]
    public void BytesThatAreNotUtf8_AreRefusedRatherThanShownAsReplacementCharacters()
    {
        // A partial render reads as a successful decrypt of a corrupt value; false reads as what it is.
        Assert.False(DocumentEncryptionFormat.TryRenderPlaintext([0xC3, 0x28, 0xA0], out var text));
        Assert.Equal("", text);
    }
}
