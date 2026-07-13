using System.Linq.Expressions;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using MongoDB.Driver;
using Shiny.DocumentDb.MongoDb;

namespace Shiny.DocumentDb.DocumentDb;

/// <summary>
/// Amazon DocumentDB provider. Amazon DocumentDB emulates the MongoDB wire protocol on a different engine,
/// so the entire document-native store — CRUD, LINQ query pushdown, ordering, paging, projection, spatial
/// (<c>2dsphere</c>), temporal history, backup, maintenance, CAS, unit-of-work, and in-process change
/// observation — is inherited verbatim from <see cref="MongoDbDocumentStore"/>. Only the connection setup
/// and the two capabilities Amazon DocumentDB does not implement diverge.
///
/// <para><b>Divergences handled:</b></para>
/// <list type="bullet">
///   <item><b>Connection.</b> TLS is on by default (Amazon DocumentDB clusters require it) using the Amazon
///   RDS CA bundle, and <c>retryWrites</c> is forced <c>false</c> (Amazon DocumentDB has no retryable
///   writes; the driver default of <c>true</c> otherwise breaks every write). See
///   <see cref="DocumentDbDocumentStoreOptions"/>.</item>
///   <item><b>Full-text search is unsupported.</b> Amazon DocumentDB has no MongoDB <c>$text</c> index, so
///   <see cref="SupportsFullText"/> is <c>false</c> and <see cref="FullTextSearch{T}"/> throws. Use Amazon
///   OpenSearch (or the OpenSearch integration) for full-text.</item>
///   <item><b>Vector search is unsupported.</b> Amazon DocumentDB's vector search uses a different syntax to
///   Atlas <c>$vectorSearch</c>, so <see cref="SupportsVector"/> is <c>false</c> and
///   <see cref="NearestVectors{T}"/> throws in this release.</item>
/// </list>
/// </summary>
public class DocumentDbDocumentStore : MongoDbDocumentStore
{
    public DocumentDbDocumentStore(DocumentDbDocumentStoreOptions options, IServiceProvider serviceProvider)
        : base(options, serviceProvider)
    {
    }

    public DocumentDbDocumentStore(DocumentDbDocumentStoreOptions options)
        : base(options)
    {
    }

    /// <summary>
    /// Builds the MongoDB driver client for an Amazon DocumentDB cluster: applies TLS with the configured CA
    /// bundle and forces <c>retryWrites=false</c>. A pre-configured
    /// <see cref="MongoDbDocumentStoreOptions.MongoClient"/> is honoured as-is when supplied.
    /// </summary>
    protected override IMongoClient CreateMongoClient(MongoDbDocumentStoreOptions options)
    {
        if (options.MongoClient != null)
            return options.MongoClient;

        var ddb = (DocumentDbDocumentStoreOptions)options;
        var settings = MongoClientSettings.FromConnectionString(options.ConnectionString);

        // Amazon DocumentDB does not support retryable writes; the driver default of true breaks writes.
        settings.RetryWrites = ddb.RetryWrites;

        if (ddb.UseTls)
        {
            settings.UseTls = true;
            var caCerts = ddb.ResolveCaCertificates();
            if (caCerts is { Count: > 0 })
            {
                settings.SslSettings = new SslSettings
                {
                    ServerCertificateValidationCallback = (_, certificate, _, errors)
                        => ValidateServerCertificate(caCerts, certificate, errors)
                };
            }
        }
        else
        {
            settings.UseTls = false;
        }
        return new MongoClient(settings);
    }

    // Validates the DocumentDB server certificate against the supplied Amazon RDS CA bundle by building a
    // chain rooted in that bundle (custom trust store), rather than the machine trust store which does not
    // contain the Amazon RDS roots.
    static bool ValidateServerCertificate(
        X509Certificate2Collection caCerts,
        X509Certificate? certificate,
        SslPolicyErrors errors)
    {
        if (errors == SslPolicyErrors.None)
            return true;

        if (certificate == null || (errors & SslPolicyErrors.RemoteCertificateChainErrors) == 0)
            return false;

        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.CustomTrustStore.AddRange(caCerts);

        using var serverCert = new X509Certificate2(certificate);
        return chain.Build(serverCert);
    }

    // Amazon DocumentDB has no MongoDB $text index.
    public override bool SupportsFullText => false;

    public override Task<IReadOnlyList<FullTextResult<T>>> FullTextSearch<T>(
        string searchText,
        int maxResults = 50,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "Amazon DocumentDB does not support MongoDB $text full-text search. Use Amazon OpenSearch " +
            "(or the OpenSearch integration) for full-text queries.");

    // Amazon DocumentDB's vector search uses a different syntax to Atlas $vectorSearch.
    public override bool SupportsVector => false;

    public override Task<IReadOnlyList<VectorResult<T>>> NearestVectors<T>(
        ReadOnlyMemory<float> query,
        int k,
        Expression<Func<T, bool>>? filter = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "Amazon DocumentDB vector search is not supported by this provider. Its vector syntax differs " +
            "from Atlas $vectorSearch; use the MongoDB (Atlas) provider for NearestVectors.");
}
