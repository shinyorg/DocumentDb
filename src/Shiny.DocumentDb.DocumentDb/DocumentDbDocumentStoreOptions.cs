using System.Security.Cryptography.X509Certificates;
using Shiny.DocumentDb.MongoDb;

namespace Shiny.DocumentDb.DocumentDb;

/// <summary>
/// Options for the Amazon DocumentDB provider. Amazon DocumentDB is MongoDB wire-compatible, so this
/// extends <see cref="MongoDbDocumentStoreOptions"/> verbatim and layers the two connection concerns that
/// trip everyone up when pointing the MongoDB driver at a DocumentDB cluster:
///
/// <list type="bullet">
///   <item><b>TLS is on by default.</b> Amazon DocumentDB clusters require TLS. Point
///   <see cref="CaCertificatePath"/> at the Amazon RDS <c>global-bundle.pem</c> (or preload it into
///   <see cref="CaCertificates"/>) so the server certificate chains to the Amazon root. Set
///   <see cref="UseTls"/> to <c>false</c> only for local testing against a plain MongoDB endpoint.</item>
///   <item><b><c>retryWrites</c> is off by default.</b> Amazon DocumentDB does not implement retryable
///   writes; the MongoDB driver's default of <c>true</c> makes every write fail. <see cref="RetryWrites"/>
///   defaults to <c>false</c> to match the cluster.</item>
/// </list>
/// </summary>
public class DocumentDbDocumentStoreOptions : MongoDbDocumentStoreOptions
{
    /// <summary>
    /// Whether to connect over TLS. Amazon DocumentDB clusters require TLS, so this defaults to <c>true</c>.
    /// Set to <c>false</c> only for local testing against a plain (non-TLS) MongoDB-compatible endpoint.
    /// </summary>
    public bool UseTls { get; set; } = true;

    /// <summary>
    /// Path to the Amazon RDS CA bundle (<c>global-bundle.pem</c>) used to validate the DocumentDB server
    /// certificate. Ignored when <see cref="UseTls"/> is <c>false</c> or when <see cref="CaCertificates"/> is
    /// supplied. Download from
    /// <c>https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem</c>.
    /// </summary>
    public string? CaCertificatePath { get; set; }

    /// <summary>
    /// Pre-loaded CA certificate(s) used to validate the DocumentDB server certificate, as an alternative to
    /// <see cref="CaCertificatePath"/>. When set, this takes precedence over the path.
    /// </summary>
    public X509Certificate2Collection? CaCertificates { get; set; }

    /// <summary>
    /// Whether retryable writes are enabled. Amazon DocumentDB does not support retryable writes, so this
    /// defaults to <c>false</c>. Leaving the MongoDB driver default (<c>true</c>) causes writes to fail.
    /// </summary>
    public bool RetryWrites { get; set; }

    /// <summary>
    /// Resolves the CA certificate collection from <see cref="CaCertificates"/> or, failing that,
    /// <see cref="CaCertificatePath"/>. Returns <c>null</c> when neither is configured (the platform trust
    /// store is then used for validation).
    /// </summary>
    internal X509Certificate2Collection? ResolveCaCertificates()
    {
        if (this.CaCertificates is { Count: > 0 })
            return this.CaCertificates;

        if (!String.IsNullOrWhiteSpace(this.CaCertificatePath))
        {
            var collection = new X509Certificate2Collection();
            collection.ImportFromPemFile(this.CaCertificatePath);
            return collection;
        }
        return null;
    }
}
