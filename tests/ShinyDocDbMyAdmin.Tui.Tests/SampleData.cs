namespace ShinyDocDbMyAdmin.Tui.Tests;

/// <summary>
/// A minimal store with one of everything the workspace can show: documents, temporal history,
/// blobs, geometry in the bodies, and an embedding.
/// </summary>
/// <remarks>
/// Written out rather than produced through the library so the tests are pinned to the <i>shape</i>
/// the tool reads - the <c>Id / TypeName / Data / CreatedAt / UpdatedAt</c> envelope and the sidecar
/// names - rather than to whatever the library happens to emit this release. The DDL is copied from
/// what <c>SqliteDatabaseProvider</c> writes; a divergence there should fail these tests, which is
/// the point.
/// </remarks>
public static class SampleData
{
    public const string Sql =
        """
        CREATE TABLE IF NOT EXISTS "documents" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Data TEXT NOT NULL,
            CreatedAt TEXT NOT NULL,
            UpdatedAt TEXT NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        CREATE INDEX IF NOT EXISTS idx_documents_typename ON "documents" (TypeName);

        CREATE TABLE IF NOT EXISTS "documents_history" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Version INTEGER NOT NULL,
            ValidFrom TEXT NOT NULL,
            ValidTo TEXT NULL,
            Operation TEXT NOT NULL,
            Actor TEXT NULL,
            Data TEXT NULL,
            TenantId TEXT NULL,
            PRIMARY KEY (Id, TypeName, Version)
        );

        CREATE TABLE IF NOT EXISTS "documents_blobs" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            BlobKey TEXT NOT NULL,
            Data BLOB NOT NULL,
            Length INTEGER NOT NULL,
            ContentType TEXT NULL,
            FileName TEXT NULL,
            Hash TEXT NULL,
            CreatedAt TEXT NOT NULL,
            UpdatedAt TEXT NOT NULL,
            PRIMARY KEY (Id, TypeName, BlobKey)
        );

        INSERT INTO "documents" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES
        ('order-1', 'Order',
         '{"id":"order-1","reference":"SO-1001","status":"Shipped","total":128.5,"customer":{"name":"Ada Lovelace","city":"London"},"deliverTo":{"type":"Point","coordinates":[-0.1276,51.5072]},"embedding":[0.11,0.42,-0.19,0.07,0.55,-0.31,0.02,0.88,0.14,-0.6]}',
         '2026-01-04 09:15:00.000000+00:00', '2026-02-11 16:40:00.000000+00:00'),
        ('order-2', 'Order',
         '{"id":"order-2","reference":"SO-1002","status":"Pending","total":42.0,"customer":{"name":"Grace Hopper","city":"New York"},"deliverTo":{"type":"Point","coordinates":[-74.006,40.7128]},"embedding":[0.09,0.4,-0.2,0.11,0.51,-0.28,0.05,0.9,0.1,-0.58]}',
         '2026-02-02 11:00:00.000000+00:00', '2026-02-02 11:00:00.000000+00:00'),
        ('order-3', 'Order',
         '{"id":"order-3","reference":"SO-1003","status":"Cancelled","total":9.99,"customer":{"name":"Alan Turing","city":"Manchester"},"deliverTo":{"type":"Point","coordinates":[-2.2426,53.4808]}}',
         '2026-02-08 08:20:00.000000+00:00', '2026-02-09 08:20:00.000000+00:00'),
        ('product-1', 'Product',
         '{"id":"product-1","sku":"WID-1","name":"Widget","price":19.95,"tags":["hardware","small"]}',
         '2026-01-02 10:00:00.000000+00:00', '2026-01-02 10:00:00.000000+00:00');

        INSERT INTO "documents_history" (Id, TypeName, Version, ValidFrom, ValidTo, Operation, Actor, Data) VALUES
        ('order-1', 'Order', 1, '2026-01-04 09:15:00.000000+00:00', '2026-02-11 16:40:00.000000+00:00', 'Inserted', 'seed',
         '{"id":"order-1","reference":"SO-1001","status":"Pending","total":128.5,"customer":{"name":"Ada Lovelace","city":"London"}}'),
        ('order-1', 'Order', 2, '2026-02-11 16:40:00.000000+00:00', NULL, 'Updated', 'seed',
         '{"id":"order-1","reference":"SO-1001","status":"Shipped","total":128.5,"customer":{"name":"Ada Lovelace","city":"London"}}'),
        ('order-3', 'Order', 1, '2026-02-08 08:20:00.000000+00:00', '2026-02-09 08:20:00.000000+00:00', 'Inserted', 'seed',
         '{"id":"order-3","reference":"SO-1003","status":"Pending","total":9.99}'),
        ('order-3', 'Order', 2, '2026-02-09 08:20:00.000000+00:00', NULL, 'Updated', 'seed',
         '{"id":"order-3","reference":"SO-1003","status":"Cancelled","total":9.99}');

        INSERT INTO "documents_blobs" (Id, TypeName, BlobKey, Data, Length, ContentType, FileName, Hash, CreatedAt, UpdatedAt) VALUES
        ('order-1', 'Order', 'invoice', CAST('invoice SO-1001' AS BLOB), 15, 'text/plain', 'SO-1001.txt', 'abc123',
         '2026-02-11 16:41:00.000000+00:00', '2026-02-11 16:41:00.000000+00:00');

        -- The transactional outbox, in the dedicated table options.AddOutbox() maps it to. Three of the
        -- four states; Scheduled is the one a fixed timestamp cannot express (it means "backed off past
        -- *now*"), so the screen's Scheduled column reads 0 here and that is correct.
        --
        -- The body's timestamps are System.Text.Json's DateTimeOffset shape, NOT the envelope-column
        -- format above. The outbox filters compare them as ISO-8601 text, so a mismatch here would show
        -- up as a screen that renders but sorts every message into the wrong bucket.
        CREATE TABLE IF NOT EXISTS "outbox" (
            Id TEXT NOT NULL,
            TypeName TEXT NOT NULL,
            Data TEXT NOT NULL,
            CreatedAt TEXT NOT NULL,
            UpdatedAt TEXT NOT NULL,
            PRIMARY KEY (Id, TypeName)
        );
        CREATE INDEX IF NOT EXISTS idx_outbox_typename ON "outbox" (TypeName);

        INSERT INTO "outbox" (Id, TypeName, Data, CreatedAt, UpdatedAt) VALUES
        ('0192a1000001700080aaaaaaaaaaaaaa', 'OutboxMessage',
         '{"id":"0192a1000001700080aaaaaaaaaaaaaa","messageType":"Shop.OrderPlaced","payload":"{\"orderId\":\"order-1\"}","partitionKey":"order-1","headers":{"traceparent":"00-1111111111111111111111111111111a-2222222222222222-01"},"createdAt":"2026-02-11T16:42:00+00:00","availableAt":"2026-02-11T16:42:00+00:00","attempts":1,"processedAt":"2026-02-11T16:42:03+00:00","deadLetteredAt":null,"error":null,"version":2}',
         '2026-02-11 16:42:00.000000+00:00', '2026-02-11 16:42:03.000000+00:00'),
        ('0192a1000002700080bbbbbbbbbbbbbb', 'OutboxMessage',
         '{"id":"0192a1000002700080bbbbbbbbbbbbbb","messageType":"Shop.OrderPlaced","payload":"{\"orderId\":\"order-2\"}","partitionKey":"order-2","headers":{"traceparent":"00-3333333333333333333333333333333b-4444444444444444-01"},"createdAt":"2026-02-12T09:00:00+00:00","availableAt":"2026-02-12T09:00:00+00:00","attempts":0,"processedAt":null,"deadLetteredAt":null,"error":null,"version":1}',
         '2026-02-12 09:00:00.000000+00:00', '2026-02-12 09:00:00.000000+00:00'),
        ('0192a1000003700080cccccccccccccc', 'OutboxMessage',
         '{"id":"0192a1000003700080cccccccccccccc","messageType":"Billing.InvoiceRequested","payload":"{\"orderId\":\"order-3\"}","partitionKey":"order-3","headers":{"traceparent":"00-5555555555555555555555555555555c-6666666666666666-01"},"createdAt":"2026-02-12T09:05:00+00:00","availableAt":"2026-02-12T09:20:00+00:00","attempts":8,"processedAt":null,"deadLetteredAt":"2026-02-12T09:20:00+00:00","error":"HttpRequestException: POST https://billing.internal/v1/invoices returned 503","version":9}',
         '2026-02-12 09:05:00.000000+00:00', '2026-02-12 09:20:00.000000+00:00');
        """;
}
