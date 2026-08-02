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
        """;
}
