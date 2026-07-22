# Document Blobs (v11.2)

Binary payloads attached to documents, in a **sidecar table**, **loaded on demand**, **metadata inline**.

## Status

**SHIPPED + tested on the relational family** (committed: `1c35907 Converter fixes`, `fb6f5b9 Blob sidecar`;
backup/temporal/sweep in working tree):

- **Object model** — `DocumentBlob` (self-loading `LoadAsync`/`GetBytesAsync`), `DocumentBlobCollection`
  (`LoadAllAsync`, fluent `Add(bytes,…)`), two metadata-only converters. `Bytes` throws until loaded.
- **Mapping** — `MapBlob`/`MapBlobCollection` (+ AOT accessor overloads); `BlobOptions` (Key/ComputeHash/MaxSize).
- **Provider contract** — `MaxBlobSize` (0 = unsupported) + SQL builders on `IDatabaseProvider`; DDL for
  SQLite, PostgreSQL, SQL Server, MySQL, Oracle, DuckDB (MariaDB/CockroachDB inherit).
- **Store** — `IBlobDocumentStore` (`BatchLoadBlobs` + `GetBlob`), `DocumentStore.Blob.cs`: prepare/hash/
  size-guard, sidecar write + prune, cascade, self-loading via `AttachBlobLoaders` + shared `Materialize<T>`
  seam routed through `DocumentQuery.Deserialize` and the non-tx read paths.
- **Init** — blob DDL in both init paths; blob-mapped types off the `IsBatchFastEligible` fast path.
- **Backup** — `BackupExportOptions.IncludeBlobs` (default true); payloads base64 inline per document
  (`RawBlob` on `RawDocument`/`RawBulkRow`, `blobs[]` in the record DTO), restored into the sidecar in the
  chunk transaction. Export buffers a blob-mapped table's doc rows (payloads read one at a time) to avoid a
  nested reader on shared-connection providers.
- **Temporal** — blobs not versioned; `RestampBlobsFromCurrent` keeps live blobs on `Restore` so metadata
  never describes a superseded payload.
- **Maintenance** — `IDocumentMaintenance.SweepOrphanedBlobs<T>()` (default-impl returns 0).
- **AOT** — `Generated` fixture carries `DocumentBlob` + `DocumentBlobCollection`; compiling proves the
  converter branch beats the non-`List<T>` collection guard (depends on the shipped generator fix).
- **Docs** — `blobs.mdx` + sidebar, release note, `SKILL.md`, `readme.md`, `limitations.mdx` fix. Site builds.

Tests: 20 blob tests + Generated AOT; full suite green (last confirmed 4551/0, re-run in progress).

**Known limitation:** a document materialized inside an explicit transaction stamps a loader that reads via
the parent store (committed state) — self-loading an uncommitted blob mid-transaction won't see it.

## Outstanding — the 8 non-relational providers

Each is a standalone `*DocumentStore` with a standalone options class (they mirror, not inherit), so each
needs: `MapBlob`/`MapBlobCollection` mirrored onto its options + `blobMappings`/`ResolveBlobMappings`; a
`{collection}_blobs` sidecar; strip-to-sidecar on write + write-sidecar; self-loading stamp at its
materialization; cascade delete; **blobs-first write ordering** (crash → orphan bytes, never dangling
metadata); `SweepOrphanedBlobs`. Providers: Mongo, Cosmos, LiteDB, IndexedDB, Redis, Firestore, Azure Table,
DynamoDB — RavenDB via **native attachments**. 7 of 8 need Testcontainers. These are 8 separate substantial
implementations, not a shared change. A reusable core helper (`BlobSupport.PrepareBlobs(mappings, max, doc)`)
would cut the per-provider cost.

## Design decisions (locked)

Reads-only store API (mutations via the document); byte[] only; cascade delete; backup includes by default;
temporal not versioned; Hash opt-in; single-blob Key defaults to property name, collection items carry keys.
