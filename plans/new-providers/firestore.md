# Plan — Google Firestore provider (`Shiny.DocumentDb.Firestore`)

## Why / fit

Firestore completes the cloud-NoSQL trio alongside Cosmos and DynamoDB, and it's *the* mobile/offline
document database — a natural pairing with our IndexedDB (Blazor WASM) provider for the Shiny/MAUI
offline story. It brings native features the key-partitioned providers can't: **real-time snapshot
listeners** (→ change feed *and* per-query `NotifyOnChange`), **native ACID transactions** (→ real
UoW), **aggregation queries** (count/sum/avg push down), and **native KNN vector search** (2024+).

**Archetype:** document-native / NoSQL, closer to Mongo than to Dynamo because Firestore
auto-indexes fields (rich single-field pushdown) rather than requiring declared indexed columns.
`FirestoreDocumentStore : DocumentProviderBase, IDocumentStore, …`.

## Dependencies & shape

- NuGet: `Google.Cloud.Firestore` (pin centrally). TFM net10.0. `ProjectReference` → core.
- Files: `FirestoreDocumentStore.cs` (+ `.ChangeFeed.cs`, `.Backup.cs` partials), `FirestoreDocumentQuery.cs`, `FirestoreDocumentStoreOptions.cs`, `FirestoreExpressionVisitor.cs` (Expression→`Query` filters), `FirestoreDocument.cs`, `ServiceCollectionExtensions.cs`.
- **AOT:** the gRPC/`Google.Cloud.Firestore` stack is not trim-clean; annotate but don't claim `IsAotCompatible`.

## Storage model — decision: native-map storage (not opaque blob)

Firestore only queries **actual document fields** and auto-indexes every single field. To get broad
Where/OrderBy pushdown for free, store the document as a **native Firestore map** (convert our JSON to
`Dictionary<string, object>` / Firestore field values) rather than an opaque `data` string.

- Collection per type (`TypeNameResolver`, overridable via `MapTypeToCollection<T>`); document id = our id string.
- Reserved fields for bookkeeping under a metadata sub-map: `__typeName`, `__version`, `__createdAt`, `__updatedAt`. Keep them out of the user's field namespace.
- Firestore `UpdateTime` is the native CAS token (see below).

Trade-off vs. the Dynamo/Azure "opaque `data` + promoted `idx_*`" model: native-map gives far richer
pushdown, but composite queries (multiple range/`OrderBy` fields) require **composite indexes** the
user must create in the Firebase console. Document this; surface the Firestore
`FAILED_PRECONDITION`/index-creation-link error verbatim so users can one-click create the index.

## CRUD, ids, CAS

- **Ids**: Firestore natively generates string ids. Guid→`N`; String→caller-assigned (or allow `Add` auto-id as an option); Custom→converter; **Int/Long autogen throws** (parity with Dynamo/Azure — no cheap monotonic counter).
- **Upsert** = `SetAsync(..., SetOptions.MergeAll)` — server-side merge, cleaner than the client-side merge Mongo/Dynamo do.
- **CAS**: `MapVersionProperty` → `__version` guarded inside a Firestore transaction; additionally use `Precondition.LastUpdated(updateTime)` as a native ETag-equivalent. On precondition failure → `ConcurrencyException`.

## Query (`FirestoreDocumentQuery<T>`)

Translate `Expression` → Firestore `Query` (`FirestoreExpressionVisitor`). Firestore's query model is
constrained; be explicit about what pushes down:

| Surface | Pushdown | Notes |
|---|---|---|
| `Where` equality / range on a single field, `array-contains`, `in`, `array-contains-any` | ✅ | maps to `WhereEqualTo`/`WhereGreaterThan`/… |
| `Where` with `OrElse` across fields | ⚠️ | Firestore `Filter.Or` is limited (disjunctions, ≤30 clauses); translate where possible, else **client-side fallback** after a partition/collection scan |
| `!=` combined, multiple range fields | ⚠️ | Firestore restrictions — fall back client-side and note the read cost |
| `OrderBy` / `OrderByDescending` | ✅ | requires the field; multi-field ordering needs composite index |
| `Paginate(offset, take)` | ✅ | `Offset` (billed as reads) + `Limit`; prefer cursors |
| `ToCursorPage` | ✅ **native** | `StartAfter`/`StartAt` snapshot cursors — a genuine keyset pager (better than Mongo's client-side one) |
| `Count` / `Sum` / `Average` | ✅ **native** | Firestore **aggregation queries** (`Count()`, `Sum()`, `Average()`) push down |
| `Any` | ✅ | limit-1 query |
| `GroupBy` | ❌ throws | no server-side grouping; matching Dynamo/Azure/Cosmos-client tier. (Client-side grouping means a full collection read — throw and document.) |
| `Select` / `Project` | ⚠️ | Firestore field masks reduce payload; projection of computed shapes stays client-side (materialize + selector like Mongo) |
| `ExecuteDelete` / `ExecuteUpdate` | ⚠️ | query then batched writes (500/batch) client-side |
| String `Query(whereClause)` | ❌ throws | no server query language; steer to LINQ (like Mongo) |

**Guiding rule** (same as Dynamo/Azure): pushdown is an optimization; always re-apply the full
predicate + ordering + paging client-side so results are correct even when a clause couldn't be
pushed. But warn (via docs) that a non-pushable predicate degrades to a collection read.

## Capability interfaces

| Interface / feature | Support | Mechanism |
|---|---|---|
| `IUnitOfWorkEngine` | ✅ **real** | `RunTransactionAsync` or `WriteBatch` (≤500 writes) — atomic, not compensating |
| `IExplicitTransactionEngine` | ✅ optional | Firestore transactions (read-then-write) |
| `IChangeFeedDocumentStore` | ✅ **native** | `Query.Listen` / `CollectionReference.Listen` snapshot listeners → `DocumentChange<T>` (Added/Modified/Removed map directly) |
| `IObservableDocumentStore.NotifyOnChange` (per query) | ✅ **native** | filtered snapshot listener on the query — a real server-backed per-query feed, unlike Mongo (which throws) |
| `SupportsVector` / `NearestVectors` | ✅ (2024+) | Firestore `FindNearest` KNN vector search; `MapVectorProperty` → vector field + index |
| `ITemporalDocumentStore` | ⚠️ sidecar | no native history; append-only `<collection>_history` sub-collection like the Mongo sidecar, or **defer to v2** to keep scope |
| `SupportsFullText` / `FullTextSearch` | ❌ | Firestore has no native full-text (recommends Algolia/Elastic extension) — throw, document the extension route |
| `SupportsSpatial` / `Geo*` | ❌ v1 | Firestore has a `GeoPoint` type but no native geo query (needs geohashing). Throw in v1; geohash bounding-box is a possible v2 |
| `IDocumentBackup` | ✅ | stream export/import over collections |
| `IDocumentMaintenance.ClearAll` | ✅ | recursive delete over collections |
| `MapComputedProperty` | ✅ (alias) | `ComputedReadBack` on read; materialized variant writes a real field |

## Implementation phases

1. **Skeleton + CRUD (native-map) + ids + CAS + DI** (`AddFirestoreDocumentStore`; accept a pre-built `FirestoreDb`, or projectId + credentials/emulator host). CRUD conformance green.
2. **Query**: single-field Where/OrderBy/Limit/aggregation pushdown + client-side fallback for the rest; native `ToCursorPage`.
3. **Real UoW / transactions**.
4. **Change feed + per-query NotifyOnChange** via snapshot listeners (the headline).
5. **Vector** (`FindNearest`).
6. **Backup + maintenance**. Temporal sidecar optional/v2.

## Testing

- Testcontainers: the **Firestore emulator** (`gcr.io/google.com/cloudsdktool/google-cloud-cli` with `gcloud emulators firestore start`, or the `firebase-tools` emulator image). Fixture sets `FIRESTORE_EMULATOR_HOST`, implements `IDocumentStoreFixture`.
- Provider-specific: pushdown vs client-fallback assertions, cursor paging, snapshot-listener change feed, aggregation queries, KNN vector, `UpdateTime` precondition → `ConcurrencyException`.
- Note in tests: composite-index requirement — emulator does **not** enforce composite indexes, so add explicit docs/tests warning that prod requires them.

## Four-artifact sync

1. **Code + tests** above. Release note tier: cloud NoSQL, key/collection-based, native change feed + transactions + vector; no native full-text/spatial/temporal in v1.
2. **Docs**: `firestore.mdx` provider page (call out composite-index + no-full-text caveats and the mobile/offline pairing with IndexedDB); capability matrix updates; `<RN type="feature">`.
3. **Skill**: add Firestore + `AddFirestoreDocumentStore` to `triggers:` and provider list; note native real-time change feed + no full-text.
4. **readme.md**: provider list + capability callout.

## Risks / open questions

- Composite-index friction is the main UX risk — mitigate by surfacing Firestore's index-creation link on `FAILED_PRECONDITION`.
- Native-map storage means Firestore type coercion (e.g. all numbers are `long`/`double`, timestamps) must round-trip cleanly back through System.Text.Json — prototype the map↔JSON converter early and test numeric/enum/`DateTime`/nested-array fidelity.
- Offset pagination is billed as document reads — steer users to cursors in docs.
