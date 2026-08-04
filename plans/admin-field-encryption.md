# Plan: Field-level encryption in the admin UIs

**Status:** Designed, not started.
**Target version:** `13.0` — the same release the feature ships in, while `version.json` is still
`13.0.0-beta.{height}`. If `13.0` cuts before this lands, it becomes `13.1`.
**Projects:** `ShinyDocDbMyAdmin.Core` (all logic), `ShinyDocDbMyAdmin` (web), `ShinyDocDbMyAdmin.Tui`
(terminal), plus one small **public** addition to core (`Shiny.DocumentDb/Encryption/`).

> Self-contained build spec. Read `CLAUDE.md` (repo root) for the four-artifact rule (code+tests, docs site,
> skill, readme) before considering any commit "done". Branch off `v12` (the active branch).

---

## Goal

Field-level encryption shipped in `13.0` (see the `encryption.mdx` docs page and
`src/Shiny.DocumentDb/Encryption/`). The admin tools know nothing about it. Today an encrypted property is a
wall of base64 in the browse grid, `string` in the Structure tab, silently unmatchable in every filter, and
editable in a way that quietly writes cleartext over ciphertext.

The application side that produces this data:

```csharp
var key = AesGcmDocumentEncryptor.GenerateKey();               // 32 bytes, from a real secret store
options.UseEncryptor(new AesGcmDocumentEncryptor("k1", key));
options.ConfigureDocument<Patient>(cfg => cfg.MapProperty(x => x.Ssn, p => p.Encrypt()));                                // Randomized (default)
options.ConfigureDocument<Member>(cfg => cfg.MapProperty(x => x.Email, p => p.Encrypt(EncryptionMode.Deterministic)));   // equality-queryable
```

…and what lands in the `Data` column is `"ssn": "enc:1:k1:<base64>"`.

Make the admin tools **fluent in that envelope**: read it, describe it, report on the key ring behind it,
refuse to quietly destroy it, and stop offering query surfaces that cannot work against it. Optionally — and
only when an operator deliberately hands over key material — decrypt it for display.

## What exists today (verified, not assumed)

| Fact | Where | Consequence |
|---|---|---|
| Nothing in the admin tree recognises `enc:` | grep across all three admin projects: the only hit is `SecretProtector.Prefix = "enc:v1:"` — its *own*, unrelated profile-secret format | The whole feature is greenfield |
| `SecretProtector`'s `enc:v1:` cannot be confused with a document envelope | `EncryptionEnvelope.IsEnvelope` requires the version segment to parse as an `int`; `"v1"` does not | No disambiguation work needed — but it needs a regression test |
| The admin writes raw JSON bodies over ADO | `DocumentAdminService.SaveDocument` → `provider.BuildInsertSql/BuildUpdateSql`, binding `@data` | It never goes through the library, so nothing encrypts on save and nothing decrypts on read |
| Editing an envelope into plaintext does not *break* the app | the converter treats a non-envelope as pre-encryption plaintext and returns it as-is | The failure mode is a **silent protection downgrade**, not an exception. That is worse |
| The Filter console compiles to SQL directly | `DocumentAdminService.Filter.cs` | The library's `EncryptedPredicateRewriter` never runs here — no constant gets encrypted, so a deterministic equality typed into the console still cannot match |
| Every `ValueCodec` encodes to **UTF-8 text** | `Encryption/ValueCodec.cs` — string/bool/int/long/double/decimal/Guid/DateTime/DateTimeOffset all `Utf8(v.ToString(Invariant))` | **Decrypted bytes are always displayable without knowing the CLR type.** This is what makes the optional decrypt lane cheap |
| The admin is relational-only | `Providers/ProviderKind.cs` (9 dialects) | Matches encryption's reach: the envelope is in the JSON body on every provider, but only these expose it over ADO |
| `ShinyDocDbMyAdmin.Core` has a **ProjectReference** to `Shiny.DocumentDb` | its `.csproj` | A new public helper in core is usable immediately, no package round trip |

## Non-goals

- **Not encrypting on write.** The admin will never *create* an envelope. Doing so needs the type's mapping
  (which property, which mode) and the key ring, and getting either wrong produces a document the application
  cannot read. The admin's job is to not destroy what is there and to say so loudly when it would.
- **No mode discovery beyond what the data proves.** See the inference rule below — the admin reports
  "deterministic (observed)" or "mode unknown", never "randomized".
- **No key management.** No generating, rotating, retiring, or storing the application's keys as a service.
  Phase 5 stores key material for *reading*, and says so in those words.
- **No `RewrapAsync` button.** Rewrapping is a library call needing the mapping and the key ring in-process;
  a `dotnet` one-liner is the right tool. The admin tells you *whether you need to run it* — that is the part
  it is uniquely able to do.
- **Nothing in the AI lane.** See the locked decision below.

---

## Design decisions (locked)

| Decision | Choice | Why |
|---|---|---|
| Envelope shape test | New **public** `DocumentEncryptionFormat` in `Shiny.DocumentDb`; the existing internal `EncryptionEnvelope` delegates to it | The envelope is already a documented on-disk contract that backups, replicas and operators all see. A second copy of the format in the admin would drift, and anyone writing their own tooling hits the identical problem. Considered and rejected: `InternalsVisibleTo` (the pattern used for the AI `Where` filter) — that seam is for a query interpreter, not a storage format |
| Where detection lives | `ShinyDocDbMyAdmin.Core` only; the front ends receive text/flags | Same rule `JsonDisplay` already documents — "this decides only the text". Keeps web and TUI honest with each other |
| Mode inference | Duplicate ciphertext across the sample ⇒ **deterministic, certain**. Absence of duplicates ⇒ **unknown**, never "randomized" | Randomized mode can never repeat a ciphertext, so a repeat is proof. The converse is not proof — a deterministic column of unique values looks identical to a randomized one |
| Key inventory query | Per (path, keyId) `COUNT(*)` with `LIKE 'enc:1:<keyId>:%'` over `provider.JsonExtract("Data", path)`, plus a plaintext count and an **`Other` bucket** = total − known − plaintext | Portable across all 9 dialects using helpers that already exist (`ColumnExpression`, `EscapeLikePattern`). A portable `SUBSTRING` over a JSON extract is not worth writing 9 times. The `Other` bucket self-corrects when the 200-doc sample missed a key |
| Where the inventory surfaces | An **Encryption card on the Structure tab**, not a new tab | It is per-type metadata about three or four fields. A tab implies an interaction surface that does not exist |
| Editor protection | Save-time diff + confirm, plus a banner listing encrypted paths — **not** a locked editor region | The editor is a raw JSON textarea; there is no substring to lock. Guard the operation, not the keystrokes |
| Downgrade default | `SaveDocument` **throws** unless the caller passes `allowEncryptionDowngrade: true` | The silent-cleartext failure mode is the single most damaging thing the admin can do to an encrypted store |
| AI assistant | The tool surface **never** decrypts, whatever keys are configured | Decrypted values would be posted to a third-party model endpoint. Ciphertext reaching a model is harmless; plaintext SSNs are a breach. Non-negotiable, and stated in the docs |
| Decrypt lane (Phase 5) | Opt-in per profile, off by default, **never** in demo mode, masked until revealed | It puts data keys next to connection strings. That is a real trade an operator may want; it is not a default |

---

## Phase 0 — the format helper (core, ~40 lines)

`src/Shiny.DocumentDb/Encryption/DocumentEncryptionFormat.cs`:

```csharp
namespace Shiny.DocumentDb;

/// <summary>What an encryption envelope says about itself, without any key.</summary>
public readonly record struct EncryptedValueInfo(int Version, string KeyId, int PayloadLength);

/// <summary>
/// The stored form of an encrypted value — <c>enc:&lt;version&gt;:&lt;keyId&gt;:&lt;base64&gt;</c> — as a
/// read-only contract, for tooling that inspects stored documents without the key ring.
/// </summary>
public static class DocumentEncryptionFormat
{
    public const string Prefix = "enc:";

    public static bool IsEnvelope(string? value);
    public static bool TryParse(string? value, out EncryptedValueInfo info);

    /// <summary>
    /// Renders decrypted payload bytes for display. Every value codec encodes as UTF-8 text, so a caller
    /// that decrypted an envelope can show the value without knowing the property's CLR type.
    /// </summary>
    public static bool TryRenderPlaintext(ReadOnlySpan<byte> plaintext, out string text);
}
```

`EncryptionEnvelope.IsEnvelope`/`Parse` become thin forwards so there is exactly one implementation of the
shape test. `Parse`'s payload-returning form stays internal — nothing outside the assembly should be handed
raw ciphertext bytes by the library.

## Phase 1 — recognise and render (no keys required)

**Core.** `Services/EncryptedFields.cs`:

```csharp
public static class EncryptedFields
{
    /// <summary>True when this JSON value is an encryption envelope. String values only.</summary>
    public static bool TryRead(JsonNode? node, out EncryptedValueInfo info);

    /// <summary>The encrypted paths in a body, dotted, one level deep like the Structure walker.</summary>
    public static IReadOnlyList<string> PathsIn(JsonNode? body);
}
```

`JsonDisplay` gains `TryEncryptedSummary(JsonNode?, out string)` next to `TryVectorSummary`, and `Cell`
checks it first. Text form: `encrypted · key k1` — no emoji in Core, because the TUI cannot promise a glyph
renders. Each front end decorates.

**Web.** `JsonHtml.Render` emits `<span class="enc" title="AES-GCM envelope · key k1 · not decrypted">🔒
encrypted (k1)</span>` with a per-value **Show ciphertext** toggle — an operator sometimes genuinely needs
the base64 (to paste a deterministic ciphertext into a filter, or to compare two rows). `BrowseTab` grid
cells go through `JsonDisplay.Cell` and inherit it.

**TUI.** `BrowsePanel` / `StructurePanel` render the same Core text, styled as a dim/accent cell.

## Phase 2 — Structure card and key inventory

`InferredField` gains `EncryptedFieldInfo? Encryption`:

```csharp
public sealed record EncryptedFieldInfo(
    IReadOnlyList<string> KeyIds,     // key ids seen in the sample, first-seen order
    int EncryptedCount,
    int PlaintextCount,               // values on this path that are NOT envelopes
    bool DeterministicObserved        // a ciphertext repeated ⇒ deterministic, proven
);
```

`FieldAccumulator.Observe` classifies string values as it already classifies kinds; `TypeSummary` reports
`encrypted` rather than `string`, which is the honest description of what is in the column.

New `Services/DocumentAdminService.Encryption.cs` — the inventory, run on an explicit action (it is `N+2`
counts over the whole type, not a page):

```csharp
public Task<EncryptionInventory> DescribeEncryption(
    string profileId, string table, string typeName, CancellationToken ct = default);

public sealed record EncryptedPathStats(
    string Path,
    long Total,
    long Plaintext,
    IReadOnlyDictionary<string, long> ByKeyId,
    long OtherKeys)
{
    public bool RotationComplete => this.ByKeyId.Count <= 1 && this.OtherKeys == 0 && this.Plaintext == 0;
}

public sealed record EncryptionInventory(string TypeName, IReadOnlyList<EncryptedPathStats> Paths);
```

The card states the operational verdict in a sentence, because that is the whole value:

- `ssn — 4,812 values, all under k2. Nothing left under an older key.`
- `ssn — 620 of 4,812 still under k1. Run RewrapAsync<T>() before retiring k1.`
- `email — 44 values are not encrypted. They were written before the property was mapped; RewrapAsync
  converts them.`
- `notes — 12 values under a key not seen in the sample. Widen the sample or check the key ring.`

This is the one thing only the admin can answer: the library gives you `RewrapAsync`, but nothing tells you
whether it finished, and retiring a key early makes documents unreadable.

## Phase 3 — write guardrails

`SaveDocument(..., bool allowEncryptionDowngrade = false)`:

1. On update, it already reads nothing — add a fetch of the stored body (one row, by id, already-written
   `GetDocument` path).
2. Diff by path: a path that held an envelope and whose submitted value is **not** an envelope is a
   downgrade. A byte-identical envelope is untouched and fine.
3. Throw `EncryptedFieldDowngradeException(IReadOnlyList<string> Paths)` unless the flag is set.

Both front ends catch it and confirm with the paths named, in these words: *"`ssn` would be saved in clear
text. The application will read it back as plaintext and the value is no longer protected."* The document
editor also shows a banner listing the type's encrypted paths **before** anyone starts typing.

`ImportExportService`: an import that writes plaintext into an encrypted path is the same downgrade, but
blocking a bulk import on it is wrong — count them and report `N documents wrote clear text into an
encrypted field` in the import summary. Export is already correct (the envelope exports verbatim); the export
UI should say so — *"encrypted fields export as ciphertext and are readable only with the key ring"* — because
an operator's mental model of "export" is "readable file".

## Phase 4 — stop offering query surfaces that cannot work

- `SearchableColumns` **excludes** encrypted paths. A `LIKE` over ciphertext is guaranteed noise, and a quick
  search that silently returns nothing reads as "no data".
- `DefaultColumns` excludes them too (a locked column of `encrypted` teaches nothing), but they stay
  selectable — seeing *that* a field is protected is worth a column when you want it.
- **Filter console**: after `FilterPaths.Extract`, cross-reference the type's encrypted paths and warn
  inline. Two messages, because the two modes fail differently:
  - unknown/randomized — *"`ssn` is encrypted; a predicate over it cannot match. The console compiles to SQL
    directly, so it does not encrypt your constant the way the library's LINQ path does."*
  - deterministic (observed) — *"`email` is deterministically encrypted: only exact-ciphertext equality can
    match. Copy the ciphertext from a row."*
- **Index tab**: creating a JSON index on a randomized path is dead weight — warn, do not block (a
  deterministic path is legitimately indexable, and the admin cannot always tell which it is).
- **AI assistant** (`AiToolSurface`): no change to behaviour, but the locked rule gets a comment and a docs
  line — the tool surface reads what is stored and never decrypts, whatever Phase 5 is configured with. The
  existing `AiDataWarning` copy should mention that encrypted fields reach the model as ciphertext.

## Phase 5 — decrypt for display (optional, off by default)

Only worth building because `ValueCodec` encodes everything as UTF-8 text: decryption needs the key, not the
CLR type. Without that finding this phase would be guesswork about numeric encodings.

```csharp
// ConnectionProfile
/// <summary>Read keys for field-level encryption. Ciphertext; use IProfileStore.Reveal.</summary>
public List<EncryptionKeyEntry> EncryptionKeys { get; set; } = [];

public sealed class EncryptionKeyEntry
{
    public string KeyId { get; set; } = "";
    public string Key { get; set; } = "";     // base64, SecretProtector-wrapped at rest
}
```

Stored and revealed exactly like `ConnectionString`/`Password` — same `SecretProtector`, same lifecycle,
same delete semantics. Decryption uses the library's own `AesGcmDocumentEncryptor` (key ring constructor)
plus `DocumentEncryptionFormat.TryRenderPlaintext`.

Rules, all enforced in code rather than documented:

- **Masked by default.** A decryptable value renders as `••••• (k1)` with a per-value reveal, never
  auto-expanded. Grids never decrypt — only the document view and an explicit reveal do.
- **Never in demo mode.** `DemoMode` closes it like every other configuration surface.
- **Never in the AI lane**, per Phase 4.
- **Never in export.** An export decides what leaves the building; it stays ciphertext.
- A failed decrypt is reported precisely — *wrong key* (`CryptographicException`, the value was tampered
  with or the key is not the one it was written under) vs *key not held* (`InvalidOperationException` naming
  the key id, which the library already words well).
- The connection editor states the trade in one line: *"These keys let this tool read protected values. They
  are stored beside the connection string and are only as safe as this installation."*

---

## Tests

Admin logic lives in `ShinyDocDbMyAdmin.Core` with no test project today, so this adds
`tests/ShinyDocDbMyAdmin.Core.Tests` (SQLite only — everything here is dialect-independent except the
inventory SQL, which uses existing per-provider helpers).

- **Format**: `enc:v1:…` (the `SecretProtector` prefix) is **not** an envelope; `enc:1:k1:<b64>` is; a
  document value that legitimately starts with `enc:` and is not an envelope round-trips untouched.
- **Detection**: `PathsIn` finds nested and top-level encrypted paths; a numeric array is still a vector
  summary, not an envelope.
- **Mode inference**: two documents sharing a ciphertext ⇒ `DeterministicObserved`; all-unique ⇒ not set
  (and specifically *not* reported as randomized).
- **Inventory**: seed a table with a mix of `k1`, `k2` and plaintext values on one path; assert per-key
  counts, the plaintext count, and that a key absent from the sample lands in `OtherKeys`. Run it against at
  least SQLite + PostgreSQL to prove the `LIKE`-over-`JsonExtract` form is portable.
- **Downgrade guard**: saving a body with the envelope replaced by plaintext throws; with the flag it
  writes and is reported; an untouched envelope saves cleanly; a *new* document with plaintext in a path that
  is encrypted elsewhere is a downgrade too.
- **Round trip against the library** (the test that matters): write documents through a real `DocumentStore`
  with `cfg.MapProperty(…, p => p.Encrypt(…))`, then read them through `DocumentAdminService` and assert the admin's key ids,
  path list and counts match what the store actually wrote. Same shape as the vector/temporal sidecar tests.
- **Phase 5**: correct key decrypts to the original plaintext for a string, a `Guid`, a `DateTime` and an
  `int` property (the UTF-8 claim, proven per codec); wrong key reports tampering; missing key reports the
  key id; demo mode refuses.

Full suite + Docker as always (`CLAUDE.md`) — the inventory test needs PostgreSQL via Testcontainers.

## Four-artifact checklist

- **Code + tests** — as above; new test project into `DocumentDb.slnx` and `build.slnf`.
- **Docs** (`~/Desktop/dev/documentation/src/content/docs/documentdb/`):
  - `admin/` — a new **Encrypted fields** page: what the badge means, reading the rotation card, the
    downgrade confirmation, why filters warn, and Phase 5's trade stated plainly. Screenshots per
    `reference_blog_screenshots` / the scripted capture setup (the seed has no encrypted data — extend it).
  - `encryption.mdx` — an "Inspecting encrypted data" section pointing at the admin, and the explicit line
    that the AI assistant never decrypts.
  - Release note under the `13.0` section, `type="feature"` (the core `DocumentEncryptionFormat` addition is
    additive — no `breaking` note needed).
- **Skill** (`skills/shiny-documentdb/SKILL.md`) — one line in the encryption section: the admin reads
  envelopes and reports key coverage; `triggers:` += `DocumentEncryptionFormat`.
- **readme.md** — the admin bullet mentions encrypted-field awareness.

## Risks

- **The downgrade guard is the whole point.** If Phase 3 ships without Phases 1–2 the tool is prettier; if
  Phases 1–2 ship without Phase 3 the tool is still capable of silently unprotecting data. Phase 3 is not
  optional and should not be deferred for scope.
- **Inventory cost on a large type.** `N+2` counts, each a full scan on a path with no index. Make it an
  explicit action with a spinner, never something the Structure tab runs on load, and say the row count in
  the button.
- **Mode inference over-claiming.** The temptation to print "randomized" is strong and it is unprovable.
  Reviewers should treat any wording that asserts randomized mode as a bug.
- **Phase 5 becomes the headline.** "The admin can decrypt" is a more exciting sentence than "the admin
  reports key coverage", and it is the smaller half of the value. Ship 1–4 first, and keep the docs ordered
  the same way.
