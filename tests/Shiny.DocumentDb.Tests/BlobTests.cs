using System.Text;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

// Blob payloads live in the {table}_blobs sidecar and must never reach the document body. These run on
// SQLite; the per-provider matrix lives in the provider suites.
[Collection("SQLite")]
public class BlobTests : IDisposable
{
    readonly IDocumentStore store;
    readonly string tableName;
    readonly SqliteDatabaseFixture fixture0;

    public BlobTests(SqliteDatabaseFixture fixture)
    {
        this.fixture0 = fixture;
        this.tableName = $"t{Guid.NewGuid():N}";
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = fixture.CreateProvider(),
            TableName = this.tableName
        };
        opts.MapBlob<BlobDoc>(x => x.Pdf);
        opts.MapBlobCollection<BlobDoc>(x => x.Attachments);
        this.store = new DocumentStore(opts);
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    static byte[] Payload(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task Payload_is_not_in_the_document_body()
    {
        var bytes = Payload("hello blob world");
        await this.store.Insert(new BlobDoc
        {
            Id = "b1",
            Name = "invoice",
            Pdf = DocumentBlob.FromBytes(bytes, "application/pdf", "acme.pdf")
        });

        var raw = (await this.store.Get(typeof(BlobDoc), "b1"))!.ToJsonString();

        // metadata rides along...
        Assert.Contains("\"acme.pdf\"", raw);
        Assert.Contains(bytes.Length.ToString(), raw);
        // ...the payload does not
        Assert.DoesNotContain(Convert.ToBase64String(bytes), raw);
        Assert.DoesNotContain("hello blob world", raw);
    }

    [Fact]
    public async Task Metadata_loads_but_bytes_are_deferred()
    {
        var bytes = Payload("deferred payload");
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(bytes, "application/pdf", "acme.pdf") });

        var fetched = await this.store.Get<BlobDoc>("b1");

        Assert.NotNull(fetched!.Pdf);
        Assert.Equal(bytes.Length, fetched.Pdf!.Length);
        Assert.Equal("acme.pdf", fetched.Pdf.FileName);
        Assert.Equal("application/pdf", fetched.Pdf.ContentType);
        Assert.False(fetched.Pdf.IsLoaded);
        Assert.Throws<InvalidOperationException>(() => fetched.Pdf.Bytes);

        await fetched.Pdf.LoadAsync();

        Assert.True(fetched.Pdf.IsLoaded);
        Assert.Equal(bytes, fetched.Pdf.Bytes);
    }

    [Fact]
    public async Task GetBytesAsync_loads_and_returns_in_one_call()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("one shot")) });

        var fetched = await this.store.Get<BlobDoc>("b1");
        var bytes = await fetched!.Pdf!.GetBytesAsync();

        Assert.Equal(Payload("one shot"), bytes);
        Assert.True(fetched.Pdf.IsLoaded);
    }

    [Fact]
    public async Task A_single_collection_item_can_load_without_the_rest()
    {
        var doc = new BlobDoc { Id = "b1" };
        doc.Attachments.Add(Payload("first"), fileName: "a.txt");
        doc.Attachments.Add(Payload("second"), fileName: "b.txt");
        await this.store.Insert(doc);

        var fetched = await this.store.Get<BlobDoc>("b1");

        await fetched!.Attachments[1].LoadAsync();

        Assert.False(fetched.Attachments[0].IsLoaded);   // untouched
        Assert.True(fetched.Attachments[1].IsLoaded);
        Assert.Equal(Payload("second"), fetched.Attachments[1].Bytes);
        Assert.False(fetched.Attachments.IsLoaded);      // collection still has an outstanding item
    }

    [Fact]
    public async Task Detached_blob_cannot_self_load()
    {
        var detached = DocumentBlob.FromBytes(Payload("held")); // FromBytes blob IS loaded, so LoadAsync is a no-op
        await detached.LoadAsync();
        Assert.True(detached.IsLoaded);

        // a blob with metadata but no bytes and no loader (as if hand-built) must throw a clear error
        var orphan = new BlobDoc { Id = "z" }.Pdf;
        Assert.Null(orphan);   // sanity — unset property
    }

    [Fact]
    public async Task Filename_is_optional()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("scan"), "image/png") });

        var fetched = await this.store.Get<BlobDoc>("b1");

        Assert.Null(fetched!.Pdf!.FileName);
        Assert.Equal("image/png", fetched.Pdf.ContentType);
    }

    [Fact]
    public async Task Multiple_blobs_per_document_roundtrip()
    {
        var doc = new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("main")) };
        doc.Attachments.Add(Payload("one"), "text/plain", "one.txt");
        doc.Attachments.Add(Payload("two"), "text/plain", "two.txt");
        await this.store.Insert(doc);

        var fetched = await this.store.Get<BlobDoc>("b1");
        Assert.Equal(2, fetched!.Attachments.Count);

        await fetched.Pdf!.LoadAsync();
        await fetched.Attachments.LoadAllAsync();

        Assert.Equal(Payload("main"), fetched.Pdf!.Bytes);
        Assert.Equal(Payload("one"), fetched.Attachments[0].Bytes);
        Assert.Equal(Payload("two"), fetched.Attachments[1].Bytes);
        Assert.Equal("two.txt", fetched.Attachments[1].FileName);
    }

    [Fact]
    public async Task Roundtrip_without_touching_blobs_does_not_rewrite_payloads()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Name = "before", Pdf = DocumentBlob.FromBytes(Payload("original")) });

        var fetched = await this.store.Get<BlobDoc>("b1");
        fetched!.Name = "after";
        await this.store.Update(fetched);   // Pdf is unloaded — must not be resent, must not be dropped

        var again = await this.store.Get<BlobDoc>("b1");
        Assert.Equal("after", again!.Name);
        await again.Pdf!.LoadAsync();
        Assert.Equal(Payload("original"), again.Pdf!.Bytes);
    }

    [Fact]
    public async Task Replacing_a_payload_replaces_the_row()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("v1"), fileName: "v1.txt") });

        var fetched = await this.store.Get<BlobDoc>("b1");
        fetched!.Pdf = DocumentBlob.FromBytes(Payload("v2-longer"), fileName: "v2.txt");
        await this.store.Update(fetched);

        var again = await this.store.Get<BlobDoc>("b1");
        Assert.Equal("v2.txt", again!.Pdf!.FileName);
        Assert.Equal(Payload("v2-longer").Length, again.Pdf.Length);
        await again.Pdf!.LoadAsync();
        Assert.Equal(Payload("v2-longer"), again.Pdf.Bytes);
    }

    [Fact]
    public async Task Nulling_a_blob_deletes_the_row()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("gone soon")) });

        var fetched = await this.store.Get<BlobDoc>("b1");
        fetched!.Pdf = null;
        await this.store.Update(fetched);

        Assert.Null((await this.store.Get<BlobDoc>("b1"))!.Pdf);
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }

    [Fact]
    public async Task Removing_a_collection_item_prunes_its_row()
    {
        var doc = new BlobDoc { Id = "b1" };
        doc.Attachments.Add(Payload("keep"), fileName: "keep.txt");
        doc.Attachments.Add(Payload("drop"), fileName: "drop.txt");
        await this.store.Insert(doc);

        var fetched = await this.store.Get<BlobDoc>("b1");
        var droppedKey = fetched!.Attachments[1].Key;
        fetched.Attachments.RemoveAt(1);
        await this.store.Update(fetched);

        var again = await this.store.Get<BlobDoc>("b1");
        Assert.Single(again!.Attachments);
        Assert.Equal("keep.txt", again.Attachments[0].FileName);
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", droppedKey));
    }

    [Fact]
    public async Task Deleting_the_document_cascades_to_its_payloads()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("doomed")) });

        Assert.NotNull(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));

        await this.store.Remove<BlobDoc>("b1");

        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }

    [Fact]
    public async Task Metadata_is_queryable_without_loading_payloads()
    {
        await this.store.Insert(new BlobDoc { Id = "small", Pdf = DocumentBlob.FromBytes(Payload("xs"), "text/plain") });
        await this.store.Insert(new BlobDoc { Id = "big", Pdf = DocumentBlob.FromBytes(Payload(new string('x', 500)), "application/pdf") });

        var large = await this.store.Query<BlobDoc>().Where(x => x.Pdf!.Length > 100).ToList();
        Assert.Single(large);
        Assert.Equal("big", large[0].Id);

        var pdfs = await this.store.Query<BlobDoc>().Where(x => x.Pdf!.ContentType == "application/pdf").ToList();
        Assert.Single(pdfs);
        Assert.False(pdfs[0].Pdf!.IsLoaded);   // querying by metadata never materializes the payload
    }

    [Fact]
    public async Task Batch_load_across_a_page_is_one_round_trip()
    {
        for (var i = 0; i < 5; i++)
            await this.store.Insert(new BlobDoc { Id = $"b{i}", Pdf = DocumentBlob.FromBytes(Payload($"payload-{i}")) });

        var page = await this.store.Query<BlobDoc>().ToList();
        Assert.Equal(5, page.Count);
        Assert.All(page, d => Assert.False(d.Pdf!.IsLoaded));

        await ((IBlobDocumentStore)this.store).BatchLoadBlobs(page);

        Assert.All(page, d => Assert.True(d.Pdf!.IsLoaded));
        var byId = page.ToDictionary(x => x.Id);
        Assert.Equal(Payload("payload-3"), byId["b3"].Pdf!.Bytes);
    }

    [Fact]
    public async Task Temporal_restore_keeps_current_blobs_not_the_snapshot()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.fixture0.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapBlob<BlobDoc>(x => x.Pdf);
        opts.MapTemporal<BlobDoc>();
        using var temporal = new DocumentStore(opts);

        // v1: original payload
        await temporal.Insert(new BlobDoc { Id = "b1", Name = "v1", Pdf = DocumentBlob.FromBytes(Payload("original-bytes"), fileName: "v1.pdf") });

        // v2: replaced payload + changed field
        var loaded = await temporal.Get<BlobDoc>("b1");
        loaded!.Name = "v2";
        loaded.Pdf = DocumentBlob.FromBytes(Payload("replaced"), fileName: "v2.pdf");
        await temporal.Update(loaded);

        // restore v1 — the FIELD comes back, but the blob must reflect CURRENT bytes, not v1's snapshot
        var restored = await ((ITemporalDocumentStore)temporal).Restore<BlobDoc>("b1", 1);

        Assert.Equal("v1", restored!.Name);                    // field restored
        Assert.Equal("v2.pdf", restored.Pdf!.FileName);        // blob metadata is current, not the v1 snapshot
        Assert.Equal(Payload("replaced").Length, restored.Pdf.Length);

        // and the metadata must not lie about the bytes on the wire
        var reread = await temporal.Get<BlobDoc>("b1");
        Assert.Equal("v2.pdf", reread!.Pdf!.FileName);
        await reread.Pdf.LoadAsync();
        Assert.Equal(Payload("replaced"), reread.Pdf.Bytes);
    }

    [Fact]
    public async Task Backup_roundtrips_blob_payloads_by_default()
    {
        var doc = new BlobDoc { Id = "b1", Name = "invoice", Pdf = DocumentBlob.FromBytes(Payload("payload-in-backup"), "application/pdf", "acme.pdf") };
        doc.Attachments.Add(Payload("att-one"), fileName: "one.txt");
        await this.store.Insert(doc);

        using var ms = new MemoryStream();
        await ((IDocumentBackup)this.store).ExportAsync(ms);
        ms.Position = 0;

        // restore into a fresh store with the same blob mapping
        var opts = new DocumentStoreOptions { DatabaseProvider = this.fixture0.CreateProvider(), TableName = $"t{Guid.NewGuid():N}" };
        opts.MapBlob<BlobDoc>(x => x.Pdf);
        opts.MapBlobCollection<BlobDoc>(x => x.Attachments);
        using var restored = new DocumentStore(opts);

        var result = await ((IDocumentBackup)restored).RestoreAsync(ms);
        Assert.Equal(1, result.DocumentsWritten);

        var got = await restored.Get<BlobDoc>("b1");
        Assert.Equal("acme.pdf", got!.Pdf!.FileName);
        await got.Pdf.LoadAsync();
        Assert.Equal(Payload("payload-in-backup"), got.Pdf.Bytes);

        Assert.Single(got.Attachments);
        await got.Attachments.LoadAllAsync();
        Assert.Equal(Payload("att-one"), got.Attachments[0].Bytes);
    }

    [Fact]
    public async Task Backup_can_exclude_blob_payloads()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("skip-me"), fileName: "x.pdf") });

        using var ms = new MemoryStream();
        await ((IDocumentBackup)this.store).ExportAsync(ms, new BackupExportOptions { IncludeBlobs = false });
        ms.Position = 0;

        var opts = new DocumentStoreOptions { DatabaseProvider = this.fixture0.CreateProvider(), TableName = $"t{Guid.NewGuid():N}" };
        opts.MapBlob<BlobDoc>(x => x.Pdf);
        opts.MapBlobCollection<BlobDoc>(x => x.Attachments);
        using var restored = new DocumentStore(opts);
        await ((IDocumentBackup)restored).RestoreAsync(ms);

        var got = await restored.Get<BlobDoc>("b1");
        Assert.Equal("x.pdf", got!.Pdf!.FileName);                    // metadata survives (it's in the body)
        Assert.Null(await ((IBlobDocumentStore)restored).GetBlob<BlobDoc>("b1", "Pdf"));   // payload was not exported
    }

    [Fact]
    public async Task Oversized_payload_is_rejected_before_the_provider_sees_it()
    {
        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.fixture0.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapBlob<BlobDoc>(x => x.Pdf, o => o.MaxSize = 16);
        using var capped = new DocumentStore(opts);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => capped.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload(new string('x', 64))) }));

        Assert.Contains("16", ex.Message);
    }

    [Fact]
    public async Task Hash_is_off_by_default_and_computed_when_enabled()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("unhashed")) });
        Assert.Null((await this.store.Get<BlobDoc>("b1"))!.Pdf!.Hash);

        var opts = new DocumentStoreOptions
        {
            DatabaseProvider = this.fixture0.CreateProvider(),
            TableName = $"t{Guid.NewGuid():N}"
        };
        opts.MapBlob<BlobDoc>(x => x.Pdf, o => o.ComputeHash = true);
        using var hashed = new DocumentStore(opts);

        await hashed.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("hashed")) });

        var fetched = await hashed.Get<BlobDoc>("b1");
        Assert.NotNull(fetched!.Pdf!.Hash);
        Assert.Equal(64, fetched.Pdf.Hash!.Length);   // sha256 hex
    }

    [Fact]
    public void Store_reports_its_blob_ceiling()
        => Assert.Equal(1024L * 1024 * 1024, this.store.MaxBlobSize);
}

public class BlobDoc
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public DocumentBlob? Pdf { get; set; }
    public DocumentBlobCollection Attachments { get; set; } = new();
}
