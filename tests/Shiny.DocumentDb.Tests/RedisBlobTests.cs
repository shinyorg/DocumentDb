using System.Text;
using Shiny.DocumentDb.Redis;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("Redis")]
public class RedisBlobTests : IDisposable
{
    readonly RedisDatabaseFixture db;
    readonly string ns;
    readonly RedisDocumentStore store;

    public RedisBlobTests(RedisDatabaseFixture db)
    {
        this.db = db;
        this.ns = $"t{Guid.NewGuid():N}";
        this.store = db.CreateConfiguredStore(this.ns, o =>
        {
            o.ConfigureDocument<BlobDoc>(cfg =>
            {
                cfg.MapBlob(x => x.Pdf);
                cfg.MapBlobCollection(x => x.Attachments);
            });
        });
    }

    public void Dispose() => this.store.Dispose();

    static byte[] Payload(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public void Store_supports_blobs()
        => Assert.True(((IDocumentStore)this.store).MaxBlobSize > 0 && this.store is IBlobDocumentStore);

    [Fact]
    public async Task Metadata_rides_along_and_bytes_self_load()
    {
        var bytes = Payload("redis blob payload");
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(bytes, "application/pdf", "acme.pdf") });

        var fetched = await this.store.Get<BlobDoc>("b1");
        Assert.Equal(bytes.Length, fetched!.Pdf!.Length);
        Assert.Equal("acme.pdf", fetched.Pdf.FileName);
        Assert.False(fetched.Pdf.IsLoaded);
        Assert.Throws<InvalidOperationException>(() => fetched.Pdf.Bytes);

        await fetched.Pdf.LoadAsync();
        Assert.Equal(bytes, fetched.Pdf.Bytes);
        Assert.Equal(bytes, await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }

    [Fact]
    public async Task Collection_loads_all_and_prunes_on_remove()
    {
        var doc = new BlobDoc { Id = "b1" };
        doc.Attachments.Add(Payload("keep"), fileName: "keep.txt");
        doc.Attachments.Add(Payload("drop"), fileName: "drop.txt");
        await this.store.Insert(doc);

        var fetched = await this.store.Get<BlobDoc>("b1");
        Assert.Equal(2, fetched!.Attachments.Count);
        await fetched.Attachments.LoadAllAsync();
        Assert.Equal(Payload("keep"), fetched.Attachments[0].Bytes);

        var droppedKey = fetched.Attachments[1].Key;
        fetched.Attachments.RemoveAt(1);
        await this.store.Update(fetched);

        Assert.Single((await this.store.Get<BlobDoc>("b1"))!.Attachments);
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", droppedKey));
    }

    [Fact]
    public async Task Roundtrip_keeps_payload_and_replace_works()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Name = "before", Pdf = DocumentBlob.FromBytes(Payload("original")) });

        var fetched = await this.store.Get<BlobDoc>("b1");
        fetched!.Name = "after";
        await this.store.Update(fetched);
        var again = await this.store.Get<BlobDoc>("b1");
        Assert.Equal("after", again!.Name);
        await again.Pdf!.LoadAsync();
        Assert.Equal(Payload("original"), again.Pdf.Bytes);

        again.Pdf = DocumentBlob.FromBytes(Payload("v2!"), fileName: "v2.txt");
        await this.store.Update(again);
        var third = await this.store.Get<BlobDoc>("b1");
        Assert.Equal("v2.txt", third!.Pdf!.FileName);
        await third.Pdf.LoadAsync();
        Assert.Equal(Payload("v2!"), third.Pdf.Bytes);
    }

    [Fact]
    public async Task Deleting_the_document_cascades()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("doomed")) });
        Assert.NotNull(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));

        await this.store.Remove<BlobDoc>("b1");
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }

    [Fact]
    public async Task Batch_load_across_a_page()
    {
        for (var i = 0; i < 4; i++)
            await this.store.Insert(new BlobDoc { Id = $"b{i}", Pdf = DocumentBlob.FromBytes(Payload($"p{i}")) });

        var page = await this.store.Query<BlobDoc>().ToList();
        Assert.All(page, d => Assert.False(d.Pdf!.IsLoaded));

        await ((IBlobDocumentStore)this.store).BatchLoadBlobs(page);
        Assert.All(page, d => Assert.True(d.Pdf!.IsLoaded));
        Assert.Equal(Payload("p2"), page.Single(x => x.Id == "b2").Pdf!.Bytes);
    }

    [Fact]
    public async Task Sweep_removes_orphaned_blobs()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("keep-me")) });
        await this.store.Insert(new BlobDoc { Id = "orphan", Pdf = DocumentBlob.FromBytes(Payload("orphan-me")) });

        // delete the main document through a store with no blob mapping (same namespace) → no cascade → orphan
        using (var noBlob = this.db.CreateConfiguredStore(this.ns, _ => { }))
            await noBlob.Remove<BlobDoc>("orphan");

        Assert.NotNull(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("orphan", "Pdf"));

        var removed = await ((IDocumentMaintenance)this.store).SweepOrphanedBlobs<BlobDoc>();

        Assert.Equal(1, removed);
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("orphan", "Pdf"));
        Assert.Equal(Payload("keep-me"), await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }
}
