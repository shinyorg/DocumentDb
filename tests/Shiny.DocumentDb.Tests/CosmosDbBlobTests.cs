using System.Text;
using Shiny.DocumentDb.CosmosDb;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

[Collection("CosmosDB")]
public class CosmosDbBlobTests : IDisposable
{
    readonly CosmosDbDatabaseFixture db;
    readonly string container;
    readonly CosmosDbDocumentStore store;

    public CosmosDbBlobTests(CosmosDbDatabaseFixture db)
    {
        this.db = db;
        this.container = $"t{Guid.NewGuid():N}";
        this.store = db.CreateConfiguredStore(this.container, o =>
        {
            o.ConfigureDocument<BlobDoc>(cfg =>
            {
                cfg.MapBlob(x => x.Pdf);
                cfg.MapBlobCollection(x => x.Attachments);
            });
        });
    }

    public void Dispose() => (this.store as IDisposable)?.Dispose();

    static byte[] Payload(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public void Store_supports_blobs()
        => Assert.True(((IDocumentStore)this.store).MaxBlobSize > 0 && this.store is IBlobDocumentStore);

    [Fact]
    public async Task Metadata_rides_along_and_bytes_self_load()
    {
        var bytes = Payload("cosmos blob payload");
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(bytes, "application/pdf", "acme.pdf") });

        var fetched = await this.store.Get<BlobDoc>("b1");
        Assert.Equal(bytes.Length, fetched!.Pdf!.Length);
        Assert.Equal("acme.pdf", fetched.Pdf.FileName);
        Assert.False(fetched.Pdf.IsLoaded);

        await fetched.Pdf.LoadAsync();
        Assert.Equal(bytes, fetched.Pdf.Bytes);
        Assert.Equal(bytes, await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }

    [Fact]
    public async Task Collection_loads_all_and_prunes()
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
    public async Task Roundtrip_keeps_payload_replace_and_null()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Name = "before", Pdf = DocumentBlob.FromBytes(Payload("original")) });

        var fetched = await this.store.Get<BlobDoc>("b1");
        fetched!.Name = "after";
        await this.store.Update(fetched);
        var again = await this.store.Get<BlobDoc>("b1");
        Assert.Equal("after", again!.Name);
        await again.Pdf!.LoadAsync();
        Assert.Equal(Payload("original"), again.Pdf.Bytes);

        again.Pdf = null;
        await this.store.Update(again);
        Assert.Null((await this.store.Get<BlobDoc>("b1"))!.Pdf);
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
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
    public async Task Sweep_removes_orphaned_blobs()
    {
        await this.store.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload("keep-me")) });
        await this.store.Insert(new BlobDoc { Id = "orphan", Pdf = DocumentBlob.FromBytes(Payload("orphan-me")) });

        using (var noBlob = this.db.CreateConfiguredStore(this.container, _ => { }))
            await noBlob.Remove<BlobDoc>("orphan");

        Assert.NotNull(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("orphan", "Pdf"));
        var removed = await ((IDocumentMaintenance)this.store).SweepOrphanedBlobs<BlobDoc>();
        Assert.Equal(1, removed);
        Assert.Null(await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("orphan", "Pdf"));
        Assert.Equal(Payload("keep-me"), await ((IBlobDocumentStore)this.store).GetBlob<BlobDoc>("b1", "Pdf"));
    }

    [Fact]
    public async Task Oversized_payload_rejected()
    {
        using var capped = this.db.CreateConfiguredStore($"t{Guid.NewGuid():N}", o => o.ConfigureDocument<BlobDoc>(cfg => cfg.MapBlob(x => x.Pdf, b => b.MaxSize = 8)));
        await Assert.ThrowsAsync<NotSupportedException>(
            () => capped.Insert(new BlobDoc { Id = "b1", Pdf = DocumentBlob.FromBytes(Payload(new string('x', 32))) }));
    }
}
