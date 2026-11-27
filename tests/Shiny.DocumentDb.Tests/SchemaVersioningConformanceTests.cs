using System.Text.Json.Nodes;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests.Legacy
{
    // The original shape: same short type name as the current class, so it lands in the same table/type name.
    public class UpPerson
    {
        public string Id { get; set; } = "";
        public string FirstName { get; set; } = "";
        public string LastName { get; set; } = "";
        public int Age { get; set; }
    }

    public class UpThreeStep
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }
}

namespace Shiny.DocumentDb.Tests
{
    public class UpPerson
    {
        public string Id { get; set; } = "";
        public string FullName { get; set; } = "";
        public int Age { get; set; }
    }

    // v1 { name }  →  v2 { title }  →  v3 { title, slug }
    public class UpThreeStep
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public string Slug { get; set; } = "";
    }

    public abstract class SchemaVersioningConformanceTestsBase(IDocumentStoreFixture fixture)
    {
        protected readonly IDocumentStoreFixture Fixture = fixture;

        static void Configure(IDocumentStoreOptions o)
        {
            o.ConfigureDocument<UpPerson>(cfg => cfg.AddUpcaster(1, json =>
            {
                json["fullName"] = $"{json["firstName"]} {json["lastName"]}";
                json.Remove("firstName");
                json.Remove("lastName");
            }));
            o.ConfigureDocument<UpThreeStep>(cfg => cfg
                .AddUpcaster(2, json => json["slug"] = json["title"]!.GetValue<string>().ToLowerInvariant().Replace(' ', '-'))
                .AddUpcaster(1, json =>
                {
                    json["title"] = json["name"]!.DeepClone();
                    json.Remove("name");
                }));
        }

        async Task<IDocumentStore> SeededAsync()
        {
            var store = this.Fixture.CreateStore($"v{Guid.NewGuid():N}", Configure);
            await store.Insert(new Legacy.UpPerson { Id = "ada", FirstName = "Ada", LastName = "Lovelace", Age = 36 });
            await store.Insert(new Legacy.UpPerson { Id = "alan", FirstName = "Alan", LastName = "Turing", Age = 41 });
            return store;
        }

        [Fact]
        public async Task Get_UpcastsAnOldDocument()
        {
            var store = await SeededAsync();
            var ada = await store.Get<UpPerson>("ada");

            Assert.Equal("Ada Lovelace", ada!.FullName);
            Assert.Equal(36, ada.Age);
        }

        [Fact]
        public async Task Query_UpcastsEveryOldDocument()
        {
            var store = await SeededAsync();
            var people = await store.Query<UpPerson>().Where(x => x.Age > 30).OrderBy(x => x.Age).ToList();

            Assert.Equal(["Ada Lovelace", "Alan Turing"], people.Select(x => x.FullName));
        }

        [Fact]
        public async Task CurrentDocuments_AreNotUpcastAgain()
        {
            var store = this.Fixture.CreateStore($"v{Guid.NewGuid():N}", Configure);
            await store.Insert(new UpPerson { Id = "grace", FullName = "Grace Hopper", Age = 85 });

            // Were the v1 → v2 step to run again it would rebuild fullName from the missing first/last names.
            Assert.Equal("Grace Hopper", (await store.Get<UpPerson>("grace"))!.FullName);
        }

        [Fact]
        public async Task ChainedSteps_RunInOrder()
        {
            var store = this.Fixture.CreateStore($"v{Guid.NewGuid():N}", Configure);
            await store.Insert(new Legacy.UpThreeStep { Id = "p1", Name = "Hello World" });

            var doc = await store.Get<UpThreeStep>("p1");

            Assert.Equal("Hello World", doc!.Title);
            Assert.Equal("hello-world", doc.Slug);
        }

        [Fact]
        public async Task UpgradeDocuments_RewritesOldDocuments_SoQueriesSeeTheNewShape()
        {
            var store = await SeededAsync();
            await store.Insert(new UpPerson { Id = "grace", FullName = "Grace Hopper", Age = 85 });

            // Before the upgrade a server-side filter sees the stored v1 JSON (no fullName); a provider that filters
            // materialized documents already sees the upcast shape. After it, every provider agrees.
            var upgraded = await store.UpgradeDocuments<UpPerson>(batchSize: 1);

            Assert.Equal(2, upgraded);
            Assert.Equal(1, await store.Query<UpPerson>().Where(x => x.FullName == "Ada Lovelace").Count());
            Assert.Equal(0, await store.UpgradeDocuments<UpPerson>());
            Assert.Equal(3, store.GetSchemaVersion<UpThreeStep>());
        }

        [Fact]
        public async Task UpdatedDocuments_AreStoredAtTheCurrentVersion()
        {
            var store = await SeededAsync();
            var ada = (await store.Get<UpPerson>("ada"))!;
            ada.Age = 37;
            await store.Update(ada);

            Assert.Equal(1, await store.Query<UpPerson>().Where(x => x.FullName == "Ada Lovelace" && x.Age == 37).Count());
        }
    }

    public class SchemaVersioningUnitTests
    {
        public class GapDoc
        {
            public string Id { get; set; } = "";
        }

        public class StampDoc
        {
            public string Id { get; set; } = "";
        }

        [Fact]
        public void StampWrite_AddsTheVersion_ToEmptyAndNonEmptyObjects()
        {
            Internal.DocumentShapes.AddUpcaster(typeof(StampDoc), 1, _ => { });

            Assert.Equal("""{"_v":2}""", Internal.DocumentShapes.StampWrite<StampDoc>("{}"));
            Assert.Equal("""{"id":"a","_v":2}""", Internal.DocumentShapes.StampWrite<StampDoc>("""{"id":"a"}"""));
            Assert.Equal("""{"id":"a","_v":7}""", Internal.DocumentShapes.StampWrite<StampDoc>("""{"id":"a","_v":7}"""));
            Assert.Equal("""{"id":"a"}""", Internal.DocumentShapes.StampWrite<User>("""{"id":"a"}"""));
        }

        [Fact]
        public void TryUpcast_IgnoresNestedVersionProperties()
        {
            Internal.DocumentShapes.AddUpcaster(typeof(StampDoc), 1, _ => { });
            var json = """{"inner":{"_v":5},"id":"a"}""";

            Assert.True(Internal.DocumentShapes.TryUpcast<StampDoc>(ref json));
            Assert.Equal(2, JsonNode.Parse(json)!["_v"]!.GetValue<int>());
        }

        [Fact]
        public void MissingStep_Throws()
        {
            Internal.DocumentShapes.AddUpcaster(typeof(GapDoc), 2, _ => { });
            var json = "{}";
            Assert.Throws<InvalidOperationException>(() => Internal.DocumentShapes.TryUpcast<GapDoc>(ref json));
        }

        [Fact]
        public void VersionBelowOne_Throws()
            => Assert.Throws<ArgumentOutOfRangeException>(() => Internal.DocumentShapes.AddUpcaster(typeof(GapDoc), 0, _ => { }));
    }
}
