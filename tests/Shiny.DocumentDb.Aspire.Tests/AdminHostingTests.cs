using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Shiny.DocumentDb.Aspire.Hosting;

namespace Shiny.DocumentDb.Aspire.Tests;

/// <summary>
/// Model-only coverage of <c>AddDocumentDbAdmin</c>: nothing here pulls an image or starts a
/// container, it asserts on the resource graph the AppHost would hand to Aspire.
/// </summary>
public class AdminHostingTests
{
    static IDistributedApplicationBuilder CreateRunBuilder()
        => DistributedApplication.CreateBuilder();

    static IDistributedApplicationBuilder CreatePublishBuilder()
        => DistributedApplication.CreateBuilder(["--operation", "publish", "--publisher", "manifest", "--output-path", "."]);

    static EndpointAnnotation HttpEndpoint(IResource resource)
        => resource.Annotations.OfType<EndpointAnnotation>().Single(x => x.Name == "http");

    [Fact]
    public void IsAContainerOnTheAdminImage()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin();

        Assert.IsType<DocumentDbAdminResource>(admin.Resource);
        Assert.Equal("documentdb-admin", admin.Resource.Name);

        var image = admin.Resource.Annotations.OfType<ContainerImageAnnotation>().Single();
        Assert.Equal(DocumentDbAdminResourceBuilderExtensions.ContainerRegistry, image.Registry);
        Assert.Equal(DocumentDbAdminResourceBuilderExtensions.ContainerImage, image.Image);
        Assert.False(string.IsNullOrWhiteSpace(image.Tag));
    }

    [Fact]
    public void RunAndPublishAreTheSameImage()
    {
        // One shape either way - there is no separate local build of this thing to drift from.
        var run = CreateRunBuilder();
        var publish = CreatePublishBuilder();
        Assert.True(publish.ExecutionContext.IsPublishMode);

        var fromRun = run.AddDocumentDbAdmin().Resource.Annotations.OfType<ContainerImageAnnotation>().Single();
        var fromPublish = publish.AddDocumentDbAdmin().Resource.Annotations.OfType<ContainerImageAnnotation>().Single();

        Assert.Equal(fromRun.Registry, fromPublish.Registry);
        Assert.Equal(fromRun.Image, fromPublish.Image);
        Assert.Equal(fromRun.Tag, fromPublish.Tag);
    }

    [Fact]
    public void TargetsThePortTheImageListensOn()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin();

        Assert.Equal(DocumentDbAdminResourceBuilderExtensions.ContainerPort, HttpEndpoint(admin.Resource).TargetPort);
    }

    [Fact]
    public void Port_IsTheHostPortYouBrowseTo()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin(port: 8085);

        Assert.Equal(8085, HttpEndpoint(admin.Resource).Port);
    }

    [Fact]
    public void WithHostPort_SetsThePortAfterTheFact()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin().WithHostPort(9111);

        Assert.Equal(9111, HttpEndpoint(admin.Resource).Port);
    }

    [Fact]
    public void WithHostPort_Null_HandsThePortBackToAspire()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin(port: 8085).WithHostPort(null);

        Assert.Null(HttpEndpoint(admin.Resource).Port);
    }

    [Fact]
    public async Task WithReference_HandsTheAdminAConnectedStore()
    {
        var builder = CreateRunBuilder();
        var store = builder.AddSqliteDocumentStore("orders", "/data/orders.db");

        var admin = builder.AddDocumentDbAdmin().WithReference(store);

#pragma warning disable CS0618 // model-only resolution, as elsewhere in these tests
        var env = await ((IResourceWithEnvironment)admin.Resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

        // Exactly the pair ShinyDocDbMyAdmin's ProvidedConnections reads: without both, the store
        // is silently ignored rather than shown as a connection. In Publish the connection string is
        // still a manifest expression, so assert it points at the store rather than at its value.
        Assert.True(env.ContainsKey("ConnectionStrings__orders"));
        Assert.Contains("orders", env["ConnectionStrings__orders"]);
        Assert.Equal("Sqlite", env["Shiny__DocumentDb__orders__Provider"]);
    }

    [Fact]
    public async Task WithReadOnly_BlocksWritesAcrossEveryStore()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin().WithReadOnly();

#pragma warning disable CS0618
        var env = await ((IResourceWithEnvironment)admin.Resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

        Assert.Equal("true", env["ShinyDocDbMyAdmin__ReadOnly"]);
    }

    [Fact]
    public async Task WithReadOnly_False_IsExplicitlyWritable()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin().WithReadOnly(false);

#pragma warning disable CS0618
        var env = await ((IResourceWithEnvironment)admin.Resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

        Assert.Equal("false", env["ShinyDocDbMyAdmin__ReadOnly"]);
    }

    [Fact]
    public async Task WithSecretKey_ReachesTheImageAsConfiguration()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin().WithSecretKey("a-passphrase");

#pragma warning disable CS0618
        var env = await ((IResourceWithEnvironment)admin.Resource)
            .GetEnvironmentVariableValuesAsync(DistributedApplicationOperation.Publish);
#pragma warning restore CS0618

        Assert.Equal("a-passphrase", env["ShinyDocDbMyAdmin__SecretKey"]);
    }

    [Fact]
    public void WithDataVolume_MountsOnTheImagesDataPath()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin().WithDataVolume();

        var mount = admin.Resource.Annotations.OfType<ContainerMountAnnotation>().Single();
        Assert.Equal(ContainerMountType.Volume, mount.Type);
        Assert.Equal("documentdb-admin-data", mount.Source);
        Assert.Equal(DocumentDbAdminResourceBuilderExtensions.DataPath, mount.Target);
    }

    [Fact]
    public void WithDataVolume_TakesAName()
    {
        var builder = CreateRunBuilder();

        var admin = builder.AddDocumentDbAdmin().WithDataVolume("my-admin-state");

        Assert.Equal("my-admin-state", admin.Resource.Annotations.OfType<ContainerMountAnnotation>().Single().Source);
    }

    [Fact]
    public void WithHostPath_IsHowAFileBackedStoreBecomesReachable()
    {
        var builder = CreateRunBuilder();

        // A SQLite file on the host is invisible to the container without this.
        var admin = builder.AddDocumentDbAdmin().WithHostPath("/Users/me/databases", "/data", isReadOnly: true);

        var mount = admin.Resource.Annotations.OfType<ContainerMountAnnotation>().Single();
        Assert.Equal(ContainerMountType.BindMount, mount.Type);
        Assert.Equal("/data", mount.Target);
        Assert.True(mount.IsReadOnly);
    }

    [Fact]
    public void Name_IsHonoured()
    {
        var builder = CreateRunBuilder();

        builder.AddDocumentDbAdmin("db-browser");

        Assert.Contains(builder.Resources, r => r.Name == "db-browser" && r is DocumentDbAdminResource);
    }
}
