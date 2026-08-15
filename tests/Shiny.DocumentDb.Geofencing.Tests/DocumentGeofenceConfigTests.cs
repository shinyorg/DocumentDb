using Shiny.Locations;
using Xunit;

namespace Shiny.DocumentDb.Geofencing.Tests;

public class DocumentGeofenceConfigTests
{
    [Fact]
    public void Defaults_Throttle_Readings_And_Track_In_The_Background()
    {
        var config = new DocumentGeofenceConfig();

        Assert.Equal(300, config.MinimumDistance!.TotalMeters);
        Assert.Equal(TimeSpan.FromMinutes(1), config.MinimumTime);
        Assert.Null(config.MaximumDistance);
        Assert.Null(config.MaximumTime);
        Assert.Equal(GpsBackgroundMode.Realtime, config.GpsRequest.BackgroundMode);
        Assert.Empty(config.RegionSets);
    }


    [Fact]
    public void Containment_Is_The_Default_Mode()
    {
        var config = new DocumentGeofenceConfig().AddRegionSet<Zone>("zones", z => z.Id);

        var set = Assert.Single(config.RegionSets);
        Assert.Equal("zones", set.Name);
        Assert.Null(set.WithinMeters);
    }


    [Fact]
    public void Duplicate_Set_Names_Are_Rejected()
    {
        var config = new DocumentGeofenceConfig().AddRegionSet<Zone>("zones", z => z.Id);

        Assert.Throws<ArgumentException>(() => config.AddRegionSet<Shop>("ZONES", s => s.Id));
    }


    [Theory]
    [InlineData(0d)]
    [InlineData(-1d)]
    public void A_Non_Positive_Radius_Is_Rejected(double meters)
        => Assert.Throws<ArgumentOutOfRangeException>(
            () => new DocumentGeofenceConfig().AddRegionSet<Shop>("shops", s => s.Id, withinMeters: meters)
        );


    [Fact]
    public void A_Blank_Set_Name_Is_Rejected()
        => Assert.Throws<ArgumentException>(() => new DocumentGeofenceConfig().AddRegionSet<Zone>(" ", z => z.Id));
}
