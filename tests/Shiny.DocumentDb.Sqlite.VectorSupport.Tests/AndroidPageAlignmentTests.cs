using Xunit;

namespace Shiny.DocumentDb.Sqlite.VectorSupport.Tests;

// Android 15+ can run with 16 KB memory pages, and a .so whose PT_LOAD segments are aligned to a
// smaller page simply refuses to load ("program alignment (4096) cannot be smaller than system page
// size (16384)"). The upstream sqlite-vec release binaries are 4 KB aligned, which is why
// build/build-sqlite-vec.sh compiles the Android ABIs itself with -Wl,-z,max-page-size=16384.
// This asserts the binaries actually sitting in the package layout kept that alignment.
public class AndroidPageAlignmentTests
{
    const ulong RequiredAlignment = 16 * 1024;

    // Only the 64-bit ABIs matter — no 16 KB-page device supports the 32-bit ones.
    [Theory]
    [InlineData("arm64-v8a")]
    [InlineData("x86_64")]
    public void BundledAndroidLib_Is16KPageAligned(string abi)
    {
        var path = FindNative(Path.Combine("android", abi, "libsqlite_vec0.so"));
        if (path == null)
        {
            Assert.Skip("sqlite-vec Android natives not present — run build/build-sqlite-vec.sh.");
            return;
        }

        var aligns = ReadLoadSegmentAlignments(path);
        Assert.NotEmpty(aligns);
        Assert.All(aligns, a => Assert.True(
            a >= RequiredAlignment,
            $"{abi}/libsqlite_vec0.so has a PT_LOAD segment aligned to {a} bytes; Android 15+ needs {RequiredAlignment}."
        ));
    }

    static string? FindNative(string relative)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null)
        {
            var candidate = Path.Combine(
                dir.FullName,
                "src", "Shiny.DocumentDb.Sqlite.VectorSupport", "native", relative
            );
            if (File.Exists(candidate))
                return candidate;

            dir = dir.Parent;
        }
        return null;
    }

    // Minimal ELF64 program-header walk: p_align of every PT_LOAD segment.
    static List<ulong> ReadLoadSegmentAlignments(string path)
    {
        const int PT_LOAD = 1;
        var bytes = File.ReadAllBytes(path);

        Assert.True(bytes.Length > 64 && bytes[0] == 0x7F && bytes[1] == 'E' && bytes[2] == 'L' && bytes[3] == 'F', $"{path} is not an ELF file.");
        Assert.True(bytes[4] == 2, $"{path} is not ELF64.");

        var phoff = BitConverter.ToUInt64(bytes, 0x20);
        var phentsize = BitConverter.ToUInt16(bytes, 0x36);
        var phnum = BitConverter.ToUInt16(bytes, 0x38);

        var aligns = new List<ulong>();
        for (var i = 0; i < phnum; i++)
        {
            var offset = (int)phoff + (i * phentsize);
            if (BitConverter.ToUInt32(bytes, offset) == PT_LOAD)
                aligns.Add(BitConverter.ToUInt64(bytes, offset + 0x30));
        }
        return aligns;
    }
}
