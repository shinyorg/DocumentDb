using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ShinyDocDbMyAdmin.Tui;

/// <summary>
/// Writes log lines to <c>{DataDirectory}/shinydocdb.log</c>.
/// </summary>
/// <remarks>
/// A terminal app cannot log to the terminal: the console is the frame buffer, and one stray line
/// from a background task tears the render. But the services in Core log for a reason - a connection
/// that failed to open, a sidecar that could not be written - and losing that entirely would make the
/// tool harder to diagnose than the web app it mirrors. So the output goes to a file, truncated at
/// each start so it describes this run rather than every run.
/// </remarks>
public sealed class FileLoggerProvider : ILoggerProvider
{
    readonly StreamWriter? writer;
    readonly Lock gate = new();

    public FileLoggerProvider(IConfiguration configuration)
    {
        try
        {
            var directory = new AppPathsProbe(configuration).DataDirectory;
            Directory.CreateDirectory(directory);
            this.writer = new StreamWriter(
                new FileStream(Path.Combine(directory, "shinydocdb.log"), FileMode.Create, FileAccess.Write, FileShare.ReadWrite),
                Encoding.UTF8
            ) { AutoFlush = true };
        }
        catch (Exception)
        {
            // A log file that cannot be opened is not a reason to refuse to start. Run without one.
            this.writer = null;
        }
    }

    public ILogger CreateLogger(string categoryName) => new FileLogger(this, categoryName);

    public void Dispose()
    {
        lock (this.gate)
            this.writer?.Dispose();
    }

    void Write(string line)
    {
        if (this.writer is null)
            return;

        lock (this.gate)
            this.writer.WriteLine(line);
    }

    sealed class FileLogger(FileLoggerProvider provider, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            if (!this.IsEnabled(logLevel))
                return;

            var line = $"{DateTimeOffset.Now:HH:mm:ss.fff} {logLevel,-11} {category}: {formatter(state, exception)}";
            if (exception is not null)
                line += Environment.NewLine + exception;

            provider.Write(line);
        }
    }

    /// <summary>
    /// Resolves the data directory the same way <c>AppPaths</c> does, without constructing it -
    /// <c>AppPaths</c> is a container service and the logger is built while the container still is.
    /// </summary>
    sealed class AppPathsProbe(IConfiguration configuration)
    {
        public string DataDirectory { get; } =
            configuration["ShinyDocDbMyAdmin:DataDirectory"]
            ?? Environment.GetEnvironmentVariable("SHINYDOCDBMYADMIN_DATA")
            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".shinydocdbmyadmin");
    }
}
