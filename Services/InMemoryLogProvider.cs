using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace MediciMonitor.Services;

// ════════════════════════════════════════════════════════════════════════
//  In-Memory Log Provider — keeps last N log entries in a ring buffer
//  and exposes them via API.  Also writes a rolling log file.
// ════════════════════════════════════════════════════════════════════════

public sealed class LogEntry
{
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public string Level { get; set; } = "Information";
    public string Category { get; set; } = "";
    public string Message { get; set; } = "";
    public string? Exception { get; set; }
    public int EventId { get; set; }

    [JsonIgnore]
    public string Line =>
        $"{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level,-12}] {Category}: {Message}" +
        (Exception != null ? $"\n  Exception: {Exception}" : "");
}

/// <summary>Thread-safe ring buffer that holds the last <paramref name="capacity"/> log entries.</summary>
public sealed class LogBuffer
{
    private readonly LogEntry[] _buffer;
    private long _position = -1;

    public int Capacity { get; }

    public LogBuffer(int capacity = 2000)
    {
        Capacity = capacity;
        _buffer = new LogEntry[capacity];
    }

    public void Add(LogEntry entry)
    {
        var idx = Interlocked.Increment(ref _position) % Capacity;
        _buffer[idx] = entry;
    }

    /// <summary>Returns entries in chronological order (oldest → newest).</summary>
    public List<LogEntry> GetAll()
    {
        var pos = Interlocked.Read(ref _position);
        if (pos < 0) return new();

        var count = Math.Min(pos + 1, Capacity);
        var list = new List<LogEntry>((int)count);

        var start = pos >= Capacity ? (pos + 1) % Capacity : 0;
        for (long i = 0; i < count; i++)
        {
            var entry = _buffer[(start + i) % Capacity];
            if (entry != null) list.Add(entry);
        }
        return list;
    }

    /// <summary>Get entries filtered by level, category, search text, max count.</summary>
    public List<LogEntry> Query(
        string? level = null,
        string? category = null,
        string? search = null,
        int? last = null,
        DateTime? since = null)
    {
        IEnumerable<LogEntry> q = GetAll();

        if (since.HasValue)
            q = q.Where(e => e.Timestamp >= since.Value);
        if (!string.IsNullOrEmpty(level))
            q = q.Where(e => e.Level.Equals(level, StringComparison.OrdinalIgnoreCase));
        if (!string.IsNullOrEmpty(category))
            q = q.Where(e => e.Category.Contains(category, StringComparison.OrdinalIgnoreCase));
        if (!string.IsNullOrEmpty(search))
            q = q.Where(e => e.Message.Contains(search, StringComparison.OrdinalIgnoreCase) ||
                             (e.Exception?.Contains(search, StringComparison.OrdinalIgnoreCase) ?? false));

        var result = q.ToList();
        if (last.HasValue && last.Value > 0 && result.Count > last.Value)
            result = result.Skip(result.Count - last.Value).ToList();

        return result;
    }
}

// ── ILoggerProvider implementation ──────────────────────────────────

public sealed class InMemoryLoggerProvider : ILoggerProvider
{
    private readonly LogBuffer _buffer;
    private readonly ConcurrentDictionary<string, InMemoryLogger> _loggers = new();
    private readonly string? _logFilePath;
    private readonly object _fileLock = new();

    public InMemoryLoggerProvider(LogBuffer buffer, string? logDirectory = null)
    {
        _buffer = buffer;

        if (!string.IsNullOrEmpty(logDirectory))
        {
            try
            {
                Directory.CreateDirectory(logDirectory);
                _logFilePath = Path.Combine(logDirectory, $"medici-monitor-{DateTime.UtcNow:yyyyMMdd}.log");
            }
            catch (IOException)
            {
                // Read-only filesystem — skip file logging, keep in-memory only
                _logFilePath = null;
            }
        }
    }

    public ILogger CreateLogger(string categoryName)
        => _loggers.GetOrAdd(categoryName, name => new InMemoryLogger(name, _buffer, WriteToFile));

    public void Dispose() { _loggers.Clear(); }

    private void WriteToFile(string line)
    {
        if (_logFilePath == null) return;
        try
        {
            lock (_fileLock)
            {
                // Rotate if date changed
                var expected = Path.Combine(
                    Path.GetDirectoryName(_logFilePath)!,
                    $"medici-monitor-{DateTime.UtcNow:yyyyMMdd}.log");

                File.AppendAllText(expected, line + Environment.NewLine);
            }
        }
        catch { /* don't crash the app for log I/O failures */ }
    }
}

public sealed class InMemoryLogger : ILogger
{
    private readonly string _category;
    private readonly LogBuffer _buffer;
    private readonly Action<string>? _fileWriter;

    public InMemoryLogger(string category, LogBuffer buffer, Action<string>? fileWriter = null)
    {
        _category = category;
        _buffer = buffer;
        _fileWriter = fileWriter;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        var entry = new LogEntry
        {
            Timestamp = DateTime.UtcNow,
            Level = logLevel.ToString(),
            Category = ShortenCategory(_category),
            Message = formatter(state, exception),
            Exception = exception?.ToString(),
            EventId = eventId.Id
        };

        _buffer.Add(entry);
        _fileWriter?.Invoke(entry.Line);
    }

    private static string ShortenCategory(string cat)
    {
        // "MediciMonitor.Services.AlertingService" → "AlertingService"
        var lastDot = cat.LastIndexOf('.');
        return lastDot >= 0 ? cat[(lastDot + 1)..] : cat;
    }
}
