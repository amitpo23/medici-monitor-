using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MediciMonitor.Services;

// ════════════════════════════════════════════════════════════════════════
//  Enhanced In-Memory Log Provider — 10K ring buffer, structured logging,
//  export capabilities, rolling log file.
// ════════════════════════════════════════════════════════════════════════

public sealed class LogEntry
{
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public string Level { get; set; } = "Information";
    public string Category { get; set; } = "";
    public string Message { get; set; } = "";
    public string? Exception { get; set; }
    public int EventId { get; set; }
    public string? CorrelationId { get; set; }

    [JsonIgnore]
    public string Line =>
        $"{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level,-12}] {Category}: {Message}" +
        (Exception != null ? $"\n  Exception: {Exception}" : "");

    [JsonIgnore]
    public string StructuredJson => JsonSerializer.Serialize(new
    {
        timestamp = Timestamp,
        level = Level,
        category = Category,
        message = Message,
        exception = Exception,
        eventId = EventId,
        correlationId = CorrelationId
    });
}

/// <summary>Thread-safe ring buffer that holds the last <paramref name="capacity"/> log entries.</summary>
public sealed class LogBuffer
{
    private readonly LogEntry[] _buffer;
    private long _position = -1;

    public int Capacity { get; }

    public LogBuffer(int capacity = 10000)
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

    /// <summary>Export logs as CSV string.</summary>
    public string ExportCsv(string? level = null, string? category = null, string? search = null, int? last = null, DateTime? since = null)
    {
        var entries = Query(level, category, search, last, since);
        var sb = new StringBuilder();
        sb.AppendLine("Timestamp,Level,Category,Message,Exception");
        foreach (var e in entries)
        {
            sb.AppendLine($"\"{e.Timestamp:yyyy-MM-dd HH:mm:ss.fff}\",\"{e.Level}\",\"{Escape(e.Category)}\",\"{Escape(e.Message)}\",\"{Escape(e.Exception ?? "")}\"");
        }
        return sb.ToString();
    }

    /// <summary>Export logs as JSON string.</summary>
    public string ExportJson(string? level = null, string? category = null, string? search = null, int? last = null, DateTime? since = null)
    {
        var entries = Query(level, category, search, last, since);
        return JsonSerializer.Serialize(entries, new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>Get detailed statistics.</summary>
    public LogStats GetStats()
    {
        var all = GetAll();
        var last5m = all.Where(e => e.Timestamp >= DateTime.UtcNow.AddMinutes(-5)).ToList();
        var last1h = all.Where(e => e.Timestamp >= DateTime.UtcNow.AddHours(-1)).ToList();

        return new LogStats
        {
            TotalEntries = all.Count,
            BufferCapacity = Capacity,
            BufferUsagePercent = Capacity > 0 ? Math.Round(all.Count * 100.0 / Capacity, 1) : 0,
            Last5Minutes = last5m.Count,
            LastHour = last1h.Count,
            ByLevel = all.GroupBy(e => e.Level).ToDictionary(g => g.Key, g => g.Count()),
            ByCategory = all.GroupBy(e => e.Category).ToDictionary(g => g.Key, g => g.Count()),
            ErrorCount = all.Count(e => e.Level == "Error" || e.Level == "Critical"),
            WarningCount = all.Count(e => e.Level == "Warning"),
            ErrorsLast5Min = last5m.Count(e => e.Level == "Error" || e.Level == "Critical"),
            ErrorsLastHour = last1h.Count(e => e.Level == "Error" || e.Level == "Critical"),
            LatestError = all.LastOrDefault(e => e.Level == "Error" || e.Level == "Critical"),
            OldestEntry = all.FirstOrDefault()?.Timestamp,
            NewestEntry = all.LastOrDefault()?.Timestamp,
            ErrorRatePerMinute = last5m.Count > 0 ? Math.Round(last5m.Count(e => e.Level == "Error" || e.Level == "Critical") / 5.0, 2) : 0
        };
    }

    private static string Escape(string s) => s.Replace("\"", "\"\"").Replace("\n", " ").Replace("\r", "");
}

public class LogStats
{
    public int TotalEntries { get; set; }
    public int BufferCapacity { get; set; }
    public double BufferUsagePercent { get; set; }
    public int Last5Minutes { get; set; }
    public int LastHour { get; set; }
    public Dictionary<string, int> ByLevel { get; set; } = new();
    public Dictionary<string, int> ByCategory { get; set; } = new();
    public int ErrorCount { get; set; }
    public int WarningCount { get; set; }
    public int ErrorsLast5Min { get; set; }
    public int ErrorsLastHour { get; set; }
    public LogEntry? LatestError { get; set; }
    public DateTime? OldestEntry { get; set; }
    public DateTime? NewestEntry { get; set; }
    public double ErrorRatePerMinute { get; set; }
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
        var lastDot = cat.LastIndexOf('.');
        return lastDot >= 0 ? cat[(lastDot + 1)..] : cat;
    }
}
