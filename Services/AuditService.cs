namespace MediciMonitor.Services;

/// <summary>
/// Audit trail — logs who accessed what, when, from where.
/// Stores entries in a ring buffer and optional file.
/// </summary>
public class AuditService
{
    private readonly ILogger<AuditService> _logger;
    private readonly List<AuditEntry> _entries = new();
    private readonly object _lock = new();
    private readonly string? _auditFilePath;
    private const int MaxEntries = 5000;

    public AuditService(ILogger<AuditService> logger)
    {
        _logger = logger;
        try
        {
            var baseDir = Directory.Exists("/home") ? "/home/MediciMonitor"
                : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MediciMonitor");
            var dir = Path.Combine(baseDir, "Audit");
            Directory.CreateDirectory(dir);
            _auditFilePath = Path.Combine(dir, $"audit-{DateTime.UtcNow:yyyyMMdd}.log");
        }
        catch { _auditFilePath = null; }
    }

    // ── Record an audit entry ──

    public void Record(string action, string endpoint, string? clientIp = null, string? userAgent = null, string? detail = null)
    {
        var entry = new AuditEntry
        {
            Timestamp = DateTime.UtcNow,
            Action = action,
            Endpoint = endpoint,
            ClientIp = clientIp ?? "unknown",
            UserAgent = userAgent,
            Detail = detail
        };

        lock (_lock)
        {
            _entries.Add(entry);
            while (_entries.Count > MaxEntries) _entries.RemoveAt(0);
        }

        // Write to file
        if (_auditFilePath != null)
        {
            try
            {
                var line = $"{entry.Timestamp:yyyy-MM-dd HH:mm:ss.fff} | {entry.Action,-20} | {entry.Endpoint,-40} | {entry.ClientIp,-15} | {entry.Detail}";
                File.AppendAllText(_auditFilePath, line + Environment.NewLine);
            }
            catch { /* don't crash for audit I/O */ }
        }
    }

    // ── Record from HttpContext ──

    public void RecordFromHttp(HttpContext ctx, string action, string? detail = null)
    {
        var ip = ctx.Connection.RemoteIpAddress?.ToString();
        var ua = ctx.Request.Headers.UserAgent.FirstOrDefault();
        var endpoint = ctx.Request.Path.ToString();
        Record(action, endpoint, ip, ua, detail);
    }

    // ── Query audit log ──

    public AuditReport GetReport(string? action = null, string? endpoint = null, int last = 100, DateTime? since = null)
    {
        lock (_lock)
        {
            IEnumerable<AuditEntry> q = _entries;

            if (since.HasValue)
                q = q.Where(e => e.Timestamp >= since.Value);
            if (!string.IsNullOrEmpty(action))
                q = q.Where(e => e.Action.Contains(action, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(endpoint))
                q = q.Where(e => e.Endpoint.Contains(endpoint, StringComparison.OrdinalIgnoreCase));

            var filtered = q.OrderByDescending(e => e.Timestamp).Take(last).ToList();

            return new AuditReport
            {
                Timestamp = DateTime.UtcNow,
                TotalEntries = _entries.Count,
                FilteredCount = filtered.Count,
                Entries = filtered,
                TopEndpoints = _entries.GroupBy(e => e.Endpoint)
                    .OrderByDescending(g => g.Count())
                    .Take(10)
                    .ToDictionary(g => g.Key, g => g.Count()),
                TopActions = _entries.GroupBy(e => e.Action)
                    .OrderByDescending(g => g.Count())
                    .Take(10)
                    .ToDictionary(g => g.Key, g => g.Count()),
                TopIps = _entries.GroupBy(e => e.ClientIp)
                    .OrderByDescending(g => g.Count())
                    .Take(10)
                    .ToDictionary(g => g.Key, g => g.Count()),
                RequestsPerMinute = _entries.Count(e => e.Timestamp >= DateTime.UtcNow.AddMinutes(-1)),
                RequestsPerHour = _entries.Count(e => e.Timestamp >= DateTime.UtcNow.AddHours(-1))
            };
        }
    }
}

// ── Models ──

public class AuditEntry
{
    public DateTime Timestamp { get; set; }
    public string Action { get; set; } = "";
    public string Endpoint { get; set; } = "";
    public string ClientIp { get; set; } = "";
    public string? UserAgent { get; set; }
    public string? Detail { get; set; }
}

public class AuditReport
{
    public DateTime Timestamp { get; set; }
    public int TotalEntries { get; set; }
    public int FilteredCount { get; set; }
    public List<AuditEntry> Entries { get; set; } = new();
    public Dictionary<string, int> TopEndpoints { get; set; } = new();
    public Dictionary<string, int> TopActions { get; set; } = new();
    public Dictionary<string, int> TopIps { get; set; } = new();
    public int RequestsPerMinute { get; set; }
    public int RequestsPerHour { get; set; }
}
