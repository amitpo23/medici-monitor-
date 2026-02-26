using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Incident Management — create, track, resolve incidents with timeline and postmortem.
/// </summary>
public class IncidentManagementService
{
    private readonly ILogger<IncidentManagementService> _logger;
    private readonly List<Incident> _incidents = new();
    private readonly object _lock = new();
    private readonly string _storePath;
    private int _nextId = 1;

    public IncidentManagementService(ILogger<IncidentManagementService> logger)
    {
        _logger = logger;
        var baseDir = Directory.Exists("/home") ? "/home/MediciMonitor"
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MediciMonitor");
        _storePath = Path.Combine(baseDir, "Incidents");
        try { Directory.CreateDirectory(_storePath); } catch { _storePath = Path.GetTempPath(); }
        LoadFromDisk();
    }

    // ── Create incident ──

    public Incident Create(string title, string description, string severity = "Warning", string? source = null)
    {
        lock (_lock)
        {
            var incident = new Incident
            {
                Id = _nextId++,
                Title = title,
                Description = description,
                Severity = severity,
                Status = "Open",
                Source = source ?? "Manual",
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
                Timeline = new List<TimelineEntry>
                {
                    new() { Timestamp = DateTime.UtcNow, Action = "Created", Detail = $"אירוע נוצר: {title}" }
                }
            };
            _incidents.Add(incident);
            SaveToDisk(incident);
            _logger.LogWarning("Incident #{Id} created: {Title} [{Severity}]", incident.Id, title, severity);
            return incident;
        }
    }

    // ── Auto-create from alert ──

    public Incident? CreateFromAlert(string alertId, string title, string message, string severity)
    {
        lock (_lock)
        {
            // Don't create duplicate incidents for same alert
            if (_incidents.Any(i => i.Source == $"Alert:{alertId}" && i.Status != "Resolved"))
                return null;

            return Create(title, message, severity, $"Alert:{alertId}");
        }
    }

    // ── Update incident ──

    public Incident? Update(int id, string status, string? note = null)
    {
        lock (_lock)
        {
            var incident = _incidents.FirstOrDefault(i => i.Id == id);
            if (incident == null) return null;

            incident.Status = status;
            incident.UpdatedAt = DateTime.UtcNow;

            var action = status switch
            {
                "Investigating" => "החלה חקירה",
                "Identified" => "הבעיה זוהתה",
                "Monitoring" => "במעקב",
                "Resolved" => "נפתר",
                _ => $"סטטוס עודכן ל-{status}"
            };

            incident.Timeline.Add(new TimelineEntry
            {
                Timestamp = DateTime.UtcNow,
                Action = action,
                Detail = note
            });

            if (status == "Resolved")
            {
                incident.ResolvedAt = DateTime.UtcNow;
                incident.DurationMinutes = (DateTime.UtcNow - incident.CreatedAt).TotalMinutes;
            }

            SaveToDisk(incident);
            _logger.LogInformation("Incident #{Id} updated to {Status}", id, status);
            return incident;
        }
    }

    // ── Add postmortem ──

    public Incident? AddPostmortem(int id, string rootCause, string resolution, string prevention)
    {
        lock (_lock)
        {
            var incident = _incidents.FirstOrDefault(i => i.Id == id);
            if (incident == null) return null;

            incident.Postmortem = new PostmortemReport
            {
                RootCause = rootCause,
                Resolution = resolution,
                Prevention = prevention,
                CreatedAt = DateTime.UtcNow
            };

            incident.Timeline.Add(new TimelineEntry
            {
                Timestamp = DateTime.UtcNow,
                Action = "Postmortem Added",
                Detail = "דוח ניתוח אחרי אירוע נוסף"
            });

            SaveToDisk(incident);
            return incident;
        }
    }

    // ── Get incidents ──

    public IncidentReport GetReport(string? status = null, int last = 50)
    {
        lock (_lock)
        {
            IEnumerable<Incident> q = _incidents;

            if (!string.IsNullOrEmpty(status))
                q = q.Where(i => i.Status.Equals(status, StringComparison.OrdinalIgnoreCase));

            var filtered = q.OrderByDescending(i => i.CreatedAt).Take(last).ToList();
            var open = _incidents.Count(i => i.Status != "Resolved");
            var resolved = _incidents.Count(i => i.Status == "Resolved");
            var avgResolutionTime = _incidents
                .Where(i => i.DurationMinutes > 0)
                .Select(i => i.DurationMinutes)
                .DefaultIfEmpty(0)
                .Average();

            return new IncidentReport
            {
                Timestamp = DateTime.UtcNow,
                TotalIncidents = _incidents.Count,
                OpenIncidents = open,
                ResolvedIncidents = resolved,
                AvgResolutionMinutes = Math.Round(avgResolutionTime, 1),
                Incidents = filtered,
                BySeverity = _incidents.GroupBy(i => i.Severity).ToDictionary(g => g.Key, g => g.Count()),
                ByStatus = _incidents.GroupBy(i => i.Status).ToDictionary(g => g.Key, g => g.Count())
            };
        }
    }

    public Incident? GetById(int id)
    {
        lock (_lock) { return _incidents.FirstOrDefault(i => i.Id == id); }
    }

    // ── Persistence ──

    private void SaveToDisk(Incident incident)
    {
        try
        {
            var file = Path.Combine(_storePath, $"incident_{incident.Id}.json");
            File.WriteAllText(file, JsonSerializer.Serialize(incident, new JsonSerializerOptions { WriteIndented = true }));
        }
        catch (Exception ex) { _logger.LogWarning("Save incident failed: {Err}", ex.Message); }
    }

    private void LoadFromDisk()
    {
        try
        {
            foreach (var file in Directory.GetFiles(_storePath, "incident_*.json").OrderBy(f => f))
            {
                try
                {
                    var json = File.ReadAllText(file);
                    var incident = JsonSerializer.Deserialize<Incident>(json);
                    if (incident != null)
                    {
                        _incidents.Add(incident);
                        if (incident.Id >= _nextId) _nextId = incident.Id + 1;
                    }
                }
                catch { /* skip corrupt */ }
            }
            _logger.LogInformation("Loaded {Count} incidents from disk", _incidents.Count);
        }
        catch { /* no history */ }
    }
}

// ── Models ──

public class Incident
{
    public int Id { get; set; }
    public string Title { get; set; } = "";
    public string Description { get; set; } = "";
    public string Severity { get; set; } = "Warning";
    public string Status { get; set; } = "Open";
    public string Source { get; set; } = "";
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public DateTime? ResolvedAt { get; set; }
    public double DurationMinutes { get; set; }
    public List<TimelineEntry> Timeline { get; set; } = new();
    public PostmortemReport? Postmortem { get; set; }
}

public class TimelineEntry
{
    public DateTime Timestamp { get; set; }
    public string Action { get; set; } = "";
    public string? Detail { get; set; }
}

public class PostmortemReport
{
    public string RootCause { get; set; } = "";
    public string Resolution { get; set; } = "";
    public string Prevention { get; set; } = "";
    public DateTime CreatedAt { get; set; }
}

public class IncidentReport
{
    public DateTime Timestamp { get; set; }
    public int TotalIncidents { get; set; }
    public int OpenIncidents { get; set; }
    public int ResolvedIncidents { get; set; }
    public double AvgResolutionMinutes { get; set; }
    public List<Incident> Incidents { get; set; } = new();
    public Dictionary<string, int> BySeverity { get; set; } = new();
    public Dictionary<string, int> ByStatus { get; set; } = new();
}
