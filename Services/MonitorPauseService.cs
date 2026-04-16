using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Central pause switch for all background DB checks.
/// When paused, all automated scans/queries skip their cycle.
/// On-demand commands (/status, /report) remain functional.
/// Auto-expires after the configured duration — survives app restarts via file persistence.
/// </summary>
public class MonitorPauseService
{
    private readonly ILogger<MonitorPauseService> _logger;
    private readonly string _stateFile = Path.Combine(AppContext.BaseDirectory, "monitor-pause.json");
    private DateTime? _pausedUntil;
    private string? _reason;
    private string? _triggeredBy;

    public MonitorPauseService(ILogger<MonitorPauseService> logger)
    {
        _logger = logger;
        Load();
    }

    public bool IsPaused
    {
        get
        {
            if (!_pausedUntil.HasValue) return false;
            if (DateTime.UtcNow >= _pausedUntil.Value)
            {
                Clear();
                return false;
            }
            return true;
        }
    }

    public DateTime? PausedUntil => _pausedUntil;
    public string? Reason => _reason;
    public string? TriggeredBy => _triggeredBy;

    public TimeSpan? TimeRemaining =>
        _pausedUntil.HasValue && _pausedUntil.Value > DateTime.UtcNow
            ? _pausedUntil.Value - DateTime.UtcNow
            : null;

    public void Pause(TimeSpan duration, string? reason = null, string? triggeredBy = null)
    {
        _pausedUntil = DateTime.UtcNow.Add(duration);
        _reason = reason;
        _triggeredBy = triggeredBy;
        Save();
        _logger.LogWarning("Monitor DB operations PAUSED for {Duration} by {By}. Reason: {Reason}",
            duration, triggeredBy ?? "?", reason ?? "?");
    }

    public void Resume(string? triggeredBy = null)
    {
        var wasPaused = IsPaused;
        Clear();
        if (wasPaused)
            _logger.LogWarning("Monitor DB operations RESUMED by {By}", triggeredBy ?? "?");
    }

    private void Clear()
    {
        _pausedUntil = null;
        _reason = null;
        _triggeredBy = null;
        Save();
    }

    private void Save()
    {
        try
        {
            var state = new { pausedUntil = _pausedUntil?.ToString("o"), reason = _reason, triggeredBy = _triggeredBy };
            File.WriteAllText(_stateFile, JsonSerializer.Serialize(state));
        }
        catch (Exception ex) { _logger.LogDebug("MonitorPause Save failed: {Err}", ex.Message); }
    }

    private void Load()
    {
        try
        {
            if (!File.Exists(_stateFile)) return;
            using var doc = JsonDocument.Parse(File.ReadAllText(_stateFile));
            var root = doc.RootElement;
            if (root.TryGetProperty("pausedUntil", out var pu) && pu.ValueKind == JsonValueKind.String
                && DateTime.TryParse(pu.GetString(), out var dt) && dt > DateTime.UtcNow)
            {
                _pausedUntil = dt;
                _reason = root.TryGetProperty("reason", out var r) && r.ValueKind == JsonValueKind.String ? r.GetString() : null;
                _triggeredBy = root.TryGetProperty("triggeredBy", out var tb) && tb.ValueKind == JsonValueKind.String ? tb.GetString() : null;
                _logger.LogInformation("MonitorPause restored: paused until {Until} by {By}", _pausedUntil, _triggeredBy);
            }
        }
        catch (Exception ex) { _logger.LogDebug("MonitorPause Load failed: {Err}", ex.Message); }
    }
}
