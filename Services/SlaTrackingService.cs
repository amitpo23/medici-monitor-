namespace MediciMonitor.Services;

/// <summary>
/// SLA Tracking — monitors uptime, MTTR, MTTD for each endpoint and the database.
/// Runs periodic checks and accumulates statistics.
/// </summary>
public class SlaTrackingService
{
    private readonly AzureMonitoringService _azure;
    private readonly string _connStr;
    private readonly ILogger<SlaTrackingService> _logger;
    private readonly object _lock = new();
    private CancellationTokenSource? _cts;
    private StateStorageService? _stateStorage;

    // In-memory SLA data per endpoint
    private readonly Dictionary<string, EndpointSla> _endpoints = new();
    private readonly List<IncidentRecord> _incidents = new();
    private const int MaxIncidents = 1000;

    // State persistence
    private readonly string _stateFilePath;
    private DateTime _lastStateSave = DateTime.MinValue;
    private static readonly TimeSpan StateSaveInterval = TimeSpan.FromMinutes(1);

    public SlaTrackingService(AzureMonitoringService azure, IConfiguration config, ILogger<SlaTrackingService> logger)
    {
        _azure = azure;
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _logger = logger;

        _stateFilePath = Path.Combine(AppContext.BaseDirectory, "sla-state.json");
        LoadPersistedState();
    }

    public void SetStateStorageService(StateStorageService sss) => _stateStorage = sss;

    // ── Start background SLA monitoring ──

    public void StartTracking(int intervalSeconds = 60)
    {
        _cts?.Cancel();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;
        _logger.LogInformation("Starting SLA tracking every {Sec}s", intervalSeconds);
        _ = Task.Run(async () =>
        {
            while (!token.IsCancellationRequested)
            {
                try { await CheckAll(); }
                catch (Exception ex) { _logger.LogError("SLA check error: {Err}", ex.Message); }
                try { await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), token); }
                catch (OperationCanceledException) { break; }
            }
            _logger.LogInformation("SLA tracking stopped");
        }, token);
    }

    public void StopTracking()
    {
        _cts?.Cancel();
        _cts = null;
    }

    // ── Perform health checks and update SLA data ──

    private async Task CheckAll()
    {
        var apiHealth = await _azure.ComprehensiveApiHealthCheck();
        var now = DateTime.UtcNow;

        lock (_lock)
        {
            foreach (var api in apiHealth)
            {
                var name = api.Endpoint.Split('(')[0].Trim();
                if (!_endpoints.ContainsKey(name))
                    _endpoints[name] = new EndpointSla { EndpointName = name };

                var sla = _endpoints[name];
                sla.TotalChecks++;
                sla.LastChecked = now;
                sla.LastResponseTimeMs = api.ResponseTimeMs;

                if (api.ResponseTimeMs > 0)
                {
                    sla.ResponseTimes.Add(api.ResponseTimeMs);
                    if (sla.ResponseTimes.Count > 1440) sla.ResponseTimes.RemoveAt(0); // keep 24h at 1/min
                }

                if (api.IsHealthy)
                {
                    sla.SuccessfulChecks++;
                    if (!sla.IsCurrentlyUp)
                    {
                        // Was down, now up — record recovery
                        sla.IsCurrentlyUp = true;
                        if (sla.LastDownTime.HasValue)
                        {
                            var downDuration = now - sla.LastDownTime.Value;
                            sla.RecoveryTimes.Add(downDuration.TotalMinutes);
                            if (sla.RecoveryTimes.Count > 100) sla.RecoveryTimes.RemoveAt(0);

                            _incidents.Add(new IncidentRecord
                            {
                                Endpoint = name,
                                StartTime = sla.LastDownTime.Value,
                                EndTime = now,
                                DurationMinutes = downDuration.TotalMinutes,
                                Type = "Downtime"
                            });
                            while (_incidents.Count > MaxIncidents) _incidents.RemoveAt(0);
                        }
                        sla.LastDownTime = null;
                    }
                }
                else
                {
                    sla.FailedChecks++;
                    if (sla.IsCurrentlyUp)
                    {
                        // Was up, now down — start tracking downtime
                        sla.IsCurrentlyUp = false;
                        sla.LastDownTime = now;
                        sla.DetectionTimes.Add(0); // Detected immediately
                        if (sla.DetectionTimes.Count > 100) sla.DetectionTimes.RemoveAt(0);
                    }
                }
            }

            // DB check
            if (!_endpoints.ContainsKey("Database"))
                _endpoints["Database"] = new EndpointSla { EndpointName = "Database" };

            var dbSla = _endpoints["Database"];
            dbSla.TotalChecks++;
            dbSla.LastChecked = now;

            try
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                conn.Open();
                sw.Stop();
                dbSla.SuccessfulChecks++;
                dbSla.LastResponseTimeMs = (int)sw.ElapsedMilliseconds;
                dbSla.ResponseTimes.Add((int)sw.ElapsedMilliseconds);
                if (dbSla.ResponseTimes.Count > 1440) dbSla.ResponseTimes.RemoveAt(0);

                if (!dbSla.IsCurrentlyUp)
                {
                    dbSla.IsCurrentlyUp = true;
                    if (dbSla.LastDownTime.HasValue)
                    {
                        var dur = now - dbSla.LastDownTime.Value;
                        dbSla.RecoveryTimes.Add(dur.TotalMinutes);
                        _incidents.Add(new IncidentRecord { Endpoint = "Database", StartTime = dbSla.LastDownTime.Value, EndTime = now, DurationMinutes = dur.TotalMinutes, Type = "Downtime" });
                    }
                    dbSla.LastDownTime = null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("SLA DB check failed: {Err}", ex.Message);
                dbSla.FailedChecks++;
                if (dbSla.IsCurrentlyUp)
                {
                    dbSla.IsCurrentlyUp = false;
                    dbSla.LastDownTime = now;
                }
            }
        }

        SaveState();
    }

    // ── State Persistence ──

    private void LoadPersistedState()
    {
        try
        {
            if (!File.Exists(_stateFilePath)) return;
            var json = File.ReadAllText(_stateFilePath);
            var state = System.Text.Json.JsonSerializer.Deserialize<SlaPersistedState>(json);
            if (state == null) return;

            lock (_lock)
            {
                _endpoints.Clear();
                foreach (var (name, saved) in state.Endpoints)
                    _endpoints[name] = saved;

                _incidents.Clear();
                _incidents.AddRange(state.Incidents);
            }

            _logger.LogInformation("SLA state restored: {Endpoints} endpoints, {Incidents} incidents",
                state.Endpoints.Count, state.Incidents.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not load SLA state: {Err}", ex.Message);
        }
    }

    public void SaveState(bool force = false)
    {
        if (!force && DateTime.UtcNow - _lastStateSave < StateSaveInterval) return;
        try
        {
            SlaPersistedState state;
            lock (_lock)
            {
                state = new SlaPersistedState
                {
                    Endpoints = new Dictionary<string, EndpointSla>(_endpoints),
                    Incidents = _incidents.TakeLast(200).ToList(),
                    LastSaved = DateTime.UtcNow
                };
            }

            // File-based (fast)
            var json = System.Text.Json.JsonSerializer.Serialize(state, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(_stateFilePath, json);

            // DB-based (async, fire-and-forget)
            if (_stateStorage != null)
                _ = _stateStorage.SaveStateAsync("SlaTrackingService", "main", state);

            _lastStateSave = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not save SLA state: {Err}", ex.Message);
        }
    }

    // ── Get SLA Dashboard Data ──

    public SlaReport GetReport()
    {
        lock (_lock)
        {
            var report = new SlaReport { Timestamp = DateTime.UtcNow };

            foreach (var (name, sla) in _endpoints)
            {
                var entry = new SlaEntry
                {
                    Endpoint = name,
                    IsUp = sla.IsCurrentlyUp,
                    TotalChecks = sla.TotalChecks,
                    SuccessfulChecks = sla.SuccessfulChecks,
                    FailedChecks = sla.FailedChecks,
                    UptimePercent = sla.TotalChecks > 0 ? Math.Round((double)sla.SuccessfulChecks / sla.TotalChecks * 100, 3) : 100,
                    LastChecked = sla.LastChecked,
                    LastResponseTimeMs = sla.LastResponseTimeMs,
                    AvgResponseTimeMs = sla.ResponseTimes.Any() ? Math.Round(sla.ResponseTimes.Average(), 1) : 0,
                    P95ResponseTimeMs = sla.ResponseTimes.Any() ? GetPercentile(sla.ResponseTimes, 95) : 0,
                    P99ResponseTimeMs = sla.ResponseTimes.Any() ? GetPercentile(sla.ResponseTimes, 99) : 0,
                    MTTR = sla.RecoveryTimes.Any() ? Math.Round(sla.RecoveryTimes.Average(), 1) : 0,
                    MTTD = sla.DetectionTimes.Any() ? Math.Round(sla.DetectionTimes.Average(), 1) : 0
                };

                // Calculate current downtime if currently down
                if (!sla.IsCurrentlyUp && sla.LastDownTime.HasValue)
                    entry.CurrentDowntimeMinutes = (DateTime.UtcNow - sla.LastDownTime.Value).TotalMinutes;

                report.Endpoints.Add(entry);
            }

            // Overall SLA
            if (report.Endpoints.Any())
            {
                report.OverallUptime = Math.Round(report.Endpoints.Average(e => e.UptimePercent), 3);
                report.OverallMTTR = report.Endpoints.Where(e => e.MTTR > 0).Select(e => e.MTTR).DefaultIfEmpty(0).Average();
                report.OverallMTTD = report.Endpoints.Where(e => e.MTTD > 0).Select(e => e.MTTD).DefaultIfEmpty(0).Average();
            }

            // Recent incidents
            report.RecentIncidents = _incidents.OrderByDescending(i => i.StartTime).Take(20).ToList();

            return report;
        }
    }

    public SlaEntry? GetEndpointSla(string endpoint)
    {
        lock (_lock)
        {
            if (_endpoints.TryGetValue(endpoint, out var sla))
            {
                return new SlaEntry
                {
                    Endpoint = endpoint,
                    IsUp = sla.IsCurrentlyUp,
                    TotalChecks = sla.TotalChecks,
                    SuccessfulChecks = sla.SuccessfulChecks,
                    UptimePercent = sla.TotalChecks > 0 ? Math.Round((double)sla.SuccessfulChecks / sla.TotalChecks * 100, 3) : 100
                };
            }
            return null;
        }
    }

    private static double GetPercentile(List<int> sorted, int percentile)
    {
        var ordered = sorted.OrderBy(x => x).ToList();
        int idx = (int)Math.Ceiling(percentile / 100.0 * ordered.Count) - 1;
        return ordered[Math.Max(0, Math.Min(idx, ordered.Count - 1))];
    }
}

// ── Internal tracking models ──

public class EndpointSla
{
    public string EndpointName { get; set; } = "";
    public bool IsCurrentlyUp { get; set; } = true;
    public int TotalChecks { get; set; }
    public int SuccessfulChecks { get; set; }
    public int FailedChecks { get; set; }
    public DateTime? LastChecked { get; set; }
    public DateTime? LastDownTime { get; set; }
    public int LastResponseTimeMs { get; set; }
    public List<int> ResponseTimes { get; set; } = new();
    public List<double> RecoveryTimes { get; set; } = new();    // MTTR minutes
    public List<double> DetectionTimes { get; set; } = new();   // MTTD minutes
}

public class IncidentRecord
{
    public string Endpoint { get; set; } = "";
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public double DurationMinutes { get; set; }
    public string Type { get; set; } = "";
}

// ── API Response Models ──

public class SlaReport
{
    public DateTime Timestamp { get; set; }
    public double OverallUptime { get; set; }
    public double OverallMTTR { get; set; }
    public double OverallMTTD { get; set; }
    public List<SlaEntry> Endpoints { get; set; } = new();
    public List<IncidentRecord> RecentIncidents { get; set; } = new();
}

public class SlaEntry
{
    public string Endpoint { get; set; } = "";
    public bool IsUp { get; set; }
    public int TotalChecks { get; set; }
    public int SuccessfulChecks { get; set; }
    public int FailedChecks { get; set; }
    public double UptimePercent { get; set; }
    public DateTime? LastChecked { get; set; }
    public int LastResponseTimeMs { get; set; }
    public double AvgResponseTimeMs { get; set; }
    public double P95ResponseTimeMs { get; set; }
    public double P99ResponseTimeMs { get; set; }
    public double MTTR { get; set; }
    public double MTTD { get; set; }
    public double CurrentDowntimeMinutes { get; set; }
}

public class SlaPersistedState
{
    public Dictionary<string, EndpointSla> Endpoints { get; set; } = new();
    public List<IncidentRecord> Incidents { get; set; } = new();
    public DateTime LastSaved { get; set; }
}
