using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Enhanced Historical data — auto-captures snapshots, trend analysis, performance reports,
/// long-term retention (5000 snapshots), CSV/JSON export, period comparison.
/// </summary>
public class HistoricalDataService
{
    private readonly AzureMonitoringService _azure;
    private readonly string _connStr;
    private readonly ILogger<HistoricalDataService> _logger;
    private readonly string _storePath;

    public HistoricalDataService(
        AzureMonitoringService azure,
        IConfiguration config,
        ILogger<HistoricalDataService> logger)
    {
        _azure = azure;
        _connStr = config.GetConnectionString("SqlServer") ?? "";
        _logger = logger;
        var baseDir = Directory.Exists("/home") ? "/home/MediciMonitor"
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MediciMonitor");
        _storePath = Path.Combine(baseDir, "HistoricalData");
        try { Directory.CreateDirectory(_storePath); }
        catch (IOException) { _storePath = Path.GetTempPath(); }
    }

    // ── Capture Snapshot ─────────────────────────────────────────

    public async Task<object> CaptureCurrentSnapshot()
    {
        try
        {
            var snap = new HistoricalSnapshot { TimeStamp = DateTime.Now };

            try
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                await conn.OpenAsync();
                snap.DatabaseConnected = true;

                using var cmd1 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_Book WHERE IsActive=1 AND DateInsert >= CAST(GETDATE() AS DATE)", conn);
                cmd1.CommandTimeout = 10;
                snap.BookingsCount = Convert.ToInt32(await cmd1.ExecuteScalarAsync() ?? 0);

                using var cmd2 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_BookError WHERE DateInsert >= CAST(GETDATE() AS DATE)", conn);
                cmd2.CommandTimeout = 10;
                snap.ErrorsCount = Convert.ToInt32(await cmd2.ExecuteScalarAsync() ?? 0);

                using var cmd3 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_CancelBookError WHERE DateInsert >= CAST(GETDATE() AS DATE)", conn);
                cmd3.CommandTimeout = 10;
                snap.CancelErrorsCount = Convert.ToInt32(await cmd3.ExecuteScalarAsync() ?? 0);

                using var cmd4 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_Book WHERE IsActive=1 AND CancellationTo < GETDATE()", conn);
                cmd4.CommandTimeout = 10;
                snap.StuckCount = Convert.ToInt32(await cmd4.ExecuteScalarAsync() ?? 0);

                using var cmd5 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND (IsSold = 0 OR IsSold IS NULL) AND CancellationTo >= GETDATE()", conn);
                cmd5.CommandTimeout = 10;
                snap.WasteRoomCount = Convert.ToInt32(await cmd5.ExecuteScalarAsync() ?? 0);
            }
            catch (Exception ex) { _logger.LogWarning("Snapshot DB partial: {Err}", ex.Message); }

            try
            {
                var api = await _azure.ComprehensiveApiHealthCheck();
                snap.ApiHealthRatio = (double)api.Count(a => a.IsHealthy) / Math.Max(api.Count, 1);
                var times = api.Where(a => a.ResponseTimeMs > 0).Select(a => (double)a.ResponseTimeMs).ToList();
                snap.AverageResponseTime = times.Any() ? times.Average() : 0;
            }
            catch (Exception ex) { _logger.LogWarning("Snapshot API partial: {Err}", ex.Message); }

            snap.OverallStatus = !snap.DatabaseConnected ? "Critical - DB Offline"
                : snap.ApiHealthRatio < 0.5 ? "Critical - API Issues"
                : snap.ErrorsCount > 20 ? "Warning - High Errors"
                : snap.ApiHealthRatio < 0.8 ? "Warning - Some Issues"
                : "Healthy";

            await SaveSnapshot(snap);
            return new { Success = true, Snapshot = snap, Message = "צילום מצב בוצע בהצלחה" };
        }
        catch (Exception ex) { return new { Success = false, Error = ex.Message }; }
    }

    // ── Trend Analysis ───────────────────────────────────────────

    public async Task<object> GetTrendAnalysis(string period = "24h")
    {
        try
        {
            var snaps = await LoadSnapshots(period);
            if (!snaps.Any()) return new { Success = false, Message = "אין נתונים היסטוריים — המתן לאיסוף או צלם ידנית" };

            var trend = new
            {
                Period = period,
                DataPoints = snaps.Count,
                Labels = snaps.Select(s => s.TimeStamp).ToList(),
                BookingsTrend = snaps.Select(s => s.BookingsCount).ToList(),
                ErrorsTrend = snaps.Select(s => s.ErrorsCount).ToList(),
                ApiHealthTrend = snaps.Select(s => s.ApiHealthRatio).ToList(),
                ResponseTimeTrend = snaps.Select(s => s.AverageResponseTime).ToList(),
                StuckTrend = snaps.Select(s => s.StuckCount).ToList(),
                WasteTrend = snaps.Select(s => s.WasteRoomCount).ToList(),
                Summary = new Dictionary<string, object>
                {
                    ["AvgBookings"] = snaps.Average(s => s.BookingsCount),
                    ["AvgErrors"] = snaps.Average(s => s.ErrorsCount),
                    ["AvgApiHealth"] = $"{snaps.Average(s => s.ApiHealthRatio):P1}",
                    ["AvgResponseTime"] = $"{snaps.Average(s => s.AverageResponseTime):F0}ms",
                    ["DbUptime"] = $"{snaps.Count(s => s.DatabaseConnected) * 100.0 / snaps.Count:F1}%",
                    ["HealthyPeriods"] = snaps.Count(s => s.OverallStatus == "Healthy"),
                    ["WarningPeriods"] = snaps.Count(s => s.OverallStatus.Contains("Warning")),
                    ["CriticalPeriods"] = snaps.Count(s => s.OverallStatus.Contains("Critical")),
                    ["MaxStuck"] = snaps.Max(s => s.StuckCount),
                    ["MaxWaste"] = snaps.Max(s => s.WasteRoomCount)
                }
            };

            var insights = new List<string>();
            if (snaps.Count >= 2)
            {
                var bTrend = Trend(snaps.Select(s => (double)s.BookingsCount).ToList());
                if (bTrend > 0.1) insights.Add("מגמת הזמנות עולה"); else if (bTrend < -0.1) insights.Add("מגמת הזמנות יורדת");
                var eTrend = Trend(snaps.Select(s => (double)s.ErrorsCount).ToList());
                if (eTrend > 0.1) insights.Add("שיעור שגיאות עולה — דורש תשומת לב"); else if (eTrend < -0.1) insights.Add("שיעור שגיאות יורד — מגמה חיובית");
                var hTrend = Trend(snaps.Select(s => s.ApiHealthRatio).ToList());
                if (hTrend > 0.05) insights.Add("בריאות API משתפרת"); else if (hTrend < -0.05) insights.Add("בריאות API מידרדרת");
                var sTrend = Trend(snaps.Select(s => (double)s.StuckCount).ToList());
                if (sTrend > 0.1) insights.Add("מספר ביטולים תקועים עולה");
                var wTrend = Trend(snaps.Select(s => (double)s.WasteRoomCount).ToList());
                if (wTrend > 0.1) insights.Add("מספר חדרים לא נמכרים עולה");
            }

            return new { Success = true, TrendData = trend, Insights = insights };
        }
        catch (Exception ex) { return new { Success = false, Error = ex.Message }; }
    }

    // ── Performance Report ───────────────────────────────────────

    public async Task<object> GetPerformanceReport(string period = "7d")
    {
        try
        {
            var snaps = await LoadSnapshots(period);
            if (!snaps.Any()) return new { Success = false, Message = "אין נתונים לדוח ביצועים" };

            return new
            {
                Success = true,
                Report = new
                {
                    Period = period, GeneratedAt = DateTime.Now, DataPoints = snaps.Count,
                    Bookings = new
                    {
                        Total = snaps.Sum(s => s.BookingsCount),
                        AvgPerDay = snaps.GroupBy(s => s.TimeStamp.Date).Average(g => g.Sum(s => s.BookingsCount)),
                        Trend = TrendLabel(snaps.Select(s => (double)s.BookingsCount).ToList())
                    },
                    Performance = new
                    {
                        AvgApiHealth = $"{snaps.Average(s => s.ApiHealthRatio):P1}",
                        DbUptime = $"{snaps.Count(s => s.DatabaseConnected) * 100.0 / snaps.Count:F1}%",
                        AvgResponseTime = $"{snaps.Average(s => s.AverageResponseTime):F0}ms"
                    },
                    Errors = new
                    {
                        Total = snaps.Sum(s => s.ErrorsCount),
                        CancelErrors = snaps.Sum(s => s.CancelErrorsCount),
                        Trend = TrendLabel(snaps.Select(s => (double)s.ErrorsCount).ToList())
                    }
                }
            };
        }
        catch (Exception ex) { return new { Success = false, Error = ex.Message }; }
    }

    // ── Period Comparison ─────────────────────────────────────────

    public async Task<object> ComparePeriods(string period1, string period2)
    {
        try
        {
            var snaps1 = await LoadSnapshots(period1);
            var snaps2 = await LoadSnapshots(period2);

            if (!snaps1.Any() || !snaps2.Any())
                return new { Success = false, Message = "אין מספיק נתונים להשוואה" };

            return new
            {
                Success = true,
                Comparison = new
                {
                    Period1 = new { Period = period1, DataPoints = snaps1.Count, AvgBookings = snaps1.Average(s => s.BookingsCount), AvgErrors = snaps1.Average(s => s.ErrorsCount), AvgApiHealth = snaps1.Average(s => s.ApiHealthRatio), AvgResponseTime = snaps1.Average(s => s.AverageResponseTime), DbUptime = snaps1.Count(s => s.DatabaseConnected) * 100.0 / snaps1.Count },
                    Period2 = new { Period = period2, DataPoints = snaps2.Count, AvgBookings = snaps2.Average(s => s.BookingsCount), AvgErrors = snaps2.Average(s => s.ErrorsCount), AvgApiHealth = snaps2.Average(s => s.ApiHealthRatio), AvgResponseTime = snaps2.Average(s => s.AverageResponseTime), DbUptime = snaps2.Count(s => s.DatabaseConnected) * 100.0 / snaps2.Count }
                }
            };
        }
        catch (Exception ex) { return new { Success = false, Error = ex.Message }; }
    }

    // ── Export snapshots as CSV ───────────────────────────────────

    public async Task<string> ExportCsv(string period = "24h")
    {
        var snaps = await LoadSnapshots(period);
        var sb = new StringBuilder();
        sb.AppendLine("Timestamp,BookingsCount,ErrorsCount,CancelErrors,StuckCount,WasteRooms,ApiHealth,AvgResponseMs,DbConnected,Status");
        foreach (var s in snaps)
            sb.AppendLine($"{s.TimeStamp:yyyy-MM-dd HH:mm:ss},{s.BookingsCount},{s.ErrorsCount},{s.CancelErrorsCount},{s.StuckCount},{s.WasteRoomCount},{s.ApiHealthRatio:F2},{s.AverageResponseTime:F0},{s.DatabaseConnected},{s.OverallStatus}");
        return sb.ToString();
    }

    // ── Auto Capture (background) ────────────────────────────────

    public void StartAutoCapture(int intervalMinutes = 15)
    {
        _logger.LogInformation("Starting auto-capture every {Min} min", intervalMinutes);
        _ = Task.Run(async () =>
        {
            while (true)
            {
                try { await CaptureCurrentSnapshot(); }
                catch (Exception ex) { _logger.LogError("Auto-capture error: {Err}", ex.Message); }
                await Task.Delay(TimeSpan.FromMinutes(intervalMinutes));
            }
        });
    }

    // ── Persistence ──────────────────────────────────────────────

    private async Task SaveSnapshot(HistoricalSnapshot snap)
    {
        try
        {
            var file = Path.Combine(_storePath, $"snapshot_{snap.TimeStamp:yyyyMMdd_HHmmss}.json");
            await File.WriteAllTextAsync(file, JsonSerializer.Serialize(snap, new JsonSerializerOptions { WriteIndented = true }));
            await Cleanup();
        }
        catch (Exception ex) { _logger.LogWarning("Save snapshot failed: {Err}", ex.Message); }
    }

    private async Task<List<HistoricalSnapshot>> LoadSnapshots(string period)
    {
        var cutoff = period.ToLower() switch
        {
            "1h" => DateTime.Now.AddHours(-1), "6h" => DateTime.Now.AddHours(-6),
            "24h" => DateTime.Now.AddDays(-1), "7d" => DateTime.Now.AddDays(-7),
            "30d" => DateTime.Now.AddDays(-30), "90d" => DateTime.Now.AddDays(-90),
            _ => DateTime.Now.AddDays(-1)
        };
        var list = new List<HistoricalSnapshot>();
        foreach (var f in Directory.GetFiles(_storePath, "snapshot_*.json").OrderByDescending(f => File.GetCreationTime(f)).Take(5000))
        {
            try
            {
                var s = JsonSerializer.Deserialize<HistoricalSnapshot>(await File.ReadAllTextAsync(f));
                if (s != null && s.TimeStamp >= cutoff) list.Add(s);
            }
            catch { /* skip corrupt */ }
        }
        return list.OrderBy(s => s.TimeStamp).ToList();
    }

    private Task Cleanup()
    {
        try
        {
            var old = Directory.GetFiles(_storePath, "snapshot_*.json")
                .Select(f => new { Path = f, Time = File.GetCreationTime(f) })
                .OrderByDescending(f => f.Time).Skip(5000).ToList();
            foreach (var f in old) File.Delete(f.Path);
            if (old.Any()) _logger.LogInformation("Cleaned {Count} old snapshots", old.Count);
        }
        catch { /* swallow */ }
        return Task.CompletedTask;
    }

    private static double Trend(List<double> vals)
    {
        if (vals.Count < 2) return 0;
        var first = vals.Take(vals.Count / 3).Average();
        var last = vals.Skip(vals.Count * 2 / 3).Average();
        return first > 0 ? (last - first) / first : 0;
    }

    private static string TrendLabel(List<double> vals)
    {
        var t = Trend(vals);
        return t > 0.1 ? "עולה" : t < -0.1 ? "יורד" : "יציב";
    }
}
