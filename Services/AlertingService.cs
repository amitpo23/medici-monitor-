using System.Text;

namespace MediciMonitor.Services;

/// <summary>
/// Alert rules engine — evaluates conditions and generates alerts.
/// Ported from Medici-Control-Panel AlertingService.
/// </summary>
public class AlertingService
{
    private readonly AzureMonitoringService _azure;
    private readonly string _connStr;
    private readonly ILogger<AlertingService> _logger;

    public AlertingService(AzureMonitoringService azure, IConfiguration config, ILogger<AlertingService> logger)
    {
        _azure = azure;
        _connStr = config.GetConnectionString("SqlServer") ?? "";
        _logger = logger;
    }

    public async Task<List<AlertInfo>> EvaluateAlerts()
    {
        var alerts = new List<AlertInfo>();
        try
        {
            // 1. DB connectivity
            bool dbOk = false;
            try
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                await conn.OpenAsync();
                dbOk = true;
            }
            catch { }

            if (!dbOk)
                alerts.Add(new AlertInfo { Id = "DB_DOWN", Title = "Database Down", Message = "לא ניתן להתחבר למסד הנתונים", Severity = "Critical", Category = "Database" });

            // 2. API health
            var apiHealth = await _azure.ComprehensiveApiHealthCheck();
            var unhealthy = apiHealth.Where(a => !a.IsHealthy).ToList();
            if (unhealthy.Any())
                alerts.Add(new AlertInfo
                {
                    Id = "API_DOWN", Title = "API Endpoints Down",
                    Message = $"{unhealthy.Count} endpoints לא פעילים: {string.Join(", ", unhealthy.Select(u => u.Endpoint.Split('(')[0].Trim()))}",
                    Severity = "Critical", Category = "API"
                });

            var slow = apiHealth.Where(a => a.ResponseTimeMs > 5000).ToList();
            if (slow.Any())
                alerts.Add(new AlertInfo
                {
                    Id = "SLOW_API", Title = "Slow APIs",
                    Message = $"{slow.Count} APIs עם זמני תגובה > 5 שניות",
                    Severity = "Warning", Category = "Performance"
                });

            // 3. DB-based alerts
            if (dbOk)
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                await conn.OpenAsync();

                // Stuck cancellations
                using var cmd1 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_Book WHERE IsActive=1 AND CancellationTo < GETDATE()", conn);
                cmd1.CommandTimeout = 10;
                var stuck = Convert.ToInt32(await cmd1.ExecuteScalarAsync() ?? 0);
                if (stuck > 10)
                    alerts.Add(new AlertInfo { Id = "STUCK_CANCEL", Title = "Stuck Cancellations", Message = $"{stuck} הזמנות תקועות בביטול", Severity = "Warning", Category = "Business" });

                // Booking errors last hour
                using var cmd2 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM MED_BookError WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())", conn);
                cmd2.CommandTimeout = 10;
                var recentErrors = Convert.ToInt32(await cmd2.ExecuteScalarAsync() ?? 0);
                if (recentErrors > 5)
                    alerts.Add(new AlertInfo { Id = "ERR_SPIKE", Title = "Error Spike", Message = $"{recentErrors} שגיאות בשעה האחרונה", Severity = "Warning", Category = "Errors" });

                // No bookings during business hours
                if (DateTime.Now.Hour >= 10 && DateTime.Now.Hour <= 18)
                {
                    using var cmd3 = new Microsoft.Data.SqlClient.SqlCommand(
                        "SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= CAST(GETDATE() AS DATE)", conn);
                    cmd3.CommandTimeout = 10;
                    var todayBookings = Convert.ToInt32(await cmd3.ExecuteScalarAsync() ?? 0);
                    if (todayBookings == 0)
                        alerts.Add(new AlertInfo { Id = "NO_BOOKINGS", Title = "No Bookings Today", Message = "לא התקבלו הזמנות היום", Severity = "Info", Category = "Business" });
                }

                // Queue errors
                using var cmd4 = new Microsoft.Data.SqlClient.SqlCommand(
                    "SELECT COUNT(*) FROM Queue WHERE Status = 'Error' AND CreatedOn >= DATEADD(HOUR, -1, GETDATE())", conn);
                cmd4.CommandTimeout = 10;
                var queueErrors = Convert.ToInt32(await cmd4.ExecuteScalarAsync() ?? 0);
                if (queueErrors > 3)
                    alerts.Add(new AlertInfo { Id = "QUEUE_ERR", Title = "Queue Errors", Message = $"{queueErrors} שגיאות בתור בשעה האחרונה", Severity = "Warning", Category = "Queue" });
            }

            foreach (var a in alerts)
            {
                a.Timestamp = DateTime.UtcNow;
                a.IsActive = true;
            }
        }
        catch (Exception ex) { _logger.LogError("EvaluateAlerts error: {Err}", ex.Message); }
        return alerts;
    }

    public string GenerateSummary(List<AlertInfo> alerts)
    {
        if (!alerts.Any()) return "אין התראות פעילות — המערכת פועלת תקין";
        var sb = new StringBuilder();
        sb.AppendLine("סיכום התראות פעילות:");
        var critical = alerts.Where(a => a.Severity == "Critical").ToList();
        var warn = alerts.Where(a => a.Severity == "Warning").ToList();
        var info = alerts.Where(a => a.Severity == "Info").ToList();
        if (critical.Any()) { sb.AppendLine($"קריטי ({critical.Count}):"); foreach (var a in critical) sb.AppendLine($"  - {a.Title}: {a.Message}"); }
        if (warn.Any()) { sb.AppendLine($"אזהרה ({warn.Count}):"); foreach (var a in warn) sb.AppendLine($"  - {a.Title}: {a.Message}"); }
        if (info.Any()) { sb.AppendLine($"מידע ({info.Count}):"); foreach (var a in info) sb.AppendLine($"  - {a.Title}: {a.Message}"); }
        return sb.ToString();
    }
}
