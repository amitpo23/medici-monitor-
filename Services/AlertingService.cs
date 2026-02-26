using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Enhanced Alert rules engine — 15 rules, acknowledge/snooze, history, configurable thresholds,
/// notification integration.
/// </summary>
public class AlertingService
{
    private readonly AzureMonitoringService _azure;
    private readonly string _connStr;
    private readonly ILogger<AlertingService> _logger;
    private NotificationService? _notifications;

    // Alert state
    private readonly List<AlertInfo> _alertHistory = new();
    private readonly Dictionary<string, DateTime> _acknowledged = new();
    private readonly Dictionary<string, DateTime> _snoozed = new();
    private readonly object _lock = new();
    private const int MaxHistory = 2000;

    // Configurable thresholds
    public AlertThresholds Thresholds { get; set; } = new();

    public AlertingService(AzureMonitoringService azure, IConfiguration config, ILogger<AlertingService> logger)
    {
        _azure = azure;
        _connStr = config.GetConnectionString("SqlServer") ?? "";
        _logger = logger;
    }

    public void SetNotificationService(NotificationService ns) => _notifications = ns;

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

            var slow = apiHealth.Where(a => a.ResponseTimeMs > Thresholds.SlowApiThresholdMs).ToList();
            if (slow.Any())
                alerts.Add(new AlertInfo
                {
                    Id = "SLOW_API", Title = "Slow APIs",
                    Message = $"{slow.Count} APIs עם זמני תגובה > {Thresholds.SlowApiThresholdMs / 1000} שניות",
                    Severity = "Warning", Category = "Performance"
                });

            // 3. API response time degradation
            var avgResponse = apiHealth.Where(a => a.ResponseTimeMs > 0).Select(a => a.ResponseTimeMs).DefaultIfEmpty(0).Average();
            if (avgResponse > Thresholds.AvgResponseDegradationMs)
                alerts.Add(new AlertInfo
                {
                    Id = "API_DEGRADATION", Title = "API Response Degradation",
                    Message = $"זמן תגובה ממוצע: {avgResponse:F0}ms — מעל סף {Thresholds.AvgResponseDegradationMs}ms",
                    Severity = "Warning", Category = "Performance"
                });

            // 4-15. DB-based alerts
            if (dbOk)
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                await conn.OpenAsync();

                // 4. Stuck cancellations
                var stuck = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive=1 AND CancellationTo < GETDATE()");
                if (stuck > Thresholds.StuckCancellationThreshold)
                    alerts.Add(new AlertInfo { Id = "STUCK_CANCEL", Title = "Stuck Cancellations", Message = $"{stuck} הזמנות תקועות בביטול (סף: {Thresholds.StuckCancellationThreshold})", Severity = "Warning", Category = "Business" });

                // 5. Booking error spike
                var recentErrors = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_BookError WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())");
                if (recentErrors > Thresholds.ErrorSpikeThresholdPerHour)
                    alerts.Add(new AlertInfo { Id = "ERR_SPIKE", Title = "Error Spike", Message = $"{recentErrors} שגיאות בשעה האחרונה (סף: {Thresholds.ErrorSpikeThresholdPerHour})", Severity = "Warning", Category = "Errors" });

                // 6. No bookings during business hours
                if (DateTime.Now.Hour >= 10 && DateTime.Now.Hour <= 18)
                {
                    var todayBookings = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= CAST(GETDATE() AS DATE)");
                    if (todayBookings == 0)
                        alerts.Add(new AlertInfo { Id = "NO_BOOKINGS", Title = "No Bookings Today", Message = "לא התקבלו הזמנות היום", Severity = "Info", Category = "Business" });
                }

                // 7. Queue errors
                var queueErrors = await ScalarInt(conn, "SELECT COUNT(*) FROM Queue WHERE Status = 'Error' AND CreatedOn >= DATEADD(HOUR, -1, GETDATE())");
                if (queueErrors > Thresholds.QueueErrorThreshold)
                    alerts.Add(new AlertInfo { Id = "QUEUE_ERR", Title = "Queue Errors", Message = $"{queueErrors} שגיאות בתור בשעה האחרונה (סף: {Thresholds.QueueErrorThreshold})", Severity = "Warning", Category = "Queue" });

                // 8. Cancel error spike
                var cancelErrors = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_CancelBookError WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())");
                if (cancelErrors > Thresholds.CancelErrorSpikeThreshold)
                    alerts.Add(new AlertInfo { Id = "CANCEL_ERR_SPIKE", Title = "Cancel Error Spike", Message = $"{cancelErrors} שגיאות ביטול בשעה האחרונה", Severity = "Warning", Category = "Errors" });

                // 9. Room waste spike
                var wasteRooms = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND (IsSold = 0 OR IsSold IS NULL) AND CancellationTo >= GETDATE() AND CancellationTo <= DATEADD(HOUR, 24, GETDATE())");
                if (wasteRooms > Thresholds.WasteRoomThreshold)
                    alerts.Add(new AlertInfo { Id = "WASTE_SPIKE", Title = "Room Waste Urgent", Message = $"{wasteRooms} חדרים לא נמכרו פגי תוקף תוך 24h (סף: {Thresholds.WasteRoomThreshold})", Severity = "Warning", Category = "Business" });

                // 10. BuyRooms system health
                var lastBook = await ScalarDateTime(conn, "SELECT MAX(DateInsert) FROM MED_Book");
                if (lastBook.HasValue)
                {
                    var minutesSince = (DateTime.UtcNow - lastBook.Value).TotalMinutes;
                    if (minutesSince > Thresholds.BuyRoomsDownMinutes)
                        alerts.Add(new AlertInfo { Id = "BUYROOMS_DOWN", Title = "BuyRooms Not Purchasing", Message = $"לא נרכשו חדרים {minutesSince:F0} דקות (סף: {Thresholds.BuyRoomsDownMinutes} דקות)", Severity = "Critical", Category = "System" });
                }

                // 11. Push failure spike
                var pushFails = await ScalarInt(conn, "SELECT COUNT(*) FROM Med_HotelsToPush WHERE IsActive = 0 AND Error IS NOT NULL AND Error != 'CancelBook' AND DateInsert >= DATEADD(HOUR, -1, GETDATE())");
                if (pushFails > Thresholds.PushFailureThreshold)
                    alerts.Add(new AlertInfo { Id = "PUSH_FAIL_SPIKE", Title = "Push Failures Spike", Message = $"{pushFails} Push failures בשעה האחרונה", Severity = "Warning", Category = "Push" });

                // 12. Revenue drop (compared to yesterday same hour)
                try
                {
                    var todayRevenue = await ScalarDouble(conn, "SELECT ISNULL(SUM(AmountAfterTax), 0) FROM Med_Reservation WHERE DateInsert >= CAST(GETDATE() AS DATE)");
                    var yesterdayRevenue = await ScalarDouble(conn, "SELECT ISNULL(SUM(AmountAfterTax), 0) FROM Med_Reservation WHERE DateInsert >= CAST(DATEADD(DAY,-1,GETDATE()) AS DATE) AND DateInsert < CAST(GETDATE() AS DATE)");
                    if (yesterdayRevenue > 0 && DateTime.Now.Hour >= 12 && todayRevenue < yesterdayRevenue * 0.3)
                        alerts.Add(new AlertInfo { Id = "REVENUE_DROP", Title = "Revenue Drop", Message = $"הכנסות היום ${todayRevenue:N0} — ירידה משמעותית מאתמול (${yesterdayRevenue:N0})", Severity = "Warning", Category = "Business" });
                }
                catch { /* optional */ }

                // 13. Price drift anomaly
                var driftCount = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND LastPrice IS NOT NULL AND Price IS NOT NULL AND ABS(LastPrice - Price) > Price * 0.2");
                if (driftCount > Thresholds.PriceDriftAnomalyThreshold)
                    alerts.Add(new AlertInfo { Id = "PRICE_DRIFT_ANOMALY", Title = "Price Drift Anomaly", Message = $"{driftCount} חדרים עם שינוי מחיר > 20%", Severity = "Warning", Category = "Business" });

                // 14. BackOffice error spike
                var boErrors = await ScalarInt(conn, "SELECT COUNT(*) FROM BackOfficeOptLog WHERE DateCreate >= DATEADD(HOUR, -1, GETDATE())");
                if (boErrors > Thresholds.BackOfficeErrorThreshold)
                    alerts.Add(new AlertInfo { Id = "BO_ERR_SPIKE", Title = "BackOffice Errors", Message = $"{boErrors} שגיאות BackOffice בשעה האחרונה", Severity = "Warning", Category = "Errors" });

                // 15. SalesOffice failures
                try
                {
                    var soFails = await ScalarInt(conn, "SELECT COUNT(*) FROM SalesOfficeOrders WHERE IsActive = 1 AND (WebJobStatus LIKE 'Failed%' OR WebJobStatus = 'DateRangeError')");
                    if (soFails > 0)
                        alerts.Add(new AlertInfo { Id = "SO_FAILURES", Title = "SalesOffice Failures", Message = $"{soFails} SalesOffice orders נכשלו", Severity = "Warning", Category = "Business" });
                }
                catch { /* table might not exist */ }
            }

            // Set timestamp & active status, filter acknowledged/snoozed
            lock (_lock)
            {
                var now = DateTime.UtcNow;
                foreach (var a in alerts)
                {
                    a.Timestamp = now;
                    a.IsActive = true;

                    // Check if acknowledged
                    if (_acknowledged.ContainsKey(a.Id))
                    {
                        a.IsAcknowledged = true;
                        a.AcknowledgedAt = _acknowledged[a.Id];
                    }

                    // Check if snoozed
                    if (_snoozed.TryGetValue(a.Id, out var snoozeUntil) && now < snoozeUntil)
                    {
                        a.IsSnoozed = true;
                        a.SnoozedUntil = snoozeUntil;
                    }
                }

                // Store in history
                foreach (var a in alerts)
                {
                    _alertHistory.Add(a);
                }
                while (_alertHistory.Count > MaxHistory) _alertHistory.RemoveAt(0);

                // Send notifications for new critical/warning alerts (not acknowledged/snoozed)
                if (_notifications != null)
                {
                    foreach (var a in alerts.Where(a => !a.IsAcknowledged && !a.IsSnoozed))
                    {
                        var minSev = _notifications.Config.MinSeverity;
                        bool shouldNotify = a.Severity == "Critical" ||
                            (a.Severity == "Warning" && minSev != "Critical") ||
                            (a.Severity == "Info" && minSev == "Info");

                        if (shouldNotify)
                        {
                            _ = _notifications.SendAsync(a.Title, a.Message, a.Severity, a.Category);
                        }
                    }
                }
            }
        }
        catch (Exception ex) { _logger.LogError("EvaluateAlerts error: {Err}", ex.Message); }
        return alerts;
    }

    // ── Acknowledge alert ──

    public bool Acknowledge(string alertId)
    {
        lock (_lock)
        {
            _acknowledged[alertId] = DateTime.UtcNow;
            _logger.LogInformation("Alert {Id} acknowledged", alertId);
            return true;
        }
    }

    // ── Snooze alert ──

    public bool Snooze(string alertId, int minutes = 60)
    {
        lock (_lock)
        {
            _snoozed[alertId] = DateTime.UtcNow.AddMinutes(minutes);
            _logger.LogInformation("Alert {Id} snoozed for {Min} minutes", alertId, minutes);
            return true;
        }
    }

    // ── Unacknowledge ──

    public bool Unacknowledge(string alertId)
    {
        lock (_lock)
        {
            _acknowledged.Remove(alertId);
            _snoozed.Remove(alertId);
            return true;
        }
    }

    // ── Get alert history ──

    public List<AlertInfo> GetHistory(int last = 100, string? severity = null)
    {
        lock (_lock)
        {
            IEnumerable<AlertInfo> q = _alertHistory;
            if (!string.IsNullOrEmpty(severity))
                q = q.Where(a => a.Severity.Equals(severity, StringComparison.OrdinalIgnoreCase));
            return q.OrderByDescending(a => a.Timestamp).Take(last).ToList();
        }
    }

    // ── Get thresholds ──

    public AlertThresholds GetThresholds() => Thresholds;

    // ── Update thresholds ──

    public AlertThresholds UpdateThresholds(AlertThresholds newThresholds)
    {
        Thresholds = newThresholds;
        _logger.LogInformation("Alert thresholds updated");
        return Thresholds;
    }

    // ── Summary ──

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
        var acked = alerts.Count(a => a.IsAcknowledged);
        var snoozed = alerts.Count(a => a.IsSnoozed);
        if (acked > 0) sb.AppendLine($"\n{acked} התראות אושרו (acknowledged)");
        if (snoozed > 0) sb.AppendLine($"{snoozed} התראות מושתקות (snoozed)");
        return sb.ToString();
    }

    // ── Helpers ──

    private static async Task<int> ScalarInt(Microsoft.Data.SqlClient.SqlConnection conn, string sql)
    {
        using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn) { CommandTimeout = 10 };
        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }

    private static async Task<double> ScalarDouble(Microsoft.Data.SqlClient.SqlConnection conn, string sql)
    {
        using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn) { CommandTimeout = 10 };
        return Convert.ToDouble(await cmd.ExecuteScalarAsync() ?? 0.0);
    }

    private static async Task<DateTime?> ScalarDateTime(Microsoft.Data.SqlClient.SqlConnection conn, string sql)
    {
        using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn) { CommandTimeout = 10 };
        var result = await cmd.ExecuteScalarAsync();
        return result != null && result != DBNull.Value ? (DateTime?)Convert.ToDateTime(result) : null;
    }
}

// ── Models ──

public class AlertThresholds
{
    public int SlowApiThresholdMs { get; set; } = 5000;
    public int AvgResponseDegradationMs { get; set; } = 3000;
    public int StuckCancellationThreshold { get; set; } = 10;
    public int ErrorSpikeThresholdPerHour { get; set; } = 5;
    public int QueueErrorThreshold { get; set; } = 3;
    public int CancelErrorSpikeThreshold { get; set; } = 5;
    public int WasteRoomThreshold { get; set; } = 5;
    public int BuyRoomsDownMinutes { get; set; } = 30;
    public int PushFailureThreshold { get; set; } = 5;
    public int PriceDriftAnomalyThreshold { get; set; } = 3;
    public int BackOfficeErrorThreshold { get; set; } = 10;
}
