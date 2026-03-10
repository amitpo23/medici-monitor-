using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Enhanced Alert rules engine — 19 rules, acknowledge/snooze, history, configurable thresholds,
/// notification integration.
/// </summary>
public class AlertingService
{
    private readonly AzureMonitoringService _azure;
    private readonly string _connStr;
    private readonly ILogger<AlertingService> _logger;
    private NotificationService? _notifications;
    private HistoricalDataService? _historical;
    private WebJobsMonitoringService? _webJobs;
    private StateStorageService? _stateStorage;

    // Alert state
    private readonly List<AlertInfo> _alertHistory = new();
    private readonly Dictionary<string, DateTime> _acknowledged = new();
    private readonly Dictionary<string, DateTime> _snoozed = new();
    private readonly Dictionary<string, DateTime> _lastNotifiedPerAlert = new();
    private readonly object _lock = new();
    private const int MaxHistory = 2000;

    // State persistence
    private readonly string _stateFilePath;
    private DateTime _lastStateSave = DateTime.MinValue;
    private static readonly TimeSpan StateSaveInterval = TimeSpan.FromMinutes(1);

    // Configurable thresholds
    public AlertThresholds Thresholds { get; set; } = new();

    public AlertingService(AzureMonitoringService azure, IConfiguration config, ILogger<AlertingService> logger)
    {
        _azure = azure;
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _logger = logger;

        _stateFilePath = Path.Combine(AppContext.BaseDirectory, "alerting-state.json");
        LoadPersistedState();
    }

    public void SetNotificationService(NotificationService ns) => _notifications = ns;
    public void SetHistoricalDataService(HistoricalDataService hds) => _historical = hds;
    public void SetWebJobsMonitoringService(WebJobsMonitoringService wjs) => _webJobs = wjs;
    public void SetStateStorageService(StateStorageService sss) => _stateStorage = sss;

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
            catch (Exception ex) { _logger.LogWarning("DB connectivity check failed: {Err}", ex.Message); }

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

            // 3b. SSL certificate expiry
            try
            {
                var sslResults = await _azure.CheckSslCertificates();
                foreach (var ssl in sslResults.Where(s => s.IsAccessible))
                {
                    if (ssl.DaysUntilExpiry <= 7)
                        alerts.Add(new AlertInfo { Id = $"SSL_EXPIRY_{ssl.Host}", Title = "SSL Certificate Expiring",
                            Message = $"תעודת SSL של {ssl.Host} פגה בעוד {ssl.DaysUntilExpiry} ימים!",
                            Severity = "Critical", Category = "Infrastructure" });
                    else if (ssl.DaysUntilExpiry <= 30)
                        alerts.Add(new AlertInfo { Id = $"SSL_WARNING_{ssl.Host}", Title = "SSL Certificate Warning",
                            Message = $"תעודת SSL של {ssl.Host} פגה בעוד {ssl.DaysUntilExpiry} ימים",
                            Severity = "Warning", Category = "Infrastructure" });
                }
            }
            catch (Exception ex) { _logger.LogDebug("SSL expiry check skipped: {Err}", ex.Message); }

            // 3c. DNS resolution failures
            try
            {
                var dnsResults = await _azure.CheckDnsHealth();
                var dnsFailed = dnsResults.Where(d => !d.IsResolved).ToList();
                if (dnsFailed.Any())
                    alerts.Add(new AlertInfo { Id = "DNS_FAILURE", Title = "DNS Resolution Failed",
                        Message = $"כשל DNS ב-{dnsFailed.Count} דומיינים: {string.Join(", ", dnsFailed.Select(d => d.Host))}",
                        Severity = "Critical", Category = "Infrastructure" });
            }
            catch (Exception ex) { _logger.LogDebug("DNS check skipped: {Err}", ex.Message); }

            // 4-15. DB-based alerts
            if (dbOk)
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                await conn.OpenAsync();

                // 4. Stuck cancellations — upgraded to Critical, added IsSold filter
                var stuck = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive=1 AND IsSold=1 AND CancellationTo < GETDATE()");
                if (stuck > 0)
                    alerts.Add(new AlertInfo { Id = "STUCK_CANCEL", Title = "Stuck Cancellations", Message = $"{stuck} active bookings past cancellation deadline — WebJob may be blocked!", Severity = "Critical", Category = "Business" });

                // 4b. Ghost cancellations — bookings marked inactive WITHOUT actual API cancellation (last 90 days)
                var ghost = await ScalarInt(conn, @"SELECT COUNT(*) FROM MED_Book b
                    WHERE b.IsActive = 0 AND b.IsSold = 1
                      AND b.CancellationTo >= DATEADD(DAY, -90, GETDATE())
                      AND NOT EXISTS (SELECT 1 FROM MED_CancelBook c WHERE c.PreBookId = b.PreBookId)
                      AND NOT EXISTS (SELECT 1 FROM MED_CancelBookError e WHERE e.PreBookId = b.PreBookId)");
                if (ghost > 0)
                    alerts.Add(new AlertInfo { Id = "GHOST_CANCEL", Title = "Ghost Cancellations", Message = $"{ghost} bookings marked inactive WITHOUT API cancellation — supplier may still charge!", Severity = "Critical", Category = "Business" });

                // 4b. Cancel retry loop detection (same bookings failing repeatedly)
                var retryLoopDistinct = await ScalarInt(conn,
                    $@"SELECT COUNT(*)
                        FROM (
                            SELECT PreBookId
                            FROM MED_CancelBookError
                            WHERE DateInsert >= DATEADD(HOUR, -{Thresholds.RetryLoopWindowHours}, GETDATE())
                              AND PreBookId IS NOT NULL
                            GROUP BY PreBookId
                            HAVING COUNT(*) >= {Thresholds.RetryLoopMinErrorsPerBooking}
                        ) d");

                if (retryLoopDistinct >= Thresholds.RetryLoopDistinctBookingsThreshold)
                {
                    var hotIds = await ScalarString(conn,
                        $@"SELECT STRING_AGG(CAST(PreBookId AS NVARCHAR(20)), ', ')
                           FROM (
                                SELECT TOP 5 PreBookId
                                FROM MED_CancelBookError
                                WHERE DateInsert >= DATEADD(HOUR, -{Thresholds.RetryLoopWindowHours}, GETDATE())
                                  AND PreBookId IS NOT NULL
                                GROUP BY PreBookId
                                HAVING COUNT(*) >= {Thresholds.RetryLoopMinErrorsPerBooking}
                                ORDER BY COUNT(*) DESC
                           ) t");

                    alerts.Add(new AlertInfo
                    {
                        Id = "CANCEL_RETRY_LOOP",
                        Title = "Cancellation Retry Loop",
                        Message = $"{retryLoopDistinct} הזמנות נכשלות שוב ושוב ב-{Thresholds.RetryLoopWindowHours} השעות האחרונות. דוגמאות: {hotIds ?? "N/A"}",
                        Severity = "Critical",
                        Category = "Errors"
                    });
                }

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

                // 9b. Sold bookings at cancellation risk (no cancel success yet)
                var soldOverdue = await ScalarInt(conn,
                    "SELECT COUNT(*) FROM MED_Book b WHERE b.IsActive = 1 AND ISNULL(b.IsSold, 0) = 1 AND b.CancellationTo < GETDATE() AND NOT EXISTS (SELECT 1 FROM MED_CancelBook c WHERE c.PreBookId = b.PreBookId)");
                var soldApproaching = await ScalarInt(conn,
                    $@"SELECT COUNT(*)
                       FROM MED_Book b
                       WHERE b.IsActive = 1
                         AND ISNULL(b.IsSold, 0) = 1
                         AND b.CancellationTo >= GETDATE()
                         AND b.CancellationTo <= DATEADD(HOUR, {Thresholds.SoldBookingRiskLookaheadHours}, GETDATE())
                         AND NOT EXISTS (SELECT 1 FROM MED_CancelBook c WHERE c.PreBookId = b.PreBookId)");

                if (soldOverdue > 0 || soldApproaching >= Thresholds.SoldBookingRiskThreshold)
                {
                    var soldExamples = await ScalarString(conn,
                        $@"SELECT STRING_AGG(CAST(PreBookId AS NVARCHAR(20)), ', ')
                           FROM (
                                SELECT TOP 5 b.PreBookId
                                FROM MED_Book b
                                WHERE b.IsActive = 1
                                  AND ISNULL(b.IsSold, 0) = 1
                                  AND b.CancellationTo <= DATEADD(HOUR, {Thresholds.SoldBookingRiskLookaheadHours}, GETDATE())
                                  AND NOT EXISTS (SELECT 1 FROM MED_CancelBook c WHERE c.PreBookId = b.PreBookId)
                                ORDER BY b.CancellationTo ASC
                           ) x");

                    alerts.Add(new AlertInfo
                    {
                        Id = "SOLD_CANCEL_RISK",
                        Title = "Sold Booking Cancellation Risk",
                        Message = $"{soldOverdue} sold הזמנות כבר עברו deadline ו-{soldApproaching} נוספות מתקרבות ב-{Thresholds.SoldBookingRiskLookaheadHours} שעות (דוגמאות: {soldExamples ?? "N/A"})",
                        Severity = soldOverdue > 0 ? "Critical" : "Warning",
                        Category = "Business"
                    });
                }

                // 10. BuyRooms system health
                var lastBook = await ScalarDateTime(conn, "SELECT MAX(DateInsert) FROM MED_Book");
                if (lastBook.HasValue)
                {
                    var minutesSince = (DateTime.UtcNow - lastBook.Value).TotalMinutes;
                    if (minutesSince > Thresholds.BuyRoomsDownMinutes)
                        alerts.Add(new AlertInfo { Id = "BUYROOMS_DOWN", Title = "BuyRooms Not Purchasing", Message = $"לא נרכשו חדרים {minutesSince:F0} דקות (סף: {Thresholds.BuyRoomsDownMinutes} דקות)", Severity = "Critical", Category = "System" });
                }

                // 10b. BuyRooms funnel drop
                var preBooks = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_PreBook WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())");
                var books = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())");
                if (preBooks > 5 && books == 0)
                    alerts.Add(new AlertInfo { Id = "BUYROOMS_FUNNEL_BROKEN", Title = "BuyRooms Funnel Broken",
                        Message = $"{preBooks} PreBooks נוצרו בשעה האחרונה אבל 0 Books — BuyRooms כנראה תקוע בשלב ההזמנה",
                        Severity = "Critical", Category = "System" });

                // 10c. Orphaned PreBooks
                try
                {
                    var orphaned = await ScalarInt(conn, @"SELECT COUNT(*) FROM MED_PreBook p
                        WHERE p.DateInsert >= DATEADD(HOUR, -2, GETDATE())
                        AND NOT EXISTS (SELECT 1 FROM MED_Book b WHERE b.PreBookId = p.PreBookId)");
                    if (orphaned > 10)
                        alerts.Add(new AlertInfo { Id = "ORPHANED_PREBOOKS", Title = "Orphaned PreBooks",
                            Message = $"{orphaned} PreBooks ללא Book תואם ב-2 שעות אחרונות — כשל חלקי ב-BuyRooms",
                            Severity = "Warning", Category = "System" });
                }
                catch (Exception ex) { _logger.LogDebug("Orphaned PreBooks check skipped: {Err}", ex.Message); }

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
                catch (Exception ex) { _logger.LogDebug("Revenue comparison skipped: {Err}", ex.Message); }

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
                catch (Exception ex) { _logger.LogDebug("SalesOffice failures check skipped: {Err}", ex.Message); }

                // 16. SalesOffice unprocessed callback backlog
                try
                {
                    int unprocessed = 0;
                    foreach (var tbl in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
                    {
                        try
                        {
                            unprocessed = await ScalarInt(conn, $"SELECT COUNT(*) FROM {tbl} WHERE IsProcessedCallback = 0");
                            break;
                        }
                        catch { /* try next */ }
                    }
                    if (unprocessed > Thresholds.SalesOfficeUnprocessedCallbackThreshold)
                        alerts.Add(new AlertInfo
                        {
                            Id = "SO_CALLBACK_BACKLOG",
                            Title = "SalesOffice Callback Backlog",
                            Message = $"{unprocessed:N0} callbacks לא מעובדים (סף: {Thresholds.SalesOfficeUnprocessedCallbackThreshold:N0})",
                            Severity = unprocessed > Thresholds.SalesOfficeUnprocessedCallbackThreshold * 3 ? "Critical" : "Warning",
                            Category = "Business"
                        });
                }
                catch (Exception ex) { _logger.LogDebug("SalesOffice callback backlog check skipped: {Err}", ex.Message); }

                // 17. SalesOffice zero-mapping orders
                try
                {
                    string? detTbl = null, ordTbl = null;
                    foreach (var t in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); detTbl = t; break; } catch { } }
                    foreach (var t in new[] { "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); ordTbl = t; break; } catch { } }

                    if (detTbl != null && ordTbl != null)
                    {
                        var zeroMapping = await ScalarInt(conn,
                            $@"SELECT COUNT(*) FROM {ordTbl} o
                               LEFT JOIN (SELECT OrderId, COUNT(*) as Cnt FROM {detTbl} GROUP BY OrderId) d ON d.OrderId = o.Id
                               WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%' AND (d.Cnt IS NULL OR d.Cnt = 0)");
                        if (zeroMapping > Thresholds.SalesOfficeZeroMappingThreshold)
                            alerts.Add(new AlertInfo
                            {
                                Id = "SO_ZERO_MAPPING",
                                Title = "SalesOffice Zero-Mapping Orders",
                                Message = $"{zeroMapping} הזמנות הושלמו ללא mapping — ריצות מבוזבזות (סף: {Thresholds.SalesOfficeZeroMappingThreshold})",
                                Severity = "Warning",
                                Category = "Business"
                            });
                    }
                }
                catch (Exception ex) { _logger.LogDebug("SalesOffice zero-mapping check skipped: {Err}", ex.Message); }

                // 18. SalesOffice slow mapping (avg time-to-map too high)
                try
                {
                    string? detTbl2 = null, ordTbl2 = null;
                    foreach (var t in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); detTbl2 = t; break; } catch { } }
                    foreach (var t in new[] { "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); ordTbl2 = t; break; } catch { } }

                    if (detTbl2 != null && ordTbl2 != null)
                    {
                        var avgTime = await ScalarDouble(conn,
                            $@"SELECT ISNULL(AVG(CAST(DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) AS FLOAT)), 0)
                               FROM {ordTbl2} o
                               INNER JOIN (SELECT OrderId, MIN(DateInsert) as FirstDetail FROM {detTbl2} GROUP BY OrderId) d ON d.OrderId = o.Id
                               WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                                 AND DATEDIFF(MINUTE, o.DateInsert, d.FirstDetail) >= 0");
                        if (avgTime > Thresholds.SalesOfficeSlowMapMinutes)
                            alerts.Add(new AlertInfo
                            {
                                Id = "SO_SLOW_MAPPING",
                                Title = "SalesOffice Slow Mapping",
                                Message = $"זמן ממוצע ליצירת Details: {avgTime:F0} דקות (סף: {Thresholds.SalesOfficeSlowMapMinutes} דקות)",
                                Severity = avgTime > Thresholds.SalesOfficeSlowMapMinutes * 3 ? "Critical" : "Warning",
                                Category = "Business"
                            });
                    }
                }
                catch (Exception ex) { _logger.LogDebug("SalesOffice slow mapping check skipped: {Err}", ex.Message); }

                // 19. SalesOffice partial mapping (orders with abnormally low details)
                try
                {
                    string? detTbl3 = null, ordTbl3 = null;
                    foreach (var t in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); detTbl3 = t; break; } catch { } }
                    foreach (var t in new[] { "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); ordTbl3 = t; break; } catch { } }

                    if (detTbl3 != null && ordTbl3 != null)
                    {
                        var partial = await ScalarInt(conn,
                            $@";WITH DetailCounts AS (
                                SELECT OrderId, COUNT(*) as Cnt FROM {detTbl3} GROUP BY OrderId
                            ),
                            AvgCnt AS (
                                SELECT AVG(CAST(d.Cnt AS FLOAT)) as Avg
                                FROM {ordTbl3} o INNER JOIN DetailCounts d ON d.OrderId = o.Id
                                WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                            )
                            SELECT COUNT(*)
                            FROM {ordTbl3} o
                            INNER JOIN DetailCounts d ON d.OrderId = o.Id
                            WHERE o.IsActive = 1 AND o.WebJobStatus LIKE 'Completed%'
                              AND d.Cnt < (SELECT Avg / 2.0 FROM AvgCnt)");
                        if (partial > Thresholds.SalesOfficePartialMappingThreshold)
                            alerts.Add(new AlertInfo
                            {
                                Id = "SO_PARTIAL_MAPPING",
                                Title = "SalesOffice Partial Mapping",
                                Message = $"{partial} הזמנות עם Details חלקיים (פחות מ-50% מהממוצע, סף: {Thresholds.SalesOfficePartialMappingThreshold})",
                                Severity = "Warning",
                                Category = "Business"
                            });
                    }
                }
                catch (Exception ex) { _logger.LogDebug("SalesOffice partial mapping check skipped: {Err}", ex.Message); }

                // 20. SalesOffice running without result (possible stuck execution)
                try
                {
                    string? ordTbl = null, detTbl = null;
                    foreach (var t in new[] { "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); ordTbl = t; break; } catch { } }
                    foreach (var t in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); detTbl = t; break; } catch { } }

                    if (ordTbl != null)
                    {
                        var hasDateInsert = await ScalarInt(conn,
                            $"SELECT CASE WHEN COL_LENGTH('{ordTbl.Replace("'", "''")}', 'DateInsert') IS NULL THEN 0 ELSE 1 END");

                        var staleDateFilter = hasDateInsert == 1
                            ? $"AND o.DateInsert <= DATEADD(HOUR, -{Thresholds.SalesOfficeRunningNoResultHours}, GETDATE())"
                            : "";

                        var runningNoResult = detTbl != null
                            ? await ScalarInt(conn,
                                $@"SELECT COUNT(*)
                                   FROM {ordTbl} o
                                   LEFT JOIN (SELECT OrderId, COUNT(*) as Cnt FROM {detTbl} GROUP BY OrderId) d ON d.OrderId = o.Id
                                   WHERE o.IsActive = 1
                                     AND (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress')
                                     AND ISNULL(d.Cnt, 0) = 0
                                     {staleDateFilter}")
                            : await ScalarInt(conn,
                                $@"SELECT COUNT(*)
                                   FROM {ordTbl} o
                                   WHERE o.IsActive = 1
                                     AND (o.WebJobStatus IS NULL OR o.WebJobStatus = 'In Progress')
                                     {staleDateFilter}");

                        if (runningNoResult >= Thresholds.SalesOfficeRunningNoResultThreshold)
                            alerts.Add(new AlertInfo
                            {
                                Id = "SO_RUNNING_NO_RESULT",
                                Title = "SalesOffice Running Without Result",
                                Message = $"{runningNoResult} הזמנות SalesOffice רצות ללא תוצאת mapping מעבר לסף זמן ({Thresholds.SalesOfficeRunningNoResultHours}h)",
                                Severity = runningNoResult >= Thresholds.SalesOfficeRunningNoResultThreshold * 3 ? "Critical" : "Warning",
                                Category = "Business"
                            });
                    }
                }
                catch (Exception ex) { _logger.LogDebug("SalesOffice running-without-result check skipped: {Err}", ex.Message); }

                // 21. SalesOffice completed but no matching reservation
                try
                {
                    string? soOrdTbl = null, soDetTbl = null;
                    foreach (var t in new[] { "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); soOrdTbl = t; break; } catch { } }
                    foreach (var t in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
                    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); soDetTbl = t; break; } catch { } }

                    if (soOrdTbl != null && soDetTbl != null)
                    {
                        var noReservation = await ScalarInt(conn,
                            $@"SELECT COUNT(DISTINCT o.Id)
                               FROM {soOrdTbl} o
                               INNER JOIN {soDetTbl} d ON d.OrderId = o.Id
                               LEFT JOIN Med_Reservation r ON r.HotelCode = CAST(d.HotelId AS NVARCHAR(20))
                                   AND r.Datefrom = o.DateFrom AND r.Dateto = o.DateTo
                               WHERE o.IsActive = 1
                                 AND o.WebJobStatus LIKE 'Completed%'
                                 AND o.DateInsert >= DATEADD(DAY, -7, GETDATE())
                                 AND r.HotelCode IS NULL");

                        if (noReservation > 0)
                            alerts.Add(new AlertInfo
                            {
                                Id = "SO_NO_RESERVATION",
                                Title = "SalesOffice Without Reservation",
                                Message = $"{noReservation} הזמנות SalesOffice הושלמו בשבוע האחרון ללא Reservation תואמת ב-Zenith — בדיקה נדרשת",
                                Severity = noReservation > 10 ? "Critical" : "Warning",
                                Category = "Business"
                            });
                    }
                }
                catch (Exception ex) { _logger.LogDebug("SalesOffice-Zenith cross-ref check skipped: {Err}", ex.Message); }

                // 22. Queue retry loop detection
                try
                {
                    var queueRetryLoop = await ScalarInt(conn,
                        @"SELECT COUNT(*) FROM (
                            SELECT [Key] FROM Queue
                            WHERE Status = 'Error' AND CreatedOn >= DATEADD(HOUR, -6, GETDATE())
                            GROUP BY [Key] HAVING COUNT(*) >= 3
                        ) x");
                    if (queueRetryLoop > 0)
                        alerts.Add(new AlertInfo { Id = "QUEUE_RETRY_LOOP", Title = "Queue Retry Loop",
                            Message = $"{queueRetryLoop} פריטים בתור נכשלים שוב ושוב (3+ כשלונות ב-6 שעות)",
                            Severity = queueRetryLoop > 10 ? "Critical" : "Warning", Category = "Queue" });
                }
                catch (Exception ex) { _logger.LogDebug("Queue retry loop check skipped: {Err}", ex.Message); }

                // 23. Push retry loop detection
                try
                {
                    var pushRetryLoop = await ScalarInt(conn,
                        @"SELECT COUNT(*) FROM (
                            SELECT HotelId FROM Med_HotelsToPush
                            WHERE IsActive = 0 AND Error IS NOT NULL AND Error != 'CancelBook'
                              AND DateInsert >= DATEADD(HOUR, -6, GETDATE())
                            GROUP BY HotelId HAVING COUNT(*) >= 3
                        ) x");
                    if (pushRetryLoop > 0)
                        alerts.Add(new AlertInfo { Id = "PUSH_RETRY_LOOP", Title = "Push Retry Loop",
                            Message = $"{pushRetryLoop} מלונות נכשלים ב-Push שוב ושוב (3+ כשלונות ב-6 שעות)",
                            Severity = pushRetryLoop > 5 ? "Critical" : "Warning", Category = "Push" });
                }
                catch (Exception ex) { _logger.LogDebug("Push retry loop check skipped: {Err}", ex.Message); }

                // 24. Conversion rate anomaly (compare today vs yesterday)
                try
                {
                    var todaySold = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND IsSold = 1 AND DateInsert >= CAST(GETDATE() AS DATE)");
                    var todayTotal = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND DateInsert >= CAST(GETDATE() AS DATE)");
                    var yesterdaySold = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND IsSold = 1 AND DateInsert >= CAST(DATEADD(DAY,-1,GETDATE()) AS DATE) AND DateInsert < CAST(GETDATE() AS DATE)");
                    var yesterdayTotal = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND DateInsert >= CAST(DATEADD(DAY,-1,GETDATE()) AS DATE) AND DateInsert < CAST(GETDATE() AS DATE)");

                    if (yesterdayTotal > 0 && todayTotal > 5 && DateTime.Now.Hour >= 14)
                    {
                        var todayRate = (double)todaySold / todayTotal;
                        var yesterdayRate = (double)yesterdaySold / yesterdayTotal;
                        if (yesterdayRate > 0 && todayRate < yesterdayRate * 0.5)
                            alerts.Add(new AlertInfo { Id = "CONVERSION_DROP", Title = "Conversion Rate Drop",
                                Message = $"שיעור המרה היום: {todayRate:P0} מול אתמול: {yesterdayRate:P0} — ירידה של {(1 - todayRate / yesterdayRate) * 100:F0}%",
                                Severity = "Warning", Category = "Business" });
                    }
                }
                catch (Exception ex) { _logger.LogDebug("Conversion anomaly check skipped: {Err}", ex.Message); }
            }

            // 25. Trend degradation from historical data
            if (_historical?.LastTrendWarning != null
                && _historical.LastTrendWarningTime.HasValue
                && (DateTime.UtcNow - _historical.LastTrendWarningTime.Value).TotalHours < 1)
            {
                alerts.Add(new AlertInfo { Id = "TREND_DEGRADATION", Title = "Trend Degradation Detected",
                    Message = _historical.LastTrendWarning, Severity = "Warning", Category = "System" });
            }

            // 26. WebJobs monitoring self-health
            if (_webJobs != null && !_webJobs.IsMonitoringHealthy)
                alerts.Add(new AlertInfo { Id = "WEBJOBS_MONITORING_DOWN", Title = "WebJobs Monitoring Down",
                    Message = _webJobs.MonitoringIssue ?? "ניטור WebJobs לא מחזיר תוצאות",
                    Severity = "Warning", Category = "Infrastructure" });

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

                // Send notifications for new critical/warning alerts (with cooldown)
                if (_notifications != null)
                {
                    var cooldownMinutes = _notifications.Config.CooldownMinutes;
                    foreach (var a in alerts.Where(a => !a.IsAcknowledged && !a.IsSnoozed))
                    {
                        var minSev = _notifications.Config.MinSeverity;
                        bool shouldNotify = a.Severity == "Critical" ||
                            (a.Severity == "Warning" && minSev != "Critical") ||
                            (a.Severity == "Info" && minSev == "Info");

                        if (!shouldNotify) continue;

                        // Check cooldown
                        if (_lastNotifiedPerAlert.TryGetValue(a.Id, out var lastSent)
                            && (DateTime.UtcNow - lastSent).TotalMinutes < cooldownMinutes)
                            continue; // Still in cooldown

                        _lastNotifiedPerAlert[a.Id] = DateTime.UtcNow;
                        _ = _notifications.SendAsync(a.Title, a.Message, a.Severity, a.Category);
                    }
                }
            }
        }
        catch (Exception ex) { _logger.LogError("EvaluateAlerts error: {Err}", ex.Message); }

        SaveState();
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

    // ── State Persistence (DB primary, file fallback) ──

    private void LoadPersistedState()
    {
        try
        {
            // Try file-based state first (fast, synchronous startup)
            if (File.Exists(_stateFilePath))
            {
                var json = File.ReadAllText(_stateFilePath);
                var state = JsonSerializer.Deserialize<AlertingPersistedState>(json);
                if (state != null) RestoreState(state);
                return;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not load alerting state from file: {Err}", ex.Message);
        }
    }

    public async Task LoadPersistedStateFromDbAsync()
    {
        if (_stateStorage == null) return;
        try
        {
            var state = await _stateStorage.LoadStateAsync<AlertingPersistedState>("AlertingService", "main");
            if (state != null) RestoreState(state);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not load alerting state from DB: {Err}", ex.Message);
        }
    }

    private void RestoreState(AlertingPersistedState state)
    {
        lock (_lock)
        {
            _alertHistory.Clear();
            _alertHistory.AddRange(state.AlertHistory);

            _acknowledged.Clear();
            foreach (var (k, v) in state.Acknowledged) _acknowledged[k] = v;

            _snoozed.Clear();
            foreach (var (k, v) in state.Snoozed) _snoozed[k] = v;

            _lastNotifiedPerAlert.Clear();
            foreach (var (k, v) in state.LastNotifiedPerAlert) _lastNotifiedPerAlert[k] = v;
        }

        _logger.LogInformation("Alerting state restored: {History} history, {Ack} acknowledged, {Snz} snoozed",
            state.AlertHistory.Count, state.Acknowledged.Count, state.Snoozed.Count);
    }

    public void SaveState(bool force = false)
    {
        if (!force && DateTime.UtcNow - _lastStateSave < StateSaveInterval) return;
        try
        {
            AlertingPersistedState state;
            lock (_lock)
            {
                state = new AlertingPersistedState
                {
                    AlertHistory = _alertHistory.TakeLast(500).ToList(),
                    Acknowledged = new Dictionary<string, DateTime>(_acknowledged),
                    Snoozed = new Dictionary<string, DateTime>(_snoozed),
                    LastNotifiedPerAlert = new Dictionary<string, DateTime>(_lastNotifiedPerAlert),
                    LastSaved = DateTime.UtcNow
                };
            }

            // File-based (fast)
            var json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(_stateFilePath, json);

            // DB-based (async, fire-and-forget)
            if (_stateStorage != null)
                _ = _stateStorage.SaveStateAsync("AlertingService", "main", state);

            _lastStateSave = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not save alerting state: {Err}", ex.Message);
        }
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

    private static async Task<string?> ScalarString(Microsoft.Data.SqlClient.SqlConnection conn, string sql)
    {
        using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn) { CommandTimeout = 10 };
        var result = await cmd.ExecuteScalarAsync();
        return result == null || result == DBNull.Value ? null : Convert.ToString(result);
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
    public int RetryLoopWindowHours { get; set; } = 24;
    public int RetryLoopMinErrorsPerBooking { get; set; } = 4;
    public int RetryLoopDistinctBookingsThreshold { get; set; } = 10;
    public int SoldBookingRiskLookaheadHours { get; set; } = 24;
    public int SoldBookingRiskThreshold { get; set; } = 1;
    public int SalesOfficeUnprocessedCallbackThreshold { get; set; } = 10000;
    public int SalesOfficeZeroMappingThreshold { get; set; } = 20;
    public double SalesOfficeSlowMapMinutes { get; set; } = 120;
    public int SalesOfficePartialMappingThreshold { get; set; } = 10;
    public int SalesOfficeRunningNoResultHours { get; set; } = 2;
    public int SalesOfficeRunningNoResultThreshold { get; set; } = 5;
}

public class AlertingPersistedState
{
    public List<AlertInfo> AlertHistory { get; set; } = new();
    public Dictionary<string, DateTime> Acknowledged { get; set; } = new();
    public Dictionary<string, DateTime> Snoozed { get; set; } = new();
    public Dictionary<string, DateTime> LastNotifiedPerAlert { get; set; } = new();
    public DateTime LastSaved { get; set; }
}
