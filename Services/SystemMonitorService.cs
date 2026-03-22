using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Full system monitor — C# port of skills/monitor/system_monitor.py.
/// Provides 8 health checks: WebJob, Tables, Mapping, Skills, Orders, Zenith, Cancellation, CancelErrors.
/// Plus trend analysis and alert escalation.
/// </summary>
public class SystemMonitorService
{
    private readonly string _connStr;
    private readonly ILogger<SystemMonitorService> _logger;
    private readonly IConfiguration _config;

    // History for trend tracking (in-memory, persisted to JSON)
    private readonly List<MonitorRunHistory> _runHistory = new();
    private readonly object _historyLock = new();
    private const int MaxHistory = 500;

    // Alert thresholds (matching Python skill config.json)
    public int WebjobStaleMinutes { get; set; } = 30;
    public int MappingMissRatePerHour { get; set; } = 10;
    public int OrderDetailGapPct { get; set; } = 5;
    public int OverrideFailurePct { get; set; } = 20;
    public int ScanCycleMaxHours { get; set; } = 24;
    public int EscalationConsecutiveThreshold { get; set; } = 3;

    // Zenith probe config
    private readonly string _zenithUrl;
    private readonly string _zenithUsername;
    private readonly string _zenithPassword;

    // Last full report
    public MonitorReport? LastReport { get; private set; }

    public SystemMonitorService(IConfiguration config, ILogger<SystemMonitorService> logger)
    {
        _connStr = config.GetConnectionString("SqlServer")
            ?? throw new InvalidOperationException("Missing SqlServer connection string");
        _logger = logger;
        _config = config;

        _zenithUrl = config["Zenith:Url"] ?? "https://hotel.tools/service/Medici%20new";
        _zenithUsername = config["Zenith:Username"] ?? "APIMedici:Medici Live";
        _zenithPassword = config["Zenith:Password"] ?? "";

        LoadHistory();
    }

    // ══════════════════════════════════════════════════════════════
    //  FULL REPORT
    // ══════════════════════════════════════════════════════════════

    public async Task<MonitorReport> RunFullScan()
    {
        var report = new MonitorReport { Timestamp = DateTime.UtcNow };

        try
        {
            await SafeRun(report, "webjob", conn => CheckWebjob(conn, report));
            await SafeRun(report, "tables", conn => CheckTables(conn, report));
            await SafeRun(report, "mapping", conn => CheckMapping(conn, report));
            await SafeRun(report, "skills", conn => CheckSkills(conn, report));
            await SafeRun(report, "orders", conn => CheckOrders(conn, report));
            await SafeRun(report, "cancellation", conn => CheckCancellation(conn, report));
            await SafeRun(report, "cancel_errors", conn => CheckCancelErrors(conn, report));
            await SafeRun(report, "buyrooms", conn => CheckBuyRooms(conn, report));
            await SafeRun(report, "reservations", conn => CheckReservations(conn, report));
            await SafeRun(report, "price_override_pipeline", conn => CheckPriceOverridePipeline(conn, report));
            await SafeRun(report, "data_freshness", conn => CheckDataFreshness(conn, report));
            await SafeRun(report, "booking_sales", conn => CheckBookingSales(conn, report));

            // Zenith runs separately (HTTP call)
            await SafeRun(report, "zenith", () => CheckZenith(report));
        }
        catch (Exception ex)
        {
            report.Error = ex.Message;
            report.Alerts.Add(new MonitorAlert("CRITICAL", "Database", $"Cannot connect to DB: {ex.Message[..Math.Min(80, ex.Message.Length)]}"));
            _logger.LogError(ex, "SystemMonitor full scan failed");
        }

        // Escalation check
        ApplyEscalation(report);

        // Save to history
        SaveRunToHistory(report);

        // Attach trend
        report.Trend = GetTrendAnalysis(24);

        LastReport = report;
        return report;
    }

    // ══════════════════════════════════════════════════════════════
    //  1. WEBJOB HEALTH
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckWebjob(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Last activity
        using (var cmd = new SqlCommand("SELECT TOP 1 SalesOfficeOrderId, DateCreated FROM [SalesOffice.Log] ORDER BY Id DESC", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var orderId = rdr.GetInt32(0);
                var lastTime = rdr.GetDateTime(1);
                var minutesAgo = (DateTime.Now - lastTime).TotalMinutes;
                checks["last_log"] = new { order_id = orderId, time = lastTime.ToString("yyyy-MM-dd HH:mm:ss"), minutes_ago = Math.Round(minutesAgo, 1) };
                if (minutesAgo > WebjobStaleMinutes)
                    report.Alerts.Add(new MonitorAlert("CRITICAL", "WebJob", $"No activity for {minutesAgo:F0} minutes (last: Order {orderId})"));
            }
            else
            {
                checks["last_log"] = null;
                report.Alerts.Add(new MonitorAlert("CRITICAL", "WebJob", "No log entries found"));
            }
        }

        // In Progress orders
        using (var cmd = new SqlCommand(@"
            SELECT o.Id, o.DestinationId, h.[Name], o.WebJobStatus
            FROM [SalesOffice.Orders] o
            LEFT JOIN Med_Hotels h ON h.HotelId = CAST(o.DestinationId AS INT)
            WHERE o.WebJobStatus LIKE '%In Progress%' AND o.Id > 26", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            var inProgress = new List<object>();
            while (await rdr.ReadAsync())
            {
                var name = rdr.IsDBNull(2) ? $"Hotel {rdr.GetValue(1)}" : rdr.GetString(2);
                inProgress.Add(new { order_id = rdr.GetInt32(0), hotel = name });
            }
            checks["in_progress"] = inProgress;
        }

        // Pending orders
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE WebJobStatus IS NULL AND IsActive=1", conn))
        {
            checks["pending_orders"] = (int)await cmd.ExecuteScalarAsync()!;
        }

        // Failed orders
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE WebJobStatus LIKE '%Failed%' AND IsActive=1", conn))
        {
            var failed = (int)await cmd.ExecuteScalarAsync()!;
            checks["failed_orders"] = failed;
            if (failed > 0) report.Alerts.Add(new MonitorAlert("WARNING", "WebJob", $"{failed} failed orders"));
        }

        // Scan cycle estimate
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE IsActive=1", conn))
        {
            var totalActive = (int)await cmd.ExecuteScalarAsync()!;
            var cycleHours = totalActive * 30.0 / 3600;
            checks["active_orders"] = totalActive;
            checks["estimated_cycle_hours"] = Math.Round(cycleHours, 1);
            if (cycleHours > ScanCycleMaxHours)
                report.Alerts.Add(new MonitorAlert("WARNING", "WebJob", $"Scan cycle estimated {cycleHours:F0}h (threshold: {ScanCycleMaxHours}h)"));
        }

        report.Results["webjob"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  2. TABLE HEALTH
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckTables(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var tables = new Dictionary<string, object?>();

        var tableQueries = new Dictionary<string, string>
        {
            ["SalesOffice.Orders"] = "SELECT COUNT(*), SUM(CASE WHEN IsActive=1 THEN 1 ELSE 0 END) FROM [SalesOffice.Orders]",
            ["SalesOffice.Details"] = "SELECT COUNT(*), SUM(CASE WHEN IsDeleted=0 THEN 1 ELSE 0 END) FROM [SalesOffice.Details]",
            ["SalesOffice.MappingMisses"] = "SELECT COUNT(*), SUM(CASE WHEN Status='new' THEN 1 ELSE 0 END) FROM [SalesOffice.MappingMisses]",
            ["SalesOffice.PriceOverride"] = "SELECT COUNT(*), SUM(CASE WHEN IsActive=1 THEN 1 ELSE 0 END) FROM [SalesOffice.PriceOverride]",
            ["SalesOffice.Log"] = "SELECT COUNT(*), NULL FROM [SalesOffice.Log]",
            ["Med_Hotels_ratebycat"] = "SELECT COUNT(*), NULL FROM Med_Hotels_ratebycat",
            ["BackOfficeOPT"] = "SELECT COUNT(*), SUM(CASE WHEN Status=1 THEN 1 ELSE 0 END) FROM BackOfficeOPT",
            ["MED_Book"] = "SELECT COUNT(*), SUM(CASE WHEN IsActive=1 THEN 1 ELSE 0 END) FROM MED_Book",
            ["MED_CancelBook"] = "SELECT COUNT(*), NULL FROM MED_CancelBook",
            ["MED_CancelBookError"] = "SELECT COUNT(*), NULL FROM MED_CancelBookError",
        };

        foreach (var (table, query) in tableQueries)
        {
            try
            {
                using var cmd = new SqlCommand(query, conn) { CommandTimeout = 15 };
                using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    tables[table] = new { total = rdr.GetInt32(0), active = rdr.IsDBNull(1) ? (int?)null : rdr.GetInt32(1) };
                }
            }
            catch (Exception ex)
            {
                tables[table] = new { error = ex.Message[..Math.Min(80, ex.Message.Length)] };
                report.Alerts.Add(new MonitorAlert("ERROR", "Tables", $"Cannot query {table}: {ex.Message[..Math.Min(60, ex.Message.Length)]}"));
            }
        }

        report.Results["tables"] = tables;
        return tables;
    }

    // ══════════════════════════════════════════════════════════════
    //  3. MAPPING QUALITY
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckMapping(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Active hotels
        var activeHotels = new List<(string id, string? name)>();
        using (var cmd = new SqlCommand(@"
            SELECT DISTINCT o.DestinationId, h.[Name]
            FROM [SalesOffice.Orders] o
            LEFT JOIN Med_Hotels h ON h.HotelId = CAST(o.DestinationId AS INT)
            WHERE o.IsActive = 1", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
                activeHotels.Add((rdr.GetValue(0).ToString()!, rdr.IsDBNull(1) ? null : rdr.GetString(1)));
        }
        checks["active_hotels"] = activeHotels.Count;

        // ratebycat coverage
        using (var cmd = new SqlCommand("SELECT COUNT(DISTINCT HotelId) FROM Med_Hotels_ratebycat", conn))
            checks["hotels_with_ratebycat"] = (int)await cmd.ExecuteScalarAsync()!;

        // BB coverage
        using (var cmd = new SqlCommand("SELECT COUNT(DISTINCT HotelId) FROM Med_Hotels_ratebycat WHERE BoardId = 2", conn))
            checks["hotels_with_bb"] = (int)await cmd.ExecuteScalarAsync()!;

        // Open mapping misses
        using (var cmd = new SqlCommand(@"
            SELECT HotelId, RoomCategory, RoomBoard, COUNT(*) as Hits
            FROM [SalesOffice.MappingMisses]
            WHERE Status = 'new'
            GROUP BY HotelId, RoomCategory, RoomBoard", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            var misses = new List<object>();
            while (await rdr.ReadAsync())
                misses.Add(new { hotel_id = rdr.GetValue(0), category = rdr.IsDBNull(1) ? "" : rdr.GetString(1), board = rdr.IsDBNull(2) ? "" : rdr.GetString(2), hits = rdr.GetInt32(3) });
            checks["open_misses"] = misses;
            if (misses.Count > 0) report.Alerts.Add(new MonitorAlert("INFO", "Mapping", $"{misses.Count} open mapping misses"));
        }

        // Miss rate last hour
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM [SalesOffice.MappingMisses] WHERE SeenAt >= DATEADD(HOUR, -1, GETDATE())", conn))
        {
            var missRate = (int)await cmd.ExecuteScalarAsync()!;
            checks["miss_rate_last_hour"] = missRate;
            if (missRate > MappingMissRatePerHour)
                report.Alerts.Add(new MonitorAlert("WARNING", "Mapping", $"High miss rate: {missRate}/hour"));
        }

        // ORDER = DETAIL gaps per hotel
        var gaps = new List<object>();
        foreach (var (hid, hname) in activeHotels)
        {
            try
            {
                var name = hname ?? $"Hotel {hid}";

                int total, withDet, apiZero, nullCount;
                using (var cmd = new SqlCommand("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE DestinationId=@hid AND IsActive=1", conn))
                { cmd.Parameters.AddWithValue("@hid", hid); total = (int)await cmd.ExecuteScalarAsync()!; }

                using (var cmd = new SqlCommand(@"
                    SELECT COUNT(*) FROM [SalesOffice.Orders] o
                    WHERE o.DestinationId=@hid AND o.IsActive=1
                    AND EXISTS (SELECT 1 FROM [SalesOffice.Details] d WHERE d.SalesOfficeOrderId=o.Id AND d.IsDeleted=0)", conn))
                { cmd.Parameters.AddWithValue("@hid", hid); withDet = (int)await cmd.ExecuteScalarAsync()!; }

                using (var cmd = new SqlCommand(@"
                    SELECT COUNT(*) FROM [SalesOffice.Orders]
                    WHERE DestinationId=@hid AND IsActive=1
                    AND (WebJobStatus LIKE '%Api Rooms: 0%' OR WebJobStatus LIKE '%Api: 0%')", conn))
                { cmd.Parameters.AddWithValue("@hid", hid); apiZero = (int)await cmd.ExecuteScalarAsync()!; }

                using (var cmd = new SqlCommand("SELECT COUNT(*) FROM [SalesOffice.Orders] WHERE DestinationId=@hid AND IsActive=1 AND WebJobStatus IS NULL", conn))
                { cmd.Parameters.AddWithValue("@hid", hid); nullCount = (int)await cmd.ExecuteScalarAsync()!; }

                var gap = total - withDet - apiZero - nullCount;
                if (gap > 0)
                    gaps.Add(new { hotel = name.Length > 30 ? name[..30] : name, hotel_id = hid, gap, total });
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Gap check failed for hotel {HotelId}: {Err}", hid, ex.Message);
            }
        }
        checks["order_detail_gaps"] = gaps;
        if (gaps.Count > 0)
            report.Alerts.Add(new MonitorAlert("WARNING", "Mapping", $"{gaps.Count} hotels with ORDER!=DETAIL gaps"));

        report.Results["mapping"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  4. SKILLS HEALTH
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckSkills(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // autofix_worker — check last audit file
        checks["autofix_worker"] = CheckSkillAudit("salesoffice-mapping-gap-skill/autofix-report");

        // PriceOverride stats
        using (var cmd = new SqlCommand(@"
            SELECT COUNT(*),
                SUM(CASE WHEN IsActive=1 AND PushStatus IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsActive=1 AND PushStatus='success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN PushStatus='failed' THEN 1 ELSE 0 END)
            FROM [SalesOffice.PriceOverride]", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var total = rdr.GetInt32(0);
                var pending = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                var pushed = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                var failed = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
                checks["price_override"] = new { total, pending, pushed, failed };
                if (failed > 0 && total > 0 && (double)failed / total * 100 > OverrideFailurePct)
                    report.Alerts.Add(new MonitorAlert("WARNING", "PriceOverride", $"{failed} failed overrides ({(double)failed / total * 100:F0}%)"));
            }
        }

        // InsertOpp recent activity
        using (var cmd = new SqlCommand(@"
            SELECT COUNT(*), SUM(CASE WHEN Status=1 THEN 1 ELSE 0 END)
            FROM BackOfficeOPT
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
                checks["insert_opp"] = new { last_24h = rdr.GetInt32(0), active = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1) };
        }

        report.Results["skills"] = checks;
        return checks;
    }

    private object CheckSkillAudit(string reportDir)
    {
        var basePath = Path.Combine(AppContext.BaseDirectory, "..", "..", reportDir);
        if (!Directory.Exists(basePath))
        {
            // Try relative to working directory
            basePath = Path.Combine(Directory.GetCurrentDirectory(), reportDir);
        }
        if (!Directory.Exists(basePath))
            return new { status = "no_report_dir" };

        var files = Directory.GetFiles(basePath, "*.json").OrderByDescending(f => File.GetLastWriteTimeUtc(f)).ToList();
        if (files.Count == 0) return new { status = "no_audit_files" };

        var latest = Path.GetFileName(files[0]);
        var ageMin = (DateTime.UtcNow - File.GetLastWriteTimeUtc(files[0])).TotalMinutes;
        return new { last_file = latest, age_minutes = Math.Round(ageMin, 1) };
    }

    // ══════════════════════════════════════════════════════════════
    //  5. ORDERS HEALTH
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckOrders(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Active hotels and orders
        using (var cmd = new SqlCommand("SELECT COUNT(DISTINCT DestinationId), COUNT(*) FROM [SalesOffice.Orders] WHERE IsActive = 1", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            { checks["active_hotels"] = rdr.GetInt32(0); checks["active_orders"] = rdr.GetInt32(1); }
        }

        // Status breakdown
        using (var cmd = new SqlCommand(@"
            SELECT
                SUM(CASE WHEN WebJobStatus IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%Completed%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%In Progress%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%Failed%' THEN 1 ELSE 0 END),
                SUM(CASE WHEN WebJobStatus LIKE '%DateRange%' THEN 1 ELSE 0 END)
            FROM [SalesOffice.Orders] WHERE IsActive = 1", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                checks["status"] = new
                {
                    null_pending = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0),
                    completed = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1),
                    in_progress = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2),
                    failed = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3),
                    date_error = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4),
                };
            }
        }

        // Details breakdown
        using (var cmd = new SqlCommand(@"
            SELECT
                SUM(CASE WHEN RoomBoard='RO' AND IsDeleted=0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN RoomBoard='BB' AND IsDeleted=0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsDeleted=0 THEN 1 ELSE 0 END)
            FROM [SalesOffice.Details]", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                checks["details"] = new
                {
                    ro = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0),
                    bb = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1),
                    total = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2),
                };
            }
        }

        report.Results["orders"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  6. ZENITH SOAP PROBE
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckZenith(MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        var soap = $@"<SOAP-ENV:Envelope xmlns:SOAP-ENV=""http://schemas.xmlsoap.org/soap/envelope/"">
  <SOAP-ENV:Header><wsse:Security soap:mustUnderstand=""1"" xmlns:wsse=""http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"" xmlns:soap=""http://schemas.xmlsoap.org/soap/envelope/""><wsse:UsernameToken><wsse:Username>{_zenithUsername}</wsse:Username><wsse:Password Type=""http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText"">{_zenithPassword}</wsse:Password></wsse:UsernameToken></wsse:Security></SOAP-ENV:Header>
  <SOAP-ENV:Body><OTA_HotelAvailNotifRQ xmlns=""http://www.opentravel.org/OTA/2003/05"" Version=""1.0"" TimeStamp=""{DateTime.UtcNow:O}"" EchoToken=""monitor""><AvailStatusMessages HotelCode=""5093""><AvailStatusMessage BookingLimit=""0""><StatusApplicationControl Start=""2026-12-31"" End=""2026-12-31"" InvTypeCode=""Stnd"" RatePlanCode=""12062""/><RestrictionStatus Status=""Open"" /></AvailStatusMessage></AvailStatusMessages></OTA_HotelAvailNotifRQ></SOAP-ENV:Body></SOAP-ENV:Envelope>";

        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var content = new StringContent(soap, System.Text.Encoding.UTF8, "text/xml");
            var resp = await http.PostAsync(_zenithUrl, content);
            sw.Stop();
            var body = await resp.Content.ReadAsStringAsync();
            var success = resp.IsSuccessStatusCode && !body.Contains("Error");
            checks["status"] = success ? "OK" : "ERROR";
            checks["latency_ms"] = sw.ElapsedMilliseconds;
            checks["http_status"] = (int)resp.StatusCode;
            if (!success)
                report.Alerts.Add(new MonitorAlert("CRITICAL", "Zenith", $"Zenith API error: HTTP {(int)resp.StatusCode}"));
        }
        catch (Exception ex)
        {
            checks["status"] = "UNREACHABLE";
            checks["error"] = ex.Message[..Math.Min(100, ex.Message.Length)];
            report.Alerts.Add(new MonitorAlert("CRITICAL", "Zenith", $"Zenith unreachable: {ex.Message[..Math.Min(60, ex.Message.Length)]}"));
        }

        report.Results["zenith"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  7. AUTO-CANCELLATION HEALTH
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckCancellation(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND CancellationTo <= DATEADD(DAY, 5, GETDATE())", conn))
        {
            var upcoming = (int)await cmd.ExecuteScalarAsync()!;
            checks["bookings_near_cx_deadline"] = upcoming;
            if (upcoming > 10) report.Alerts.Add(new MonitorAlert("INFO", "Cancellation", $"{upcoming} bookings within 5 days of CX deadline"));
        }

        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM MED_CancelBook WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())", conn))
            checks["cancellations_24h"] = (int)await cmd.ExecuteScalarAsync()!;

        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM MED_CancelBookError WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())", conn))
        {
            var errors = (int)await cmd.ExecuteScalarAsync()!;
            checks["cancel_errors_24h"] = errors;
            if (errors > 0) report.Alerts.Add(new MonitorAlert("WARNING", "Cancellation", $"{errors} cancellation errors in last 24h"));
        }

        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1", conn))
            checks["active_bookings"] = (int)await cmd.ExecuteScalarAsync()!;

        report.Results["cancellation"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  8. CANCEL ERROR ANALYSIS
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckCancelErrors(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Total errors
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM MED_CancelBookError", conn))
            checks["total_errors"] = (int)await cmd.ExecuteScalarAsync()!;

        // Top error types
        using (var cmd = new SqlCommand(@"
            SELECT TOP 10 LEFT(ErrorMessage, 80) as error_type, COUNT(*) as cnt
            FROM MED_CancelBookError
            GROUP BY LEFT(ErrorMessage, 80) ORDER BY cnt DESC", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            var topErrors = new List<object>();
            while (await rdr.ReadAsync())
                topErrors.Add(new { error = rdr.IsDBNull(0) ? "" : rdr.GetString(0), count = rdr.GetInt32(1) });
            checks["top_error_types"] = topErrors;
        }

        // Per-hotel error rates (last 30 days)
        using (var cmd = new SqlCommand(@"
            SELECT cb.HotelId, h.[Name], COUNT(*) as errors
            FROM MED_CancelBookError cb
            LEFT JOIN Med_Hotels h ON h.HotelId = cb.HotelId
            WHERE cb.DateInsert >= DATEADD(DAY, -30, GETDATE())
            GROUP BY cb.HotelId, h.[Name]
            ORDER BY errors DESC", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            var perHotel = new List<object>();
            while (await rdr.ReadAsync())
                perHotel.Add(new { hotel_id = rdr.IsDBNull(0) ? 0 : rdr.GetInt32(0), hotel = rdr.IsDBNull(1) ? $"Hotel {rdr.GetValue(0)}" : rdr.GetString(1), errors = rdr.GetInt32(2) });
            checks["per_hotel_30d"] = perHotel;
        }

        // Daily trend (7 days)
        using (var cmd = new SqlCommand(@"
            SELECT CAST(DateInsert AS DATE) as day, COUNT(*) as cnt
            FROM MED_CancelBookError
            WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())
            GROUP BY CAST(DateInsert AS DATE) ORDER BY day DESC", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            var daily = new List<(string date, int count)>();
            while (await rdr.ReadAsync())
                daily.Add((rdr.GetDateTime(0).ToString("yyyy-MM-dd"), rdr.GetInt32(1)));
            checks["daily_trend_7d"] = daily.Select(d => new { date = d.date, count = d.count }).ToList();

            // Trend detection
            if (daily.Count >= 3)
            {
                var recentAvg = daily.Take(3).Average(d => d.count);
                var olderAvg = daily.Skip(3).DefaultIfEmpty().Average(d => d.count);
                if (olderAvg > 0 && recentAvg > olderAvg * 1.5)
                {
                    checks["trend"] = "INCREASING";
                    report.Alerts.Add(new MonitorAlert("WARNING", "CancelErrors", $"Cancel errors trending up: {recentAvg:F0}/day recent vs {olderAvg:F0}/day older"));
                }
                else if (recentAvg < olderAvg * 0.5) checks["trend"] = "DECREASING";
                else checks["trend"] = "STABLE";
            }
            else checks["trend"] = "INSUFFICIENT_DATA";
        }

        report.Results["cancel_errors"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  9. BUYROOMS HEARTBEAT (skill: buyroom)
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckBuyRooms(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Last purchase
        using (var cmd = new SqlCommand("SELECT TOP 1 DateInsert FROM MED_Book ORDER BY Id DESC", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var last = rdr.GetDateTime(0);
                var minAgo = (DateTime.Now - last).TotalMinutes;
                checks["last_purchase_time"] = last.ToString("yyyy-MM-dd HH:mm:ss");
                checks["minutes_since_last_purchase"] = Math.Round(minAgo, 1);
                // BuyRooms should buy at least every 2 hours during business hours
                if (minAgo > 120)
                    report.Alerts.Add(new MonitorAlert("WARNING", "BuyRooms", $"No purchases for {minAgo:F0} minutes"));
            }
        }

        // Last PreBook (attempt)
        using (var cmd = new SqlCommand("SELECT TOP 1 DateInsert FROM MED_PreBook ORDER BY Id DESC", conn))
        {
            cmd.CommandTimeout = 10;
            try
            {
                using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    var last = rdr.GetDateTime(0);
                    checks["last_prebook_time"] = last.ToString("yyyy-MM-dd HH:mm:ss");
                    checks["minutes_since_last_prebook"] = Math.Round((DateTime.Now - last).TotalMinutes, 1);
                }
            }
            catch { checks["last_prebook_time"] = "table_not_found"; }
        }

        // Funnel: PreBooks vs Books last hour
        using (var cmd = new SqlCommand(@"
            SELECT
                (SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())),
                (SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= DATEADD(HOUR, -24, GETDATE()))", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                checks["purchases_last_hour"] = rdr.GetInt32(0);
                checks["purchases_last_24h"] = rdr.GetInt32(1);
            }
        }

        // Active vs Sold ratio
        using (var cmd = new SqlCommand(@"
            SELECT COUNT(*),
                SUM(CASE WHEN IsSold=1 THEN 1 ELSE 0 END),
                SUM(Price)
            FROM MED_Book WHERE IsActive=1", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var active = rdr.GetInt32(0);
                var sold = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                var totalValue = rdr.IsDBNull(2) ? 0.0 : rdr.GetDouble(2);
                checks["active_bookings"] = active;
                checks["sold_bookings"] = sold;
                checks["unsold_bookings"] = active - sold;
                checks["total_active_value"] = Math.Round(totalValue, 2);
                checks["sell_rate_pct"] = active > 0 ? Math.Round((double)sold / active * 100, 1) : 0;
            }
        }

        report.Results["buyrooms"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  10. RESERVATIONS / CALLBACKS (skill: reservation-callback)
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckReservations(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Recent reservations
        using (var cmd = new SqlCommand(@"
            SELECT
                COUNT(*),
                SUM(CASE WHEN DateInsert >= DATEADD(DAY, -1, GETDATE()) THEN 1 ELSE 0 END),
                SUM(CASE WHEN DateInsert >= DATEADD(HOUR, -1, GETDATE()) THEN 1 ELSE 0 END),
                SUM(CASE WHEN ResStatus='Committed' AND DateInsert >= DATEADD(DAY, -1, GETDATE()) THEN 1 ELSE 0 END),
                SUM(CASE WHEN ResStatus='Cancelled' AND DateInsert >= DATEADD(DAY, -1, GETDATE()) THEN 1 ELSE 0 END)
            FROM Med_Reservation", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                checks["total_reservations"] = rdr.GetInt32(0);
                checks["last_24h"] = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                checks["last_hour"] = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                checks["committed_24h"] = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
                checks["cancelled_24h"] = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4);
            }
        }

        // Last reservation time (callback freshness)
        using (var cmd = new SqlCommand("SELECT TOP 1 DateInsert FROM Med_Reservation ORDER BY Id DESC", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var last = rdr.GetDateTime(0);
                var minAgo = (DateTime.Now - last).TotalMinutes;
                checks["last_reservation_time"] = last.ToString("yyyy-MM-dd HH:mm:ss");
                checks["minutes_since_last_reservation"] = Math.Round(minAgo, 1);
                // Callbacks should come in regularly
                if (minAgo > 360)
                    report.Alerts.Add(new MonitorAlert("WARNING", "Reservations", $"No reservation callbacks for {minAgo:F0} minutes"));
            }
        }

        // Revenue last 24h
        using (var cmd = new SqlCommand(@"
            SELECT SUM(ISNULL(AmountAfterTax, 0))
            FROM Med_Reservation
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE()) AND ResStatus IN ('Committed','New')", conn))
        {
            cmd.CommandTimeout = 10;
            var val = await cmd.ExecuteScalarAsync();
            checks["revenue_24h"] = val is DBNull || val == null ? 0 : Math.Round(Convert.ToDouble(val), 2);
        }

        report.Results["reservations"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  11. PRICE OVERRIDE PIPELINE (skill: price-override)
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckPriceOverridePipeline(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        using (var cmd = new SqlCommand(@"
            SELECT
                COUNT(*),
                SUM(CASE WHEN IsActive=1 AND PushStatus IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsActive=1 AND PushStatus='success' THEN 1 ELSE 0 END),
                SUM(CASE WHEN PushStatus='failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN IsActive=1 AND PushStatus IS NULL AND DateInsert < DATEADD(HOUR, -2, GETDATE()) THEN 1 ELSE 0 END)
            FROM [SalesOffice.PriceOverride]", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var total = rdr.GetInt32(0);
                var pending = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                var pushed = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                var failed = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
                var stale = rdr.IsDBNull(4) ? 0 : rdr.GetInt32(4);
                checks["total"] = total;
                checks["pending"] = pending;
                checks["pushed_success"] = pushed;
                checks["failed"] = failed;
                checks["stale_pending"] = stale;
                checks["success_rate"] = total > 0 ? Math.Round((double)pushed / Math.Max(1, pushed + failed) * 100, 1) : 100;

                if (stale > 0)
                    report.Alerts.Add(new MonitorAlert("WARNING", "PriceOverride", $"{stale} overrides pending >2 hours — pipeline may be stuck"));
                if (failed > 0 && total > 0 && (double)failed / total * 100 > 20)
                    report.Alerts.Add(new MonitorAlert("WARNING", "PriceOverride", $"High failure rate: {failed}/{total} ({(double)failed / total * 100:F0}%)"));
            }
        }

        // Recent override activity (last 24h)
        using (var cmd = new SqlCommand(@"
            SELECT COUNT(*) FROM [SalesOffice.PriceOverride]
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())", conn))
        {
            checks["created_24h"] = (int)(await cmd.ExecuteScalarAsync())!;
        }

        report.Results["price_override_pipeline"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  12. DATA FRESHNESS (skill: hotel-data-explorer, scanning)
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckDataFreshness(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Last scan log entry
        using (var cmd = new SqlCommand("SELECT TOP 1 DateCreated FROM [SalesOffice.Log] ORDER BY Id DESC", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var last = rdr.GetDateTime(0);
                checks["last_scan_log"] = last.ToString("yyyy-MM-dd HH:mm:ss");
                checks["scan_log_age_minutes"] = Math.Round((DateTime.Now - last).TotalMinutes, 1);
            }
        }

        // Last detail entry (price data freshness)
        using (var cmd = new SqlCommand("SELECT TOP 1 DateInsert FROM [SalesOffice.Details] ORDER BY Id DESC", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var last = rdr.GetDateTime(0);
                var ageMin = (DateTime.Now - last).TotalMinutes;
                checks["last_detail_time"] = last.ToString("yyyy-MM-dd HH:mm:ss");
                checks["detail_age_minutes"] = Math.Round(ageMin, 1);
                if (ageMin > 60)
                    report.Alerts.Add(new MonitorAlert("WARNING", "DataFreshness", $"Price data stale: last detail {ageMin:F0} minutes ago"));
            }
        }

        // Price changes in last hours (data_explorer coverage)
        using (var cmd = new SqlCommand(@"
            SELECT
                (SELECT COUNT(*) FROM [SalesOffice.Log] WHERE Message LIKE '%RoomPrice%' AND DateCreated >= DATEADD(HOUR, -1, GETDATE())),
                (SELECT COUNT(*) FROM [SalesOffice.Log] WHERE Message LIKE '%RoomPrice%' AND DateCreated >= DATEADD(DAY, -1, GETDATE()))", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                checks["price_changes_last_hour"] = rdr.GetInt32(0);
                checks["price_changes_last_24h"] = rdr.GetInt32(1);
            }
        }

        // Hotels actively being scanned
        using (var cmd = new SqlCommand(@"
            SELECT COUNT(DISTINCT DestinationId) FROM [SalesOffice.Orders]
            WHERE IsActive=1 AND WebJobStatus LIKE '%Completed%'
            AND DateInsert >= DATEADD(DAY, -1, GETDATE())", conn))
        {
            cmd.CommandTimeout = 10;
            try
            {
                checks["hotels_scanned_24h"] = (int)(await cmd.ExecuteScalarAsync())!;
            }
            catch { checks["hotels_scanned_24h"] = "query_error"; }
        }

        report.Results["data_freshness"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  13. BOOKING & SALES (skills: sales-management, insert-opp)
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> CheckBookingSales(SqlConnection conn, MonitorReport? report = null)
    {
        report ??= new MonitorReport();
        var checks = new Dictionary<string, object?>();

        // Booking health: bought vs sold conversion
        using (var cmd = new SqlCommand(@"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN IsSold=1 THEN 1 ELSE 0 END) as sold,
                SUM(CASE WHEN IsActive=1 AND IsSold=0 AND CancellationTo <= DATEADD(DAY, 2, GETDATE()) THEN 1 ELSE 0 END) as expiring_soon,
                SUM(CASE WHEN IsActive=0 AND IsSold=0 THEN 1 ELSE 0 END) as wasted
            FROM MED_Book WHERE DateInsert >= DATEADD(DAY, -7, GETDATE())", conn))
        {
            cmd.CommandTimeout = 15;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                var total = rdr.GetInt32(0);
                var sold = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                var expiringSoon = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                var wasted = rdr.IsDBNull(3) ? 0 : rdr.GetInt32(3);
                checks["bought_7d"] = total;
                checks["sold_7d"] = sold;
                checks["conversion_7d_pct"] = total > 0 ? Math.Round((double)sold / total * 100, 1) : 0;
                checks["expiring_48h"] = expiringSoon;
                checks["wasted_7d"] = wasted;

                if (expiringSoon > 10)
                    report.Alerts.Add(new MonitorAlert("WARNING", "Sales", $"{expiringSoon} bookings expiring in 48h without sale"));
            }
        }

        // P&L snapshot
        using (var cmd = new SqlCommand(@"
            SELECT
                SUM(b.Price) as buy_total,
                SUM(ISNULL(r.AmountAfterTax, 0)) as sell_total
            FROM MED_Book b
            LEFT JOIN Med_Reservation r ON r.HotelCode = CAST(b.HotelId AS NVARCHAR(20))
                AND r.Datefrom = b.StartDate AND r.Dateto = b.EndDate
                AND r.ResStatus IN ('Committed', 'New')
            WHERE b.IsActive=1 AND b.IsSold=1 AND b.Price IS NOT NULL
              AND b.DateInsert >= DATEADD(DAY, -7, GETDATE())", conn))
        {
            cmd.CommandTimeout = 15;
            try
            {
                using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    var buy = rdr.IsDBNull(0) ? 0.0 : rdr.GetDouble(0);
                    var sell = rdr.IsDBNull(1) ? 0.0 : rdr.GetDouble(1);
                    checks["buy_total_7d"] = Math.Round(buy, 2);
                    checks["sell_total_7d"] = Math.Round(sell, 2);
                    checks["profit_7d"] = Math.Round(sell - buy, 2);
                    checks["margin_pct"] = buy > 0 ? Math.Round((sell - buy) / buy * 100, 1) : 0;

                    if (sell < buy && buy > 0)
                        report.Alerts.Add(new MonitorAlert("WARNING", "Sales", $"Negative margin last 7d: ${sell - buy:F0} ({(sell - buy) / buy * 100:F1}%)"));
                }
            }
            catch (Exception ex) { checks["pnl_error"] = ex.Message[..Math.Min(60, ex.Message.Length)]; }
        }

        // Insert Opp activity (skill: insert-opp)
        using (var cmd = new SqlCommand(@"
            SELECT COUNT(*),
                SUM(CASE WHEN Status=1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN Status=0 OR Status IS NULL THEN 1 ELSE 0 END)
            FROM BackOfficeOPT
            WHERE DateInsert >= DATEADD(DAY, -1, GETDATE())", conn))
        {
            cmd.CommandTimeout = 10;
            using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                checks["opportunities_24h"] = rdr.GetInt32(0);
                checks["opp_active"] = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                checks["opp_pending"] = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
            }
        }

        report.Results["booking_sales"] = checks;
        return checks;
    }

    // ══════════════════════════════════════════════════════════════
    //  TREND ANALYSIS
    // ══════════════════════════════════════════════════════════════

    public MonitorTrend GetTrendAnalysis(int hours = 24)
    {
        lock (_historyLock)
        {
            var cutoff = DateTime.UtcNow.AddHours(-hours);
            var recent = _runHistory.Where(r => r.Timestamp > cutoff).ToList();

            var totalRuns = recent.Count;
            var healthyRuns = recent.Count(r => r.AlertCount == 0);
            var healthPct = totalRuns > 0 ? Math.Round((double)healthyRuns / totalRuns * 100, 1) : 0;

            // Component stats
            var componentStats = new Dictionary<string, MonitorComponentTrend>();
            foreach (var run in recent)
            {
                foreach (var alert in run.Alerts)
                {
                    if (!componentStats.ContainsKey(alert.Component))
                        componentStats[alert.Component] = new MonitorComponentTrend();
                    componentStats[alert.Component].Total++;
                    if (!componentStats[alert.Component].BySeverity.ContainsKey(alert.Severity))
                        componentStats[alert.Component].BySeverity[alert.Severity] = 0;
                    componentStats[alert.Component].BySeverity[alert.Severity]++;
                }
            }

            // Consecutive CRITICALs per component
            foreach (var (comp, stats) in componentStats)
            {
                var recentAlerts = recent.SelectMany(r => r.Alerts).Where(a => a.Component == comp).OrderByDescending(a => a.Timestamp).Take(10).ToList();
                var consecutive = 0;
                foreach (var a in recentAlerts)
                {
                    if (a.Severity == "CRITICAL") consecutive++;
                    else break;
                }
                stats.ConsecutiveCritical = consecutive;
            }

            // First vs second half
            var mid = DateTime.UtcNow.AddHours(-hours / 2.0);
            var firstHalf = recent.Where(r => r.Timestamp <= mid).Sum(r => r.AlertCount);
            var secondHalf = recent.Where(r => r.Timestamp > mid).Sum(r => r.AlertCount);

            string overallTrend;
            if (firstHalf > 0 && secondHalf > firstHalf * 1.5) overallTrend = "DEGRADING";
            else if (secondHalf == 0 && totalRuns > 2) overallTrend = "IMPROVING";
            else overallTrend = "STABLE";

            return new MonitorTrend
            {
                PeriodHours = hours,
                TotalRuns = totalRuns,
                HealthyRuns = healthyRuns,
                HealthPct = healthPct,
                OverallTrend = overallTrend,
                FirstHalfAlerts = firstHalf,
                SecondHalfAlerts = secondHalf,
                Components = componentStats,
            };
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  ESCALATION
    // ══════════════════════════════════════════════════════════════

    private void ApplyEscalation(MonitorReport report)
    {
        var trend = GetTrendAnalysis(6);
        for (int i = 0; i < report.Alerts.Count; i++)
        {
            var alert = report.Alerts[i];
            if (alert.Severity != "CRITICAL") continue;

            if (trend.Components.TryGetValue(alert.Component, out var stats) && stats.ConsecutiveCritical >= EscalationConsecutiveThreshold)
            {
                report.Alerts[i] = new MonitorAlert("EMERGENCY", alert.Component, $"[ESCALATED x{stats.ConsecutiveCritical + 1}] {alert.Message}");
                _logger.LogWarning("Escalation: {Component} has {Count} consecutive CRITICAL alerts → EMERGENCY", alert.Component, stats.ConsecutiveCritical + 1);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  HISTORY PERSISTENCE
    // ══════════════════════════════════════════════════════════════

    private string HistoryFilePath => Path.Combine(AppContext.BaseDirectory, "monitor-history.json");

    private void SaveRunToHistory(MonitorReport report)
    {
        lock (_historyLock)
        {
            _runHistory.Add(new MonitorRunHistory
            {
                Timestamp = report.Timestamp,
                AlertCount = report.Alerts.Count,
                Alerts = report.Alerts.Select(a => new MonitorAlertSummary { Severity = a.Severity, Component = a.Component, Message = a.Message, Timestamp = a.Timestamp }).ToList()
            });

            if (_runHistory.Count > MaxHistory)
                _runHistory.RemoveRange(0, _runHistory.Count - MaxHistory);
        }

        try
        {
            var json = JsonSerializer.Serialize(_runHistory, new JsonSerializerOptions { WriteIndented = false });
            File.WriteAllText(HistoryFilePath, json);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Failed to persist monitor history: {Err}", ex.Message);
        }
    }

    private void LoadHistory()
    {
        try
        {
            if (!File.Exists(HistoryFilePath)) return;
            var json = File.ReadAllText(HistoryFilePath);
            var history = JsonSerializer.Deserialize<List<MonitorRunHistory>>(json);
            if (history != null)
            {
                lock (_historyLock)
                {
                    _runHistory.Clear();
                    _runHistory.AddRange(history);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Failed to load monitor history: {Err}", ex.Message);
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  SINGLE CHECK (by name)
    // ══════════════════════════════════════════════════════════════

    public async Task<Dictionary<string, object?>> RunSingleCheck(string checkName)
    {
        using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        var report = new MonitorReport { Timestamp = DateTime.UtcNow };

        var result = checkName.ToLower() switch
        {
            "webjob" => await CheckWebjob(conn, report),
            "tables" => await CheckTables(conn, report),
            "mapping" => await CheckMapping(conn, report),
            "skills" => await CheckSkills(conn, report),
            "orders" => await CheckOrders(conn, report),
            "zenith" => await CheckZenith(report),
            "cancellation" => await CheckCancellation(conn, report),
            "cancel_errors" => await CheckCancelErrors(conn, report),
            "buyrooms" => await CheckBuyRooms(conn, report),
            "reservations" => await CheckReservations(conn, report),
            "price_override_pipeline" => await CheckPriceOverridePipeline(conn, report),
            "data_freshness" => await CheckDataFreshness(conn, report),
            "booking_sales" => await CheckBookingSales(conn, report),
            _ => throw new ArgumentException($"Unknown check: {checkName}. Available: webjob, tables, mapping, skills, orders, zenith, cancellation, cancel_errors, buyrooms, reservations, price_override_pipeline, data_freshness, booking_sales")
        };

        return new Dictionary<string, object?>
        {
            ["check"] = checkName,
            ["result"] = result,
            ["alerts"] = report.Alerts,
            ["timestamp"] = DateTime.UtcNow,
        };
    }

    public List<MonitorRunHistory> GetHistory(int last = 50)
    {
        lock (_historyLock)
        {
            return _runHistory.OrderByDescending(r => r.Timestamp).Take(last).ToList();
        }
    }

    private async Task SafeRun(MonitorReport report, string checkName, Func<SqlConnection, Task> action)
    {
        try
        {
            using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            await action(conn);
        }
        catch (Exception ex)
        {
            RecordCheckFailure(report, checkName, ex);
        }
    }

    private async Task SafeRun(MonitorReport report, string checkName, Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            RecordCheckFailure(report, checkName, ex);
        }
    }

    private void RecordCheckFailure(MonitorReport report, string checkName, Exception ex)
    {
        var error = ex.Message[..Math.Min(120, ex.Message.Length)];
        report.Results[checkName] = new Dictionary<string, object?>
        {
            ["status"] = "ERROR",
            ["error"] = error,
        };
        report.Alerts.Add(new MonitorAlert("CRITICAL", checkName, $"Monitor check failed: {error}"));
        _logger.LogWarning(ex, "SystemMonitor check {CheckName} failed", checkName);
    }
}

// ══════════════════════════════════════════════════════════════
//  MODELS
// ══════════════════════════════════════════════════════════════

public class MonitorReport
{
    public DateTime Timestamp { get; set; }
    public string? Error { get; set; }
    public Dictionary<string, object?> Results { get; set; } = new();
    public List<MonitorAlert> Alerts { get; set; } = new();
    public MonitorTrend? Trend { get; set; }
}

public class MonitorAlert
{
    public string Severity { get; set; }
    public string Component { get; set; }
    public string Message { get; set; }
    public DateTime Timestamp { get; set; }

    public MonitorAlert() { Severity = ""; Component = ""; Message = ""; }
    public MonitorAlert(string severity, string component, string message)
    {
        Severity = severity; Component = component; Message = message; Timestamp = DateTime.UtcNow;
    }
}

public class MonitorTrend
{
    public int PeriodHours { get; set; }
    public int TotalRuns { get; set; }
    public int HealthyRuns { get; set; }
    public double HealthPct { get; set; }
    public string OverallTrend { get; set; } = "STABLE";
    public int FirstHalfAlerts { get; set; }
    public int SecondHalfAlerts { get; set; }
    public Dictionary<string, MonitorComponentTrend> Components { get; set; } = new();
}

public class MonitorComponentTrend
{
    public int Total { get; set; }
    public Dictionary<string, int> BySeverity { get; set; } = new();
    public int ConsecutiveCritical { get; set; }
}

public class MonitorRunHistory
{
    public DateTime Timestamp { get; set; }
    public int AlertCount { get; set; }
    public List<MonitorAlertSummary> Alerts { get; set; } = new();
}

public class MonitorAlertSummary
{
    public string Severity { get; set; } = "";
    public string Component { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime Timestamp { get; set; }
}
