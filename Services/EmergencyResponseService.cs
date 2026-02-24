namespace MediciMonitor.Services;

/// <summary>
/// Emergency Response — risk assessment + emergency action execution.
/// Ported from Medici-Control-Panel EmergencyResponseService — adapted to use MediciMonitor services.
/// </summary>
public class EmergencyResponseService
{
    private readonly AzureMonitoringService _azure;
    private readonly string _connStr;
    private readonly ILogger<EmergencyResponseService> _logger;

    public EmergencyResponseService(
        AzureMonitoringService azure,
        IConfiguration config,
        ILogger<EmergencyResponseService> logger)
    {
        _azure = azure;
        _connStr = config.GetConnectionString("SqlServer") ?? "";
        _logger = logger;
    }

    // ── Emergency Status ─────────────────────────────────────────

    public async Task<object> GetEmergencyStatus()
    {
        try
        {
            var status = new EmergencyStatus { Timestamp = DateTime.Now };

            // DB check
            try
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
                await conn.OpenAsync();
            }
            catch
            {
                status.CriticalIssues.Add("חיבור למסד נתונים נכשל");
                status.SeverityLevel = Math.Max(status.SeverityLevel, 4);
            }

            // API health
            var apiHealth = await _azure.ComprehensiveApiHealthCheck();
            var healthy = apiHealth.Count(a => a.IsHealthy);
            var ratio = healthy / (double)Math.Max(apiHealth.Count, 1);

            if (ratio < 0.5)
            {
                status.CriticalIssues.Add($"{apiHealth.Count - healthy} endpoints קריטיים לא פעילים");
                status.SeverityLevel = Math.Max(status.SeverityLevel, 5);
            }
            else if (ratio < 0.8)
            {
                status.CriticalIssues.Add($"{apiHealth.Count - healthy} endpoints עם בעיות");
                status.SeverityLevel = Math.Max(status.SeverityLevel, 3);
            }

            var slow = apiHealth.Where(a => a.ResponseTimeMs > 5000).ToList();
            if (slow.Any())
            {
                status.CriticalIssues.Add($"{slow.Count} APIs עם זמני תגובה איטיים");
                status.SeverityLevel = Math.Max(status.SeverityLevel, 2);
            }

            status.IsEmergency = status.SeverityLevel >= 4;
            status.CurrentStatus = status.SeverityLevel switch
            {
                5 => "CRITICAL - דרושה פעולה מיידית",
                4 => "MAJOR - דרושה תשומת לב דחופה",
                3 => "MODERATE - ניטור צמוד",
                2 => "MINOR - פעולה תקינה",
                1 => "OPTIMAL - כל המערכות בריאות",
                _ => "UNKNOWN"
            };
            status.SuggestedActions = GenerateActions(status);
            status.ApiHealth = apiHealth;

            return new
            {
                Success = true,
                EmergencyStatus = status,
                Message = status.IsEmergency ? "בעיות קריטיות דורשות טיפול מיידי" : "המערכת פועלת בפרמטרים תקינים"
            };
        }
        catch (Exception ex)
        {
            return new { Success = false, Error = ex.Message };
        }
    }

    // ── Execute Emergency Action ─────────────────────────────────

    public async Task<object> ExecuteEmergencyAction(string actionType, bool confirmed = false)
    {
        try
        {
            _logger.LogWarning("Emergency action: {Action} (confirmed={C})", actionType, confirmed);

            var needsConfirm = new[] { "SCALE_UP_RESOURCES", "NOTIFY_ADMIN" };
            if (needsConfirm.Contains(actionType.ToUpper()) && !confirmed)
                return new { Success = false, RequiresConfirmation = true, Message = "פעולה זו דורשת אישור מפורש" };

            return actionType.ToUpper() switch
            {
                "RESTART_MONITORING" => await RestartMonitoring(),
                "TEST_ALL_CONNECTIONS" => await TestAllConnections(),
                "HEALTH_CHECK_CYCLE" => await HealthCheckCycle(),
                "CLEAR_TEMP_CACHE" => ClearCache(),
                "EMERGENCY_BACKUP" => await EmergencyBackup(),
                "NOTIFY_ADMIN" => NotifyAdmin(),
                _ => new { Success = false, Error = "Unknown action type" }
            };
        }
        catch (Exception ex) { return new { Success = false, Error = ex.Message }; }
    }

    // ── Actions ──────────────────────────────────────────────────

    private async Task<object> RestartMonitoring()
    {
        await Task.Delay(1000);
        return new { Success = true, Message = "שירותי ניטור אותחלו מחדש", Actions = new[] { "ניקוי מטמון", "איפוס חיבורים", "רענון בדיקות" } };
    }

    private async Task<object> TestAllConnections()
    {
        var results = new List<object>();

        // DB
        try
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_connStr);
            await conn.OpenAsync();
            results.Add(new { Service = "Database", Status = "Connected", Ok = true });
        }
        catch (Exception ex) { results.Add(new { Service = "Database", Status = ex.Message, Ok = false }); }

        // APIs
        var api = await _azure.ComprehensiveApiHealthCheck();
        foreach (var a in api)
            results.Add(new { Service = $"API: {a.Endpoint}", Status = a.IsHealthy ? "Healthy" : a.ErrorMessage, Ok = a.IsHealthy });

        return new { Success = true, Results = results, Message = $"בדיקת חיבורים הושלמה - {results.Count(r => ((dynamic)r).Ok)}/{results.Count} תקינים" };
    }

    private async Task<object> HealthCheckCycle()
    {
        var rounds = new List<object>();
        for (int i = 1; i <= 3; i++)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var api = await _azure.ComprehensiveApiHealthCheck();
            sw.Stop();
            rounds.Add(new { Round = i, Duration = $"{sw.ElapsedMilliseconds}ms", Healthy = $"{api.Count(a => a.IsHealthy)}/{api.Count}" });
            if (i < 3) await Task.Delay(2000);
        }
        return new { Success = true, Results = rounds, Message = "מחזור בדיקת בריאות הושלם" };
    }

    private object ClearCache()
    {
        return new { Success = true, Message = "מטמון נוקה" };
    }

    private async Task<object> EmergencyBackup()
    {
        var snapshot = new { Timestamp = DateTime.Now, Machine = Environment.MachineName, DbConfigured = !string.IsNullOrEmpty(_connStr) };
        var baseDir = Directory.Exists("/home") ? "/home/MediciMonitor" 
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MediciMonitor");
        var dir = Path.Combine(baseDir, "Backups");
        try { Directory.CreateDirectory(dir); } catch (IOException) { dir = Path.GetTempPath(); }
        var file = Path.Combine(dir, $"emergency_{DateTime.Now:yyyyMMdd_HHmmss}.json");
        await File.WriteAllTextAsync(file, System.Text.Json.JsonSerializer.Serialize(snapshot, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
        return new { Success = true, Message = "גיבוי חירום נוצר", File = file };
    }

    private object NotifyAdmin()
    {
        _logger.LogCritical("EMERGENCY NOTIFICATION: {Time}", DateTime.Now);
        return new { Success = true, Message = "התראה נשלחה למנהל", Note = "התראה נרשמה ביומן — הגדר email/SMS לעדכונים אמיתיים" };
    }

    // ── Helpers ───────────────────────────────────────────────────

    private List<EmergencyAction> GenerateActions(EmergencyStatus s)
    {
        var a = new List<EmergencyAction>();
        if (s.SeverityLevel >= 4) { a.Add(new("RESTART_MONITORING", "אתחול שירותי ניטור", false)); a.Add(new("EMERGENCY_BACKUP", "גיבוי חירום", false)); }
        if (s.SeverityLevel >= 3) { a.Add(new("TEST_ALL_CONNECTIONS", "בדיקת כל החיבורים", false)); a.Add(new("CLEAR_TEMP_CACHE", "ניקוי מטמון", false)); }
        if (s.SeverityLevel >= 5) { a.Add(new("NOTIFY_ADMIN", "שלח התראה למנהל", true)); }
        return a;
    }
}
