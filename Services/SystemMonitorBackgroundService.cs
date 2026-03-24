using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Background service that periodically runs the system monitor scan.
/// - Runs every 30 minutes (configurable)
/// - Scans on startup (after warmup)
/// - Sends Telegram alerts for CRITICAL/EMERGENCY findings
/// - Dispatches via NotificationService for all channels
/// </summary>
public class SystemMonitorBackgroundService : BackgroundService
{
    private readonly SystemMonitorService _monitor;
    private readonly NotificationService _notifications;
    private readonly IConfiguration _config;
    private readonly ILogger<SystemMonitorBackgroundService> _logger;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };
    private readonly int _intervalMinutes;
    private readonly string _predictionApiUrl;

    // Cooldown: don't spam Telegram with the same alert signature.
    private readonly Dictionary<string, DateTime> _lastTelegramAlertByKey = new(StringComparer.OrdinalIgnoreCase);
    private static readonly TimeSpan TelegramCooldown = TimeSpan.FromMinutes(15);

    public SystemMonitorBackgroundService(
        SystemMonitorService monitor,
        NotificationService notifications,
        IConfiguration config,
        ILogger<SystemMonitorBackgroundService> logger)
    {
        _monitor = monitor;
        _notifications = notifications;
        _config = config;
        _logger = logger;
        _intervalMinutes = config.GetValue<int?>("SystemMonitor:IntervalMinutes") ?? 30;
        _predictionApiUrl = config["Integration:PredictionApiUrl"] ?? "https://medici-prediction-api.azurewebsites.net";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SystemMonitor background service started — interval: {Min} min", _intervalMinutes);

        // Wait for app warmup
        await Task.Delay(TimeSpan.FromSeconds(20), stoppingToken);

        // Initial scan
        await RunScan("Startup");

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMinutes(_intervalMinutes), stoppingToken);
            await RunScan("Background");
        }
    }

    private async Task RunScan(string source)
    {
        try
        {
            var report = await _monitor.RunFullScan();
            var criticals = report.Alerts.Where(a => a.Severity is "CRITICAL" or "EMERGENCY").ToList();
            var warnings = report.Alerts.Where(a => a.Severity == "WARNING").ToList();

            if (criticals.Count > 0)
            {
                _logger.LogWarning("[SystemMonitor-{Source}] {Criticals} CRITICAL, {Warnings} WARNING alerts",
                    source, criticals.Count, warnings.Count);

                var criticalsToSend = criticals.Where(ShouldSendTelegramAlert).ToList();

                // Send Telegram alert directly (with per-alert cooldown)
                if (criticalsToSend.Count > 0)
                {
                    await SendTelegramMonitorAlert(report, criticalsToSend, warnings);
                    MarkTelegramAlertsSent(criticalsToSend);
                }

                // Send via NotificationService (all channels)
                try
                {
                    var alertMessages = criticals.Select(a => $"[{a.Severity}] {a.Component}: {a.Message}");
                    var message = $"🖥️ System Monitor — {criticals.Count} critical alerts:\n{string.Join("\n", alertMessages)}";
                    await _notifications.SendAsync("System Monitor Alert", message, "Critical", "SystemMonitor");
                }
                catch (Exception ex) { _logger.LogDebug("Notification send failed: {Err}", ex.Message); }
            }
            else if (warnings.Count > 0)
            {
                _logger.LogInformation("[SystemMonitor-{Source}] {Warnings} warnings, no criticals", source, warnings.Count);
            }
            else
            {
                _logger.LogDebug("[SystemMonitor-{Source}] All clear — 0 alerts", source);
            }

            // Push monitor results to Prediction API for confidence adjustments
            await PushToPrediction(report);
        }
        catch (Exception ex)
        {
            _logger.LogError("[SystemMonitor-{Source}] Scan error: {Err}", source, ex.Message);
        }
    }

    /// <summary>
    /// Send a formatted Telegram message directly when monitor finds critical issues.
    /// </summary>
    private async Task SendTelegramMonitorAlert(MonitorReport report, List<MonitorAlert> criticals, List<MonitorAlert> warnings)
    {
        var botToken = _config["Notifications:TelegramBotToken"] ?? "";
        var chatId = _config["Notifications:TelegramChatId"] ?? "";
        if (string.IsNullOrEmpty(botToken) || string.IsNullOrEmpty(chatId)) return;

        var sb = new StringBuilder();
        sb.AppendLine("🚨 *System Monitor — התראה אוטומטית*");
        sb.AppendLine($"🕐 {DateTime.UtcNow:dd/MM/yyyy HH:mm} UTC");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

        // Critical alerts
        if (criticals.Count > 0)
        {
            sb.AppendLine($"\n🔴 *{criticals.Count} התראות קריטיות:*");
            foreach (var a in criticals.Take(8))
            {
                var icon = a.Severity == "EMERGENCY" ? "🔴🔴" : "🔴";
                sb.AppendLine($"  {icon} *{a.Component}:* {a.Message}");
            }
        }

        // Warnings
        if (warnings.Count > 0)
        {
            sb.AppendLine($"\n🟡 *{warnings.Count} אזהרות:*");
            foreach (var a in warnings.Take(5))
                sb.AppendLine($"  🟡 *{a.Component}:* {a.Message}");
            if (warnings.Count > 5) sb.AppendLine($"  _...ועוד {warnings.Count - 5}_");
        }

        // Quick summary from results
        try
        {
            if (report.Results.TryGetValue("webjob", out var wjObj) && wjObj is Dictionary<string, object?> wj)
            {
                sb.AppendLine($"\n⚙️ *WebJob:* Active={wj.GetValueOrDefault("active_orders")} | Failed={wj.GetValueOrDefault("failed_orders")} | Cycle=~{wj.GetValueOrDefault("estimated_cycle_hours")}h");
            }
            if (report.Results.TryGetValue("zenith", out var zObj) && zObj is Dictionary<string, object?> z)
            {
                var zStatus = z.GetValueOrDefault("status")?.ToString() ?? "?";
                var zLatency = z.GetValueOrDefault("latency_ms")?.ToString() ?? "?";
                sb.AppendLine($"🌐 *Zenith:* {zStatus} ({zLatency}ms)");
            }
            if (report.Results.TryGetValue("cancellation", out var cObj) && cObj is Dictionary<string, object?> c)
            {
                sb.AppendLine($"❌ *Cancellation:* Active={c.GetValueOrDefault("active_bookings")} | Errors 24h={c.GetValueOrDefault("cancel_errors_24h")}");
            }
        }
        catch { /* non-critical formatting */ }

        // Trend
        if (report.Trend != null)
        {
            var trendIcon = report.Trend.OverallTrend switch { "DEGRADING" => "📉", "IMPROVING" => "📈", _ => "➡️" };
            sb.AppendLine($"\n{trendIcon} *טרנד:* {report.Trend.OverallTrend} | בריאות: {report.Trend.HealthPct}%");

            // Escalation warnings
            foreach (var (comp, stats) in report.Trend.Components)
            {
                if (stats.ConsecutiveCritical >= 3)
                    sb.AppendLine($"  ⚠️ *{comp}:* {stats.ConsecutiveCritical} CRITICALs רצופים — *ESCALATION*");
            }
        }

        sb.AppendLine("\n━━━━━━━━━━━━━━━━━━━━");
        sb.AppendLine("_/monitor לסריקה מלאה | /trend לטרנדים_");

        try
        {
            var url = $"https://api.telegram.org/bot{botToken}/sendMessage";
            var text = sb.ToString();
            if (text.Length > 4000) text = text[..4000] + "\n\n_...הודעה קוצרה_";
            var payload = JsonSerializer.Serialize(new { chat_id = chatId, text, parse_mode = "Markdown", disable_web_page_preview = true });
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            await _http.PostAsync(url, content);
            _logger.LogInformation("SystemMonitor Telegram alert sent ({Criticals} critical, {Warnings} warnings)", criticals.Count, warnings.Count);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Telegram send error: {Err}", ex.Message);
        }
    }

    private bool ShouldSendTelegramAlert(MonitorAlert alert)
    {
        var key = GetAlertCooldownKey(alert);
        return !_lastTelegramAlertByKey.TryGetValue(key, out var lastSent)
            || DateTime.UtcNow - lastSent > TelegramCooldown;
    }

    private void MarkTelegramAlertsSent(IEnumerable<MonitorAlert> alerts)
    {
        var now = DateTime.UtcNow;
        var staleCutoff = now - (TelegramCooldown * 2);

        foreach (var staleKey in _lastTelegramAlertByKey
            .Where(entry => entry.Value < staleCutoff)
            .Select(entry => entry.Key)
            .ToList())
        {
            _lastTelegramAlertByKey.Remove(staleKey);
        }

        foreach (var alert in alerts)
        {
            _lastTelegramAlertByKey[GetAlertCooldownKey(alert)] = now;
        }
    }

    private static string GetAlertCooldownKey(MonitorAlert alert)
        => $"{alert.Component}|{alert.Message}";

    /// <summary>
    /// Push monitor scan results to Prediction API so it can adjust confidence levels.
    /// POST /api/v1/salesoffice/monitor/ingest
    /// </summary>
    private async Task PushToPrediction(MonitorReport report)
    {
        if (string.IsNullOrEmpty(_predictionApiUrl)) return;

        try
        {
            var payload = JsonSerializer.Serialize(new
            {
                source = "medici-monitor",
                timestamp = report.Timestamp,
                alerts = report.Alerts.Select(a => new { a.Severity, a.Component, a.Message }),
                results = report.Results,
                trend = report.Trend != null ? new
                {
                    report.Trend.OverallTrend,
                    report.Trend.HealthPct,
                    report.Trend.TotalRuns,
                    report.Trend.FirstHalfAlerts,
                    report.Trend.SecondHalfAlerts
                } : null
            });

            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            var url = $"{_predictionApiUrl.TrimEnd('/')}/api/v1/salesoffice/monitor/ingest";
            var response = await _http.PostAsync(url, content);

            if (response.IsSuccessStatusCode)
                _logger.LogInformation("[SystemMonitor] Pushed scan results to Prediction API");
            else
                _logger.LogWarning("[SystemMonitor] Prediction API ingest returned {Status}", response.StatusCode);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("[SystemMonitor] Prediction push failed (non-critical): {Err}", ex.Message);
        }
    }
}
