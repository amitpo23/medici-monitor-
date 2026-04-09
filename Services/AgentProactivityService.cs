using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Proactive agent notifications — agents speak on their own when state changes.
/// Runs every 5 minutes. Template-based (no Claude API calls).
/// Checks Agent API (port 5050) for state changes and pushes to Telegram.
/// </summary>
public class AgentProactivityService : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly ILogger<AgentProactivityService> _logger;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(10) };

    // State tracking — detect changes, not just thresholds
    private string? _lastThreatLevel;
    private double _lastMissRate = -1;
    private int _lastFailedOrders;
    private string? _lastSellerReport;
    private int _lastGapCount = -1;
    private DateTime _lastControlRoomSummary = DateTime.MinValue;

    // Cooldown per agent (1 hour)
    private readonly Dictionary<string, DateTime> _agentCooldown = new();
    private static readonly TimeSpan AgentCooldown = TimeSpan.FromHours(1);

    private string _botToken = "";
    private string _chatId = "";

    public AgentProactivityService(IConfiguration config, ILogger<AgentProactivityService> logger)
    {
        _config = config;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _botToken = _config["Notifications:TelegramBotToken"] ?? "";
        _chatId = _config["Notifications:TelegramChatId"] ?? "";

        if (string.IsNullOrEmpty(_botToken) || string.IsNullOrEmpty(_chatId))
        {
            _logger.LogInformation("AgentProactivity disabled — no Telegram config");
            return;
        }

        _logger.LogInformation("AgentProactivity started — checking every 5 min");

        // Wait for everything to warm up
        await Task.Delay(TimeSpan.FromMinutes(3), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckShimon();   // Safety threat level changes
                await CheckAmir();     // SOM miss rate / failed orders
                await CheckMichael();  // New mapping gaps
                await CheckYossi();    // Room seller pricing updates
                await CheckYael();     // Monitor health changes
                await CheckAryeh();    // Morning summary at 10:00 Israel
            }
            catch (Exception ex)
            {
                _logger.LogDebug("AgentProactivity error: {Err}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
        }
    }

    // ── שמעון (Safety Officer) — threat level changes ──

    private async Task CheckShimon()
    {
        try
        {
            var json = await _http.GetStringAsync("http://127.0.0.1:5050/safety");
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var threat = root.TryGetProperty("worst_threat", out var wt) ? wt.GetString() : "OK";

            if (_lastThreatLevel != null && threat != _lastThreatLevel && CanSpeak("שמעון"))
            {
                var direction = GetThreatOrder(threat) > GetThreatOrder(_lastThreatLevel) ? "עלתה" : "ירדה";
                var icon = threat switch { "CRITICAL" => "🚨", "HIGH" => "🟠", "WARNING" => "🟡", _ => "🟢" };

                // Get failed checks
                var failedChecks = new List<string>();
                if (root.TryGetProperty("checks", out var checks))
                    foreach (var c in checks.EnumerateArray())
                        if (c.TryGetProperty("passed", out var p) && !p.GetBoolean())
                            failedChecks.Add(c.TryGetProperty("check", out var cn) ? cn.GetString() ?? "?" : "?");

                var details = failedChecks.Any() ? $"\nבדיקות שנכשלו: {string.Join(", ", failedChecks)}" : "";
                await SendAgentMessage($"{icon} *שמעון (בטיחות):* רמת האיום {direction} ל-*{threat}* (היתה: {_lastThreatLevel}){details}");
                MarkSpoken("שמעון");
            }

            _lastThreatLevel = threat;
        }
        catch { /* Agent API might be down */ }
    }

    // ── אמיר (SOM) — miss rate threshold / failed orders ──

    private async Task CheckAmir()
    {
        try
        {
            var json = await _http.GetStringAsync("http://127.0.0.1:5050/scans");
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var missRate = root.TryGetProperty("miss_rate", out var mr) ? mr.GetDouble() : 0;
            var orders = root.TryGetProperty("orders", out var o) ? o : default;
            var failed = orders.ValueKind != JsonValueKind.Undefined && orders.TryGetProperty("failed", out var f) ? f.GetInt32() : 0;

            // Miss rate crossing thresholds (80%, 90%)
            if (_lastMissRate >= 0 && CanSpeak("אמיר"))
            {
                if (missRate >= 90 && _lastMissRate < 90)
                {
                    await SendAgentMessage($"🔴 *אמיר (SOM):* שיעור החסרים חצה 90%! כרגע *{missRate:F1}%* — רוב הסריקות לא ממופות.");
                    MarkSpoken("אמיר");
                }
                else if (missRate >= 80 && _lastMissRate < 80)
                {
                    await SendAgentMessage($"🟡 *אמיר (SOM):* שיעור החסרים חצה 80%. כרגע *{missRate:F1}%*.");
                    MarkSpoken("אמיר");
                }
            }

            // New failed orders
            if (_lastFailedOrders >= 0 && failed > _lastFailedOrders && CanSpeak("אמיר"))
            {
                await SendAgentMessage($"❌ *אמיר (SOM):* {failed - _lastFailedOrders} הזמנה/ות חדשות נכשלו! סה\"כ failed: {failed}");
                MarkSpoken("אמיר");
            }

            _lastMissRate = missRate;
            _lastFailedOrders = failed;
        }
        catch { }
    }

    // ── מיכאל (Mapping Fixer) — new gaps ──

    private async Task CheckMichael()
    {
        try
        {
            var encodedName = Uri.EscapeDataString("מיכאל");
            var json = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("data", out var data) && data.TryGetProperty("phases", out var phases)
                && phases.TryGetProperty("phase2", out var p2) && p2.TryGetProperty("total", out var total))
            {
                var gapCount = total.GetInt32();
                if (_lastGapCount >= 0 && gapCount > _lastGapCount + 5 && CanSpeak("מיכאל"))
                {
                    var diff = gapCount - _lastGapCount;
                    await SendAgentMessage($"🗺️ *מיכאל (מיפויים):* זיהיתי {diff} gaps חדשים. סה\"כ: {gapCount}.");
                    MarkSpoken("מיכאל");
                }
                _lastGapCount = gapCount;
            }
        }
        catch { }
    }

    // ── אריה (Control Room) — morning summary at 10:00 Israel ──

    private async Task CheckAryeh()
    {
        var israelHour = DateTime.UtcNow.AddHours(3).Hour;
        var today = DateTime.UtcNow.Date;

        if (israelHour != 10 || _lastControlRoomSummary.Date == today) return;
        _lastControlRoomSummary = DateTime.UtcNow;

        try
        {
            var json = await _http.GetStringAsync("http://127.0.0.1:5050/agents");
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (!root.TryGetProperty("agents", out var agents)) return;

            int active = 0, stale = 0, noReport = 0;
            var staleNames = new List<string>();

            foreach (var agent in agents.EnumerateArray())
            {
                var name = agent.TryGetProperty("name", out var n) ? n.GetString() ?? "?" : "?";
                var isStale = agent.TryGetProperty("stale", out var s) && s.GetBoolean();
                var hasReport = agent.TryGetProperty("last_report", out var lr) && lr.ValueKind != JsonValueKind.Null;

                if (!hasReport) noReport++;
                else if (isStale) { stale++; staleNames.Add(name); }
                else active++;
            }

            var sb = new StringBuilder();
            sb.AppendLine($"🏢 *אריה (חדר בקרה) — סיכום בוקר*");
            sb.AppendLine($"  🟢 {active} פעילים | 🔴 {stale} stale | ⚪ {noReport} ללא דוח");
            if (staleNames.Any())
                sb.AppendLine($"  תקועים: {string.Join(", ", staleNames)}");

            // Add KPIs if available
            try
            {
                var crJson = await _http.GetStringAsync("http://127.0.0.1:5050/agent/" + Uri.EscapeDataString("אריה"));
                using var crDoc = JsonDocument.Parse(crJson);
                if (crDoc.RootElement.TryGetProperty("data", out var data) && data.TryGetProperty("kpis", out var kpis))
                {
                    var rooms = kpis.TryGetProperty("active_rooms", out var ar) ? ar.GetInt32() : 0;
                    var sold = kpis.TryGetProperty("sold_rooms", out var sr) ? sr.GetInt32() : 0;
                    var safety = kpis.TryGetProperty("safety_status", out var ss) ? ss.GetString() : "?";
                    sb.AppendLine($"  🏨 {rooms} חדרים ({sold} נמכרו) | בטיחות: {safety}");
                }
            }
            catch { }

            sb.AppendLine($"\n_בוקר טוב! 🌅_");
            await SendAgentMessage(sb.ToString());
        }
        catch { }
    }

    // ── יוסי (Room Seller) — pricing update completed ──

    private string? _lastYossiReport;
    private async Task CheckYossi()
    {
        try
        {
            var encodedName = Uri.EscapeDataString("יוסי");
            var json = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
            using var doc = JsonDocument.Parse(json);
            var reportFile = doc.RootElement.TryGetProperty("report_file", out var rf) ? rf.GetString() : null;

            if (reportFile != null && reportFile != _lastYossiReport && _lastYossiReport != null && CanSpeak("יוסי"))
            {
                // New report appeared — pricing cycle completed
                var stats = "";
                if (doc.RootElement.TryGetProperty("data", out var data) && data.TryGetProperty("stats", out var s))
                {
                    var total = s.TryGetProperty("total", out var t) ? t.GetInt32() : 0;
                    var updated = s.TryGetProperty("updated", out var u) ? u.GetInt32() : 0;
                    stats = $" סה\"כ {total} חדרים, {updated} עודכנו.";
                }
                await SendAgentMessage($"💰 *יוסי (מכירות):* סיימתי עדכון מחירים.{stats}");
                MarkSpoken("יוסי");
            }
            _lastYossiReport = reportFile;
        }
        catch { }
    }

    // ── יעל (System Monitor) — health changes ──

    private int _lastYaelAlertCount = -1;
    private async Task CheckYael()
    {
        try
        {
            var encodedName = Uri.EscapeDataString("יעל");
            var json = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("data", out var data))
            {
                // Count critical alerts from checks
                int alerts = 0;
                if (data.TryGetProperty("checks", out var checks))
                    foreach (var c in checks.EnumerateArray())
                        if (c.TryGetProperty("passed", out var p) && !p.GetBoolean()) alerts++;

                if (_lastYaelAlertCount >= 0 && alerts > _lastYaelAlertCount && alerts > 0 && CanSpeak("יעל"))
                {
                    await SendAgentMessage($"🖥️ *יעל (מוניטור):* מספר הכשלים עלה ל-{alerts} (היה {_lastYaelAlertCount}).");
                    MarkSpoken("יעל");
                }
                _lastYaelAlertCount = alerts;
            }
        }
        catch { }
    }

    // ── Helpers ──

    private bool CanSpeak(string agentName)
    {
        return !_agentCooldown.TryGetValue(agentName, out var last) || DateTime.UtcNow - last > AgentCooldown;
    }

    private void MarkSpoken(string agentName)
    {
        _agentCooldown[agentName] = DateTime.UtcNow;
    }

    private static int GetThreatOrder(string? threat) => threat switch
    {
        "CRITICAL" => 4, "HIGH" => 3, "WARNING" => 2, "MEDIUM" => 1, _ => 0
    };

    private async Task SendAgentMessage(string text)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/sendMessage";
            if (text.Length > 4000) text = text[..4000];
            var payload = JsonSerializer.Serialize(new
            {
                chat_id = _chatId,
                text,
                parse_mode = "Markdown",
                disable_web_page_preview = true
            });
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            await _http.PostAsync(url, content);
            _logger.LogInformation("Agent proactive message sent");
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Agent message send failed: {Err}", ex.Message);
        }
    }
}
