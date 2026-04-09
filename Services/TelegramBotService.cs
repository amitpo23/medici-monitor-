using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Telegram bot service — sends hourly reports and handles commands (kill switch, status).
/// Polls for commands every 30 seconds and sends comprehensive reports every hour.
/// </summary>
public class TelegramBotService : BackgroundService
{
    private readonly IConfiguration _config;
    private readonly ILogger<TelegramBotService> _logger;
    private readonly DataService _data;
    private readonly AlertingService _alerting;
    private readonly FailSafeService _failSafe;
    private readonly BookingReconciliationService _reconciliation;
    private readonly DeepVerificationService _deepVerify;
    private readonly AzureMonitoringService _azure;
    private readonly DatabaseHealthService _dbHealth;
    private readonly SlaTrackingService _sla;
    private readonly WebJobsMonitoringService _webJobs;
    private readonly NotificationService _notifications;
    private readonly InnstantApiClient _innstant;
    private readonly ClaudeAiService _claude;
    private readonly AuditService _audit;
    private readonly SystemMonitorService _monitor;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };

    // Watch mode (real-time booking alerts)
    private bool _watchEnabled = false;
    private double _watchMinAmount = 0;
    private int _lastReservationCount = -1;

    // Mute mode
    private DateTime? _muteUntil;

    // Oncall
    private string? _oncallName;

    // Pause (auto-release breakers)
    private DateTime? _pauseUntil;

    // Scheduled commands
    private readonly List<(int hour, int minute, string command)> _scheduledCommands = new();

    // Weekly summary tracking
    private DayOfWeek _lastWeeklySummaryDay = DayOfWeek.Saturday;

    private string _botToken = "";
    private string _chatId = "";
    private int _lastUpdateId = 0;
    private HashSet<string> _authorizedUsers = new();

    // State persistence
    private static readonly string BotStateFile = Path.Combine(AppContext.BaseDirectory, "bot-state.json");

    private void SaveBotState()
    {
        try
        {
            var state = new Dictionary<string, object?>
            {
                ["watchEnabled"] = _watchEnabled,
                ["watchMinAmount"] = _watchMinAmount,
                ["muteUntil"] = _muteUntil?.ToString("o"),
                ["pauseUntil"] = _pauseUntil?.ToString("o"),
                ["oncallName"] = _oncallName,
                ["scheduledCommands"] = _scheduledCommands.Select(s => new { s.hour, s.minute, s.command }).ToList(),
            };
            File.WriteAllText(BotStateFile, System.Text.Json.JsonSerializer.Serialize(state, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
        }
        catch (Exception ex) { _logger.LogDebug("SaveBotState failed: {Err}", ex.Message); }
    }

    private void LoadBotState()
    {
        try
        {
            if (!File.Exists(BotStateFile)) return;
            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(BotStateFile));
            var root = doc.RootElement;

            if (root.TryGetProperty("watchEnabled", out var we)) _watchEnabled = we.GetBoolean();
            if (root.TryGetProperty("watchMinAmount", out var wm)) _watchMinAmount = wm.GetDouble();
            if (root.TryGetProperty("oncallName", out var on) && on.ValueKind == System.Text.Json.JsonValueKind.String)
                _oncallName = on.GetString();
            if (root.TryGetProperty("muteUntil", out var mu) && mu.ValueKind == System.Text.Json.JsonValueKind.String
                && DateTime.TryParse(mu.GetString(), out var muDt) && muDt > DateTime.UtcNow)
                _muteUntil = muDt;
            if (root.TryGetProperty("pauseUntil", out var pu) && pu.ValueKind == System.Text.Json.JsonValueKind.String
                && DateTime.TryParse(pu.GetString(), out var puDt) && puDt > DateTime.UtcNow)
                _pauseUntil = puDt;
            if (root.TryGetProperty("scheduledCommands", out var sc) && sc.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                foreach (var cmd in sc.EnumerateArray())
                {
                    var h = cmd.TryGetProperty("hour", out var hh) ? hh.GetInt32() : 0;
                    var m = cmd.TryGetProperty("minute", out var mm) ? mm.GetInt32() : 0;
                    var c = cmd.TryGetProperty("command", out var cc) ? cc.GetString() ?? "" : "";
                    if (!string.IsNullOrEmpty(c)) _scheduledCommands.Add((h, m, c));
                }
            }
            _logger.LogInformation("Bot state restored from {File}", BotStateFile);
        }
        catch (Exception ex) { _logger.LogDebug("LoadBotState failed: {Err}", ex.Message); }
    }

    public TelegramBotService(
        IConfiguration config,
        ILogger<TelegramBotService> logger,
        DataService data,
        AlertingService alerting,
        FailSafeService failSafe,
        BookingReconciliationService reconciliation,
        DeepVerificationService deepVerify,
        AzureMonitoringService azure,
        DatabaseHealthService dbHealth,
        SlaTrackingService sla,
        WebJobsMonitoringService webJobs,
        NotificationService notifications,
        InnstantApiClient innstant,
        ClaudeAiService claude,
        AuditService audit,
        SystemMonitorService monitor)
    {
        _config = config;
        _logger = logger;
        _data = data;
        _alerting = alerting;
        _failSafe = failSafe;
        _reconciliation = reconciliation;
        _deepVerify = deepVerify;
        _azure = azure;
        _dbHealth = dbHealth;
        _sla = sla;
        _webJobs = webJobs;
        _notifications = notifications;
        _innstant = innstant;
        _claude = claude;
        _audit = audit;
        _monitor = monitor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _botToken = _config["Notifications:TelegramBotToken"] ?? "";
        _chatId = _config["Notifications:TelegramChatId"] ?? "";
        _authorizedUsers = (_config["Telegram:AuthorizedUsers"] ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToHashSet();
        LoadBotState();

        if (string.IsNullOrEmpty(_botToken) || string.IsNullOrEmpty(_chatId))
        {
            _logger.LogInformation("TelegramBotService disabled — no bot token or chat ID configured");
            return;
        }

        _logger.LogInformation("TelegramBotService started — hourly reports + command polling");

        // Wait for app to warm up
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        // Send startup message
        await SendToGroup("🟢 *MediciMonitor Online*\nהמערכת פעילה ומתחילה לנטר.\nדוח ראשון ייצא בעוד שעה.\n\nפקודות זמינות:\n/status — סטטוס מהיר\n/report — דוח מלא\n/alerts — התראות פעילות\n/monitor — סריקת מערכת (8 בדיקות)\n/reconcile — בדיקת התאמה\n/killswitch — הפעלת Kill Switch\n/breakers — סטטוס circuit breakers\n/help — עזרה");

        var lastReportTime = DateTime.UtcNow;
        var lastDashboardHour = -1;
        var lastDailySummaryDate = DateTime.MinValue.Date;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Poll for commands every 30 seconds
                await PollCommands();

                // Send 3-hourly report
                if (DateTime.UtcNow - lastReportTime >= TimeSpan.FromHours(3))
                {
                    await SendHourlyReport();
                    lastReportTime = DateTime.UtcNow;
                }

                // Hourly dashboards at :03 every hour (skip quiet hours 23:00-07:00 Israel)
                var nowUtc = DateTime.UtcNow;
                var israelHour = nowUtc.AddHours(3).Hour;
                var isQuietHours = israelHour >= 23 || israelHour < 7;
                if (nowUtc.Minute >= 3 && nowUtc.Minute < 6 && nowUtc.Hour != lastDashboardHour && !isQuietHours)
                {
                    lastDashboardHour = nowUtc.Hour;
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await SendDashboardAgents();
                            await Task.Delay(2000);
                            await SendDashboardSales();
                            await Task.Delay(2000);
                            await SendDashboardRisks();
                        }
                        catch (Exception ex) { _logger.LogWarning("Dashboard error: {Err}", ex.Message); }
                    });
                }

                // Daily summary at 07:00 UTC (10:00 Israel)
                if (DateTime.UtcNow.Hour == 7 && DateTime.UtcNow.Date > lastDailySummaryDate)
                {
                    await SendDailySummary();
                    lastDailySummaryDate = DateTime.UtcNow.Date;
                }

                // Weekly summary — Sunday at 08:00 UTC
                if (DateTime.UtcNow.DayOfWeek == DayOfWeek.Sunday && DateTime.UtcNow.Hour == 8
                    && _lastWeeklySummaryDay != DayOfWeek.Sunday)
                {
                    await SendWeeklySummary();
                    _lastWeeklySummaryDay = DayOfWeek.Sunday;
                }
                if (DateTime.UtcNow.DayOfWeek != DayOfWeek.Sunday) _lastWeeklySummaryDay = DateTime.UtcNow.DayOfWeek;

                // Watch mode — check for new reservations
                if (_watchEnabled) await CheckNewReservations();

                // Auto-release pause
                if (_pauseUntil.HasValue && DateTime.UtcNow >= _pauseUntil.Value)
                {
                    _failSafe.ResetAll("AutoPause-Expired");
                    await SendToGroup("✅ *הקפאה זמנית הסתיימה* — כל ה-breakers שוחררו אוטומטית");
                    _pauseUntil = null;
                }

                // Scheduled commands
                foreach (var sched in _scheduledCommands.ToList())
                {
                    if (DateTime.UtcNow.Hour == sched.hour && DateTime.UtcNow.Minute == sched.minute)
                        await PollCommands(); // Will process the scheduled command in next cycle
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("TelegramBotService error: {Err}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }

    // ── Command Polling ──────────────────────────────────────────

    private async Task PollCommands()
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/getUpdates?offset={_lastUpdateId + 1}&timeout=1";
            var resp = await _http.GetStringAsync(url);
            var doc = JsonDocument.Parse(resp);

            if (!doc.RootElement.GetProperty("ok").GetBoolean()) return;

            foreach (var update in doc.RootElement.GetProperty("result").EnumerateArray())
            {
                _lastUpdateId = update.GetProperty("update_id").GetInt32();

                // Handle button clicks (callback_query)
                if (update.TryGetProperty("callback_query", out var cbQuery))
                {
                    try { await HandleCallbackQuery(cbQuery); }
                    catch (Exception ex) { _logger.LogDebug("Callback error: {Err}", ex.Message); }
                    continue;
                }

                if (!update.TryGetProperty("message", out var msg)) continue;
                if (!msg.TryGetProperty("text", out var textEl)) continue;

                var text = textEl.GetString() ?? "";
                var chat = msg.GetProperty("chat");
                var chatId = chat.GetProperty("id").GetInt64().ToString();
                var userId = msg.TryGetProperty("from", out var fromEl)
                    ? fromEl.GetProperty("id").GetInt64().ToString() : "";
                var from = fromEl.ValueKind != JsonValueKind.Undefined
                    ? $"{fromEl.GetProperty("first_name").GetString()}" : "Unknown";

                // Handle commands (/) and natural language kill switch
                if (text.StartsWith("/"))
                {
                    var command = text.Split('@')[0].Split(' ')[0].ToLower();
                    _logger.LogInformation("Telegram command: {Cmd} from {From} (uid:{Uid})", command, from, userId);

                    // Auth check for sensitive commands
                    var sensitiveCommands = new[] { "/killswitch", "/trip", "/reset", "/cancel", "/pause", "/approve", "/reject" };
                    if (sensitiveCommands.Contains(command) && _authorizedUsers.Count > 0 && !_authorizedUsers.Contains(userId))
                    {
                        await SendToGroup($"🔒 אין הרשאה. פקודה זו מוגבלת למשתמשים מורשים בלבד.\nUser ID: {userId}", chatId);
                        _logger.LogWarning("Unauthorized command {Cmd} from {From} (uid:{Uid})", command, from, userId);
                        continue;
                    }

                    switch (command)
                    {
                        // ── Reports & Status ──
                        case "/status": await HandleStatus(chatId); break;
                        case "/report": await SendHourlyReport(chatId); break;
                        case "/daily_summary": await SendDailySummary(chatId); break;
                        case "/alerts": await HandleAlerts(chatId); break;

                        // ── Reconciliation ──
                        case "/reconcile": await HandleReconcile(chatId); break;

                        // ── Kill Switch & Breakers ──
                        case "/killswitch": await HandleKillSwitch(chatId, text, from); break;
                        case "/breakers": await HandleBreakers(chatId); break;
                        case "/trip": await HandleTrip(chatId, text, from); break;
                        case "/reset": await HandleReset(chatId, text, from); break;

                        // ── Flagged Items (Approve/Reject) ──
                        case "/flagged": await HandleFlagged(chatId); break;
                        case "/approve": await HandleApproveReject(chatId, text, from, true); break;
                        case "/reject": await HandleApproveReject(chatId, text, from, false); break;

                        // ── Financial & Business ──
                        case "/pnl": await HandlePnl(chatId, text); break;
                        case "/waste": await HandleWaste(chatId); break;
                        case "/bookings": await HandleRecentBookings(chatId, text); break;
                        case "/errors": await HandleErrors(chatId); break;

                        // ── Infrastructure ──
                        case "/health": await HandleApiHealth(chatId); break;
                        case "/sla": await HandleSla(chatId); break;
                        case "/db": await HandleDbHealth(chatId); break;
                        case "/webjobs": await HandleWebJobs(chatId); break;

                        // ── Scan & Check ──
                        case "/scan": await HandleFailSafeScan(chatId); break;
                        case "/trace": await HandleTrace(chatId, text); break;
                        case "/verify": await HandleVerifyBooking(chatId, text); break;

                        // ── Notifications ──
                        case "/test_notify": await HandleTestNotification(chatId); break;

                        // ── New: Hotel, Search, AI, Pause, Watch, Mute, Log, Oncall, Schedule, Cancel ──
                        case "/hotel": await HandleHotel(chatId, text); break;
                        case "/search": await HandleSearchHotel(chatId, text); break;
                        case "/ask": await HandleAskClaude(chatId, text); break;
                        case "/pause": await HandlePause(chatId, text, from); break;
                        case "/watch": await HandleWatch(chatId, text); break;
                        case "/mute": await HandleMute(chatId, text, from); break;
                        case "/log": await HandleLog(chatId, text, from); break;
                        case "/oncall": await HandleOncall(chatId, text); break;
                        case "/weekly_summary": await SendWeeklySummary(chatId); break;
                        case "/cancel": await HandleCancelBooking(chatId, text, from); break;
                        case "/deep_check": await HandleDeepCheck(chatId, text); break;
                        case "/anomalies": await HandleAnomalies(chatId); break;
                        case "/ghosts": await HandleGhosts(chatId); break;
                        case "/schedule": await HandleSchedule(chatId, text); break;

                        // ── System Monitor ──
                        case "/monitor": await HandleMonitorFull(chatId); break;
                        case "/monitor_check": await HandleMonitorCheck(chatId, text); break;
                        case "/tables": await HandleMonitorTables(chatId); break;
                        case "/mapping": await HandleMonitorMapping(chatId); break;
                        case "/trend": await HandleMonitorTrend(chatId, text); break;
                        case "/cancel_errors": await HandleCancelErrorAnalysis(chatId); break;
                        case "/zenith": await HandleZenithProbe(chatId); break;
                        case "/buyrooms": await HandleMonitorCheck(chatId, "/monitor_check buyrooms"); break;
                        case "/reservations_check": await HandleMonitorCheck(chatId, "/monitor_check reservations"); break;
                        case "/freshness": await HandleMonitorCheck(chatId, "/monitor_check data_freshness"); break;
                        case "/sales_check": await HandleMonitorCheck(chatId, "/monitor_check booking_sales"); break;
                        case "/overrides": await HandleMonitorCheck(chatId, "/monitor_check price_override_pipeline"); break;

                        // ── Dashboards ──
                        case "/dashboard": await SendDashboardAgents(chatId); await SendDashboardSales(chatId); await SendDashboardRisks(chatId); break;
                        case "/dash_agents": await SendDashboardAgents(chatId); break;
                        case "/dash_sales": await SendDashboardSales(chatId); break;
                        case "/dash_risks": await SendDashboardRisks(chatId); break;

                        // ── Agent API (medici-hotels) ──
                        case "/agents": await HandleAgentQuery(chatId, "/agents"); break;
                        case "/agent": await HandleAgentQuery(chatId, text); break;
                        case "/rooms": await HandleAgentQuery(chatId, "/rooms"); break;
                        case "/safety_report": await HandleAgentQuery(chatId, "/safety"); break;
                        case "/scans_report": await HandleAgentQuery(chatId, "/scans"); break;

                        // ── Agent Chat ──
                        case "/team": await HandleTeamMenu(chatId); break;
                        case "/talk": await HandleTalkToAgent(chatId, text); break;
                        case "/bye": _claude.EndAllConversations(chatId); await SendToGroup("👋 שיחות סוכנים נסגרו.", chatId); break;

                        // ── AI + Monitor ──
                        case "/ask_monitor": await HandleAskMonitor(chatId, text); break;
                        case "/analyse_monitor": await HandleAnalyseMonitor(chatId); break;

                        case "/help": await HandleHelp(chatId); break;
                    }
                }
                else
                {
                    // Natural language kill switch detection
                    await HandleNaturalLanguage(chatId, text, from);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Poll error: {Err}", ex.Message);
        }
    }

    // ── Hourly Report ────────────────────────────────────────────

    private async Task SendHourlyReport(string? targetChatId = null)
    {
        try
        {
            var sb = new StringBuilder();
            sb.AppendLine("📊 *דוח שעתי — MediciMonitor*");
            sb.AppendLine($"🕐 {DateTime.UtcNow:dd/MM/yyyy HH:mm} UTC");
            sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

            // System Status
            try
            {
                var status = await _data.GetFullStatus();
                sb.AppendLine();
                sb.AppendLine("*📋 סטטוס מערכת:*");
                sb.AppendLine($"  🗄️ DB: {(status.DbConnected ? "✅ מחובר" : "❌ מנותק!")}");
                sb.AppendLine($"  📦 הזמנות פעילות: *{status.TotalActiveBookings}*");
                sb.AppendLine($"  🔄 ביטולים תקועים: *{status.StuckCancellations}*");
                sb.AppendLine($"  📈 הזמנות עתידיות: *{status.FutureBookings}*");
                if (status.ReservationsToday > 0)
                    sb.AppendLine($"  📥 Reservations היום: *{status.ReservationsToday}*");
                if (status.WasteRoomsTotal > 0)
                    sb.AppendLine($"  💸 חדרים לא נמכרו: *{status.WasteRoomsTotal}*");
                if (status.TotalBought > 0 || status.TotalSold > 0)
                    sb.AppendLine($"  💰 קנינו: {status.TotalBought} | מכרנו: {status.TotalSold}");
            }
            catch (Exception ex)
            {
                sb.AppendLine($"\n⚠️ שגיאה בטעינת סטטוס: {ex.Message}");
            }

            // API Health
            try
            {
                var health = await _azure.ComprehensiveApiHealthCheck();
                var healthy = health.Count(h => h.IsHealthy);
                var total = health.Count;
                var icon = healthy == total ? "✅" : healthy > total / 2 ? "⚠️" : "❌";
                sb.AppendLine();
                sb.AppendLine($"*🌐 בריאות API:* {icon} {healthy}/{total} תקינים");
                foreach (var ep in health.Where(h => !h.IsHealthy))
                {
                    var name = ep.Endpoint.Split('(')[0].Trim();
                    sb.AppendLine($"  ❌ {name}");
                }
            }
            catch { sb.AppendLine("\n⚠️ לא ניתן לבדוק API"); }

            // Active Alerts
            try
            {
                var alerts = await _alerting.EvaluateAlerts();
                var critical = alerts.Count(a => a.Severity == "Critical");
                var warning = alerts.Count(a => a.Severity == "Warning");

                sb.AppendLine();
                if (critical > 0 || warning > 0)
                {
                    sb.AppendLine($"*🚨 התראות:* 🔴 {critical} קריטיות | 🟡 {warning} אזהרות");
                    foreach (var a in alerts.Where(a => a.Severity == "Critical").Take(5))
                        sb.AppendLine($"  🔴 {a.Title}");
                    foreach (var a in alerts.Where(a => a.Severity == "Warning").Take(3))
                        sb.AppendLine($"  🟡 {a.Title}");
                }
                else
                {
                    sb.AppendLine("*🚨 התראות:* ✅ אין התראות פעילות");
                }
            }
            catch { sb.AppendLine("\n⚠️ לא ניתן לבדוק התראות"); }

            // Reconciliation
            var recon = _reconciliation.LastReport;
            if (recon != null)
            {
                sb.AppendLine();
                var reconIcon = recon.CriticalMismatches > 0 ? "🔴" : recon.TotalMismatches > 0 ? "🟡" : "🟢";
                sb.AppendLine($"*🔍 התאמת הזמנות:* {reconIcon}");
                sb.AppendLine($"  Medici: {recon.MediciBookingsCount} | Zenith: {recon.MediciReservationsCount} | SO: {recon.SalesOrdersCount}");
                sb.AppendLine($"  אי-התאמות: *{recon.TotalMismatches}* ({recon.CriticalMismatches} קריטיות)");
                if (recon.Mismatches.Any())
                {
                    foreach (var m in recon.Mismatches.Take(3))
                        sb.AppendLine($"  {(m.Severity == "Critical" ? "🔴" : "🟡")} {m.Description}");
                }
            }

            // Circuit Breakers
            try
            {
                var breakers = _failSafe.GetBreakers();
                var openBreakers = breakers.Where(b => b.IsOpen).ToList();
                sb.AppendLine();
                if (openBreakers.Any())
                {
                    sb.AppendLine($"*🛑 Kill Switch:* {openBreakers.Count} breakers פתוחים!");
                    foreach (var b in openBreakers)
                        sb.AppendLine($"  🔴 {b.Name}: {b.Reason}");
                }
                else
                {
                    sb.AppendLine("*🛑 Kill Switch:* ✅ כל ה-breakers סגורים");
                }
            }
            catch { }

            // System Monitor
            try
            {
                var monReport = _monitor.LastReport;
                if (monReport != null)
                {
                    var monCrit = monReport.Alerts.Count(a => a.Severity is "CRITICAL" or "EMERGENCY");
                    var monWarn = monReport.Alerts.Count(a => a.Severity == "WARNING");
                    sb.AppendLine();
                    if (monCrit > 0 || monWarn > 0)
                    {
                        sb.AppendLine($"*🖥️ System Monitor:* 🔴 {monCrit} קריטיות | 🟡 {monWarn} אזהרות");
                        foreach (var a in monReport.Alerts.Where(a => a.Severity is "CRITICAL" or "EMERGENCY").Take(3))
                            sb.AppendLine($"  🔴 {a.Component}: {a.Message}");
                        foreach (var a in monReport.Alerts.Where(a => a.Severity == "WARNING").Take(2))
                            sb.AppendLine($"  🟡 {a.Component}: {a.Message}");
                    }
                    else
                    {
                        sb.AppendLine("*🖥️ System Monitor:* ✅ כל 8 הבדיקות תקינות");
                    }
                    // Trend
                    if (monReport.Trend != null)
                    {
                        var ti = monReport.Trend.OverallTrend switch { "DEGRADING" => "📉", "IMPROVING" => "📈", _ => "➡️" };
                        sb.AppendLine($"  {ti} טרנד: {monReport.Trend.OverallTrend} | בריאות: {monReport.Trend.HealthPct}%");
                    }
                    // Zenith status
                    if (monReport.Results.TryGetValue("zenith", out var zObj) && zObj is Dictionary<string, object?> z)
                    {
                        var zStatus = z.GetValueOrDefault("status")?.ToString() ?? "?";
                        var zLatency = z.GetValueOrDefault("latency_ms")?.ToString() ?? "?";
                        sb.AppendLine($"  🌐 Zenith: {zStatus} ({zLatency}ms)");
                    }
                    // Mapping gaps
                    if (monReport.Results.TryGetValue("mapping", out var mObj) && mObj is Dictionary<string, object?> m)
                    {
                        var gaps = m.GetValueOrDefault("order_detail_gaps");
                        var missRate = m.GetValueOrDefault("miss_rate_last_hour");
                        sb.AppendLine($"  🗺️ Mapping miss rate: {missRate}/h | BB: {m.GetValueOrDefault("hotels_with_bb")}");
                    }
                }
            }
            catch { }

            sb.AppendLine();
            sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");
            sb.AppendLine("_דוח הבא בעוד שעה | /help לפקודות_");

            await SendToGroup(sb.ToString(), targetChatId);
            _logger.LogInformation("Hourly Telegram report sent");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Failed to send hourly report: {Err}", ex.Message);
        }
    }

    // ── Dashboard 1: Agent Status ──────────────────────────────────

    private async Task SendDashboardAgents(string? targetChatId = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine("🤖 *דשבורד סוכנים*");
        sb.AppendLine($"🕐 {DateTime.UtcNow:dd/MM HH:mm} UTC");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

        try
        {
            var json = await _http.GetStringAsync("http://127.0.0.1:5050/agents");
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;

            var agents = root.GetProperty("agents");
            int active = 0, stale = 0, noReport = 0;

            foreach (var agent in agents.EnumerateArray())
            {
                var name = agent.GetProperty("name").GetString() ?? "?";
                var skill = agent.GetProperty("skill").GetString() ?? "?";
                var isStale = agent.TryGetProperty("stale", out var s) && s.GetBoolean();
                var hasReport = agent.TryGetProperty("last_report", out var lr) && lr.ValueKind != System.Text.Json.JsonValueKind.Null;
                var ageMin = agent.TryGetProperty("age_minutes", out var a) && a.ValueKind == System.Text.Json.JsonValueKind.Number
                    ? a.GetDouble() : -1;

                string icon, status;
                if (!hasReport) { icon = "⚪"; status = "ללא דוח"; noReport++; }
                else if (isStale) { icon = "🔴"; status = $"stale ({(int)ageMin}d)"; stale++; }
                else if (ageMin > 60) { icon = "🟡"; status = $"{(int)ageMin}d"; active++; }
                else { icon = "🟢"; status = $"{(int)ageMin}d"; active++; }

                sb.AppendLine($"  {icon} *{name}* ({skill}) — {status}");
            }

            sb.AppendLine();
            sb.AppendLine($"📊 סה\"כ: 🟢 {active} פעילים | 🔴 {stale} stale | ⚪ {noReport} ללא דוח");

            // Key metrics from specific agents
            sb.AppendLine();
            sb.AppendLine("*📊 מדדים מרכזיים:*");
            await AppendAgentKeyMetric(sb, "אמיר", j =>
            {
                if (j.TryGetProperty("data", out var d) && d.TryGetProperty("phases", out var p))
                {
                    var scans = p.TryGetProperty("scans", out var s) ? s : default;
                    var missRate = scans.ValueKind != System.Text.Json.JsonValueKind.Undefined && scans.TryGetProperty("miss_rate_pct", out var mr) ? mr.ToString() : "?";
                    var activeOrders = scans.ValueKind != System.Text.Json.JsonValueKind.Undefined && scans.TryGetProperty("active_orders", out var ao) ? ao.ToString() : "?";
                    return $"📋 SOM: miss rate {missRate}% | {activeOrders} orders";
                }
                return null;
            });
            await AppendAgentKeyMetric(sb, "שמעון", j =>
            {
                if (j.TryGetProperty("data", out var d))
                {
                    var threat = d.TryGetProperty("worst_threat", out var wt) ? wt.GetString() : "?";
                    var checks = d.TryGetProperty("checks", out var c) ? c.GetArrayLength() : 0;
                    var passed = 0;
                    if (d.TryGetProperty("checks", out var ch))
                        foreach (var ck in ch.EnumerateArray())
                            if (ck.TryGetProperty("passed", out var pp) && pp.GetBoolean()) passed++;
                    return $"🛡️ Safety: {threat} | {passed}/{checks} passed";
                }
                return null;
            });
            await AppendAgentKeyMetric(sb, "מיכאל", j =>
            {
                if (j.TryGetProperty("data", out var d) && d.TryGetProperty("phases", out var p) && p.TryGetProperty("phase2", out var p2))
                {
                    var total = p2.TryGetProperty("total", out var t) ? t.ToString() : "?";
                    return $"🗺️ Mapping: {total} gaps";
                }
                return null;
            });
        }
        catch (Exception ex)
        {
            sb.AppendLine($"⚠️ Agent API לא זמין: {ex.Message}");
        }

        await SendToGroup(sb.ToString(), targetChatId);
    }

    private async Task AppendAgentKeyMetric(StringBuilder sb, string agentName, Func<System.Text.Json.JsonElement, string?> extractor)
    {
        try
        {
            var encodedName = Uri.EscapeDataString(agentName);
            var json = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var metric = extractor(doc.RootElement);
            if (metric != null) sb.AppendLine($"  {metric}");
        }
        catch { /* Agent API might be down — skip silently */ }
    }

    // ── Dashboard 2: Rooms & Sales ──────────────────────────────────

    private async Task SendDashboardSales(string? targetChatId = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine("💰 *דשבורד חדרים ומכירות*");
        sb.AppendLine($"🕐 {DateTime.UtcNow:dd/MM HH:mm} UTC");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

        try
        {
            var status = await _data.GetFullStatus();

            sb.AppendLine();
            sb.AppendLine("*🏨 חדרים:*");
            sb.AppendLine($"  📦 פעילים: *{status.TotalActiveBookings}*");
            sb.AppendLine($"  📈 עתידיים: *{status.FutureBookings}*");
            if (status.WasteRoomsTotal > 0)
                sb.AppendLine($"  💸 לא נמכרו (waste): *{status.WasteRoomsTotal}* (${status.WasteTotalValue:N0})");

            sb.AppendLine();
            sb.AppendLine("*💵 כספים:*");
            sb.AppendLine($"  🛒 קנינו: *{status.TotalBought}* חדרים (${status.BoughtTodayValue:N0})");
            sb.AppendLine($"  🏷️ מכרנו: *{status.TotalSold}* חדרים (${status.SoldTodayValue:N0})");
            sb.AppendLine($"  📊 רווח/הפסד: *${status.ProfitLoss:N0}*");
            if (status.ConversionRate > 0)
                sb.AppendLine($"  🎯 המרה: *{status.ConversionRate:F1}%*");

            sb.AppendLine();
            sb.AppendLine("*📥 הזמנות:*");
            sb.AppendLine($"  📥 Reservations היום: *{status.ReservationsToday}*");
            sb.AppendLine($"  🔄 ביטולים תקועים: *{status.StuckCancellations}*");
            if (status.RoomsBoughtToday > 0)
                sb.AppendLine($"  🛍️ רכישות היום: *{status.RoomsBoughtToday}*");

            // Scans / mapping miss rate from Agent API
            try
            {
                var scansJson = await _http.GetStringAsync("http://127.0.0.1:5050/scans");
                using var scansDoc = System.Text.Json.JsonDocument.Parse(scansJson);
                var scansRoot = scansDoc.RootElement;
                var missRate = scansRoot.TryGetProperty("miss_rate", out var mr) ? mr.GetDouble() : 0;
                var details = scansRoot.TryGetProperty("details", out var dt) ? dt.GetInt32() : 0;
                var misses = scansRoot.TryGetProperty("misses", out var ms) ? ms.GetInt32() : 0;
                sb.AppendLine();
                var missIcon = missRate > 80 ? "🔴" : missRate > 50 ? "🟡" : "🟢";
                sb.AppendLine($"*🗺️ סריקות:*");
                sb.AppendLine($"  {missIcon} Miss rate: *{missRate:F1}%* ({misses} misses / {details} details)");
            }
            catch { }

            // Add rooms from Agent API
            try
            {
                var roomsJson = await _http.GetStringAsync("http://127.0.0.1:5050/rooms");
                using var doc = System.Text.Json.JsonDocument.Parse(roomsJson);
                if (doc.RootElement.TryGetProperty("hotels", out var hotels))
                {
                    sb.AppendLine();
                    sb.AppendLine("*🏨 חדרים לפי מלון:*");
                    int count = 0;
                    foreach (var h in hotels.EnumerateArray())
                    {
                        if (count++ >= 8) { sb.AppendLine("  _...ועוד_"); break; }
                        var name = h.GetProperty("hotel").GetString() ?? "?";
                        var rooms = h.GetProperty("rooms").GetInt32();
                        var unsold = h.GetProperty("unsold").GetInt32();
                        var sold = rooms - unsold;
                        var icon = sold > 0 ? "✅" : "⬜";
                        sb.AppendLine($"  {icon} {name}: {rooms} ({sold} נמכרו)");
                    }
                }
            }
            catch { }
        }
        catch (Exception ex)
        {
            sb.AppendLine($"⚠️ שגיאה: {ex.Message}");
        }

        await SendToGroup(sb.ToString(), targetChatId);
    }

    // ── Dashboard 3: Risks & WebJobs ────────────────────────────────

    private async Task SendDashboardRisks(string? targetChatId = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine("⚠️ *דשבורד כשלים וסיכונים*");
        sb.AppendLine($"🕐 {DateTime.UtcNow:dd/MM HH:mm} UTC");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

        // Safety report from Agent API
        try
        {
            var safetyJson = await _http.GetStringAsync("http://127.0.0.1:5050/safety");
            using var doc = System.Text.Json.JsonDocument.Parse(safetyJson);
            var root = doc.RootElement;

            var worstThreat = root.TryGetProperty("worst_threat", out var wt) ? wt.GetString() : "?";
            var threatIcon = worstThreat switch { "OK" => "🟢", "MEDIUM" => "🟡", "HIGH" => "🟠", "WARNING" => "🟡", "CRITICAL" => "🔴", _ => "❓" };

            sb.AppendLine();
            sb.AppendLine($"*🛡️ בטיחות (שמעון):* {threatIcon} {worstThreat}");

            if (root.TryGetProperty("checks", out var checks))
            {
                foreach (var check in checks.EnumerateArray())
                {
                    var name = check.GetProperty("check").GetString() ?? "?";
                    var passed = check.TryGetProperty("passed", out var p) && p.GetBoolean();
                    var msg = check.TryGetProperty("message", out var m) ? m.GetString() ?? "" : "";
                    var icon = passed ? "✅" : "❌";
                    sb.AppendLine($"  {icon} {name}: {(msg.Length > 60 ? msg[..60] + "..." : msg)}");
                }
            }
        }
        catch (Exception ex)
        {
            sb.AppendLine($"  ⚠️ Safety API: {ex.Message}");
        }

        // System Monitor alerts
        try
        {
            var monReport = _monitor.LastReport;
            if (monReport != null)
            {
                sb.AppendLine();
                sb.AppendLine("*🖥️ System Monitor:*");

                var criticals = monReport.Alerts.Where(a => a.Severity is "CRITICAL" or "EMERGENCY").ToList();
                var warnings = monReport.Alerts.Where(a => a.Severity == "WARNING").ToList();

                if (criticals.Any())
                {
                    foreach (var a in criticals.Take(5))
                        sb.AppendLine($"  🔴 {a.Component}: {a.Message}");
                }
                if (warnings.Any())
                {
                    foreach (var a in warnings.Take(5))
                        sb.AppendLine($"  🟡 {a.Component}: {a.Message}");
                }
                if (!criticals.Any() && !warnings.Any())
                    sb.AppendLine("  ✅ אין התראות");

                // Trend
                if (monReport.Trend != null)
                {
                    var ti = monReport.Trend.OverallTrend switch { "DEGRADING" => "📉", "IMPROVING" => "📈", _ => "➡️" };
                    sb.AppendLine($"  {ti} טרנד: {monReport.Trend.OverallTrend} | בריאות: {monReport.Trend.HealthPct}%");

                    // Escalations
                    var escalations = monReport.Trend.Components
                        .Where(c => c.Value.ConsecutiveCritical >= 3)
                        .Select(c => c.Key).ToList();
                    if (escalations.Any())
                        sb.AppendLine($"  🚨 ESCALATION: {string.Join(", ", escalations)}");
                }
            }
        }
        catch { }

        // Circuit Breakers
        try
        {
            var breakers = _failSafe.GetBreakers();
            var openBreakers = breakers.Where(b => b.IsOpen).ToList();
            sb.AppendLine();
            if (openBreakers.Any())
            {
                sb.AppendLine($"*🛑 Circuit Breakers:* {openBreakers.Count} פתוחים!");
                foreach (var b in openBreakers)
                    sb.AppendLine($"  🔴 {b.Name}: {b.Reason}");
            }
            else
            {
                sb.AppendLine("*🛑 Circuit Breakers:* ✅ הכל סגור");
            }
        }
        catch { }

        // FailSafe scan summary
        try
        {
            var lastScan = _failSafe.LastScanResult;
            if (lastScan != null)
            {
                var fsIcon = lastScan.Status switch { "CRITICAL" => "🔴", "WARNING" => "🟡", _ => "🟢" };
                sb.AppendLine();
                sb.AppendLine($"*🔒 FailSafe:* {fsIcon} {lastScan.Status}");
                if (lastScan.Violations?.Any() == true)
                {
                    foreach (var v in lastScan.Violations.Take(3))
                        sb.AppendLine($"  ⚠️ {v.RuleName}: {v.Description}");
                }
            }
        }
        catch { }

        // Data freshness from SystemMonitor
        try
        {
            var monReport = _monitor.LastReport;
            if (monReport?.Results.TryGetValue("data_freshness", out var dfObj) == true && dfObj is Dictionary<string, object?> df)
            {
                sb.AppendLine();
                sb.AppendLine("*📡 Data Freshness:*");
                if (df.TryGetValue("last_detail_minutes", out var ldm))
                    sb.AppendLine($"  Details: {ldm}min ago {((int?)ldm > 60 ? "🟡" : "🟢")}");
                if (df.TryGetValue("last_purchase_minutes", out var lpm))
                    sb.AppendLine($"  Purchase: {lpm}min ago {((int?)lpm > 360 ? "🔴" : "🟢")}");
                if (df.TryGetValue("last_reservation_minutes", out var lrm))
                    sb.AppendLine($"  Reservation: {lrm}min ago");
            }
        }
        catch { }

        // Recent errors
        try
        {
            var status = await _data.GetFullStatus();
            sb.AppendLine();
            sb.AppendLine("*❌ שגיאות 24 שעות:*");
            sb.AppendLine($"  הזמנות: *{status.BookingErrorsLast24h}*");
            sb.AppendLine($"  ביטולים: *{status.CancelErrorsLast24h}*");
            sb.AppendLine($"  BackOffice: *{status.BackOfficeErrorsLast24h}*");
            if (status.SalesOfficeFailed > 0)
                sb.AppendLine($"  SalesOffice failed: *{status.SalesOfficeFailed}*");
        }
        catch { }

        await SendToGroup(sb.ToString(), targetChatId);
    }

    // ── Command Handlers ─────────────────────────────────────────

    private async Task HandleStatus(string chatId)
    {
        try
        {
            var status = await _data.GetFullStatus();
            var msg = $"📋 *סטטוס מהיר*\n" +
                      $"🗄️ DB: {(status.DbConnected ? "✅" : "❌")}\n" +
                      $"📦 הזמנות: {status.TotalActiveBookings}\n" +
                      $"🔄 תקועים: {status.StuckCancellations}\n" +
                      $"💸 waste: {status.WasteRoomsTotal}\n" +
                      $"💰 קנינו: {status.TotalBought} | מכרנו: {status.TotalSold}";
            await SendToGroup(msg, chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleAlerts(string chatId)
    {
        try
        {
            var alerts = await _alerting.EvaluateAlerts();
            if (!alerts.Any()) { await SendToGroup("✅ אין התראות פעילות", chatId); return; }

            var sb = new StringBuilder("🚨 *התראות פעילות:*\n\n");
            foreach (var a in alerts.Take(10))
            {
                var icon = a.Severity == "Critical" ? "🔴" : "🟡";
                sb.AppendLine($"{icon} *{a.Title}*");
                sb.AppendLine($"   {a.Message.Take(100)}...");
                sb.AppendLine();
            }
            if (alerts.Count > 10) sb.AppendLine($"_...ועוד {alerts.Count - 10} נוספים_");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleReconcile(string chatId)
    {
        await SendToGroup("🔄 מריץ בדיקת התאמה... (עד דקה)", chatId);
        try
        {
            var report = await _reconciliation.RunReconciliation(24);
            var icon = report.CriticalMismatches > 0 ? "🔴" : report.TotalMismatches > 0 ? "🟡" : "🟢";
            var msg = $"{icon} *בדיקת התאמה הושלמה*\n\n" +
                      $"Medici: {report.MediciBookingsCount} | Zenith: {report.MediciReservationsCount}\n" +
                      $"אי-התאמות: *{report.TotalMismatches}* ({report.CriticalMismatches} קריטיות)\n" +
                      $"משך: {report.DurationMs}ms";
            if (report.Mismatches.Any())
            {
                msg += "\n\n*פירוט:*";
                foreach (var m in report.Mismatches.Take(5))
                    msg += $"\n{(m.Severity == "Critical" ? "🔴" : "🟡")} {m.Description}";
            }
            await SendToGroup(msg, chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleKillSwitch(string chatId, string text, string from)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            await SendToGroup(
                "🛑 *Kill Switch — פקודות:*\n\n" +
                "`/killswitch all <PIN>` — הפעל הכל\n" +
                "`/killswitch reset <PIN>` — אפס הכל\n" +
                "`/trip <BREAKER> <PIN>` — הפעל breaker ספציפי\n" +
                "`/reset <BREAKER> <PIN>` — אפס breaker ספציפי\n" +
                "`/breakers` — סטטוס כל ה-breakers\n\n" +
                "Breakers: BUYING, SELLING, QUEUE, PUSH, CANCELS", chatId);
            return;
        }

        var action = parts[1].ToLower();
        var pin = parts.Length > 2 ? parts[2] : "";

        if (!_failSafe.ValidatePin(pin))
        {
            await SendToGroup("❌ PIN שגוי!", chatId);
            return;
        }

        if (action == "all")
        {
            _failSafe.TripAll($"Kill Switch via Telegram by {from}", from);
            await SendToGroup($"🚨 *KILL SWITCH הופעל!*\nכל ה-circuit breakers נפתחו.\nהופעל ע\"י: {from}", chatId);
        }
        else if (action == "reset")
        {
            _failSafe.ResetAll(from);
            await SendToGroup($"✅ *כל ה-breakers אופסו*\nהופעל ע\"י: {from}", chatId);
        }
    }

    private async Task HandleTrip(string chatId, string text, string from)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) { await SendToGroup("שימוש: `/trip BUYING <PIN>`", chatId); return; }

        var name = parts[1].ToUpper();
        var pin = parts[2];

        if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }

        var result = _failSafe.TripBreaker(name, $"Tripped via Telegram by {from}", from);
        if (result != null)
            await SendToGroup($"🔴 *Breaker {name} הופעל!*\nע\"י: {from}", chatId);
        else
            await SendToGroup($"❌ Breaker '{name}' לא נמצא", chatId);
    }

    private async Task HandleReset(string chatId, string text, string from)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) { await SendToGroup("שימוש: `/reset BUYING <PIN>`", chatId); return; }

        var name = parts[1].ToUpper();
        var pin = parts[2];

        if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }

        var result = _failSafe.ResetBreaker(name, from);
        if (result != null)
            await SendToGroup($"✅ *Breaker {name} אופס*\nע\"י: {from}", chatId);
        else
            await SendToGroup($"❌ Breaker '{name}' לא נמצא", chatId);
    }

    private async Task HandleBreakers(string chatId)
    {
        var breakers = _failSafe.GetBreakers();
        var sb = new StringBuilder("🛑 *Circuit Breakers:*\n\n");
        foreach (var b in breakers)
        {
            var icon = b.IsOpen ? "🔴" : "🟢";
            sb.AppendLine($"{icon} *{b.Name}*: {(b.IsOpen ? "פתוח" : "סגור")}");
            if (b.IsOpen)
                sb.AppendLine($"   סיבה: {b.Reason}\n   ע\"י: {b.TriggeredBy}");
        }
        await SendToGroup(sb.ToString(), chatId);
    }

    private async Task HandleHelp(string chatId)
    {
        await SendToGroup(
            "📖 *MediciMonitor Bot — 30+ פקודות:*\n\n" +
            "*📋 דוחות:*\n" +
            "`/status` `/report` `/daily_summary` `/weekly_summary` `/alerts`\n\n" +
            "*💰 פיננסי:*\n" +
            "`/pnl [today|week|month]` `/waste` `/bookings` `/errors`\n\n" +
            "*🏨 מלונות:*\n" +
            "`/hotel <ID>` `/search <שם>`\n\n" +
            "*🔍 בדיקות:*\n" +
            "`/reconcile` `/scan` `/verify <BookingId>` `/trace <OrderId>`\n\n" +
            "*🌐 תשתית:*\n" +
            "`/health` `/sla` `/db` `/webjobs` `/test_notify`\n\n" +
            "*🛑 Kill Switch:*\n" +
            "`/killswitch [all|reset] <PIN>` `/trip` `/reset` `/breakers`\n" +
            "`/pause <min> [reason]` — הקפאה זמנית עם שחרור אוטומטי\n\n" +
            "*📝 אישורים:*\n" +
            "`/flagged` `/approve <ID> <PIN>` `/reject <ID> <PIN>`\n" +
            "`/cancel <PreBookId> <PIN>` — בקשת ביטול\n\n" +
            "*🖥️ System Monitor:*\n" +
            "`/monitor` — סריקת מערכת מלאה (8 בדיקות)\n" +
            "`/monitor_check <name>` — בדיקה ספציפית\n" +
            "`/tables` — בריאות טבלאות\n" +
            "`/mapping` — איכות Mapping\n" +
            "`/zenith` — בדיקת Zenith SOAP\n" +
            "`/cancel_errors` — ניתוח שגיאות ביטול\n" +
            "`/trend [hours]` — ניתוח טרנד\n\n" +
            "`/buyrooms` — בריאות רכישת חדרים\n" +
            "`/reservations_check` — Zenith callbacks\n" +
            "`/freshness` — טריות נתונים\n" +
            "`/sales_check` — מכירות ו-P&L\n" +
            "`/overrides` — Price Override pipeline\n\n" +
            "*🤖 AI + Monitor:*\n" +
            "`/ask <שאלה>` — שאל Claude (עם הקשר מוניטור מלא)\n" +
            "`/ask_monitor <שאלה>` — שאל על בעיה ספציפית במוניטור\n" +
            "`/analyse_monitor` — ניתוח AI מלא של כל 13 הבדיקות\n\n" +
            "*📡 מעקב:*\n" +
            "`/watch [on|off|500]` — התראה על הזמנות חדשות\n" +
            "`/mute <hours>` — השתקה זמנית\n\n" +
            "*👥 צוות:*\n" +
            "`/oncall <name>` `/log <הערה>` `/schedule HH:MM /cmd`\n\n" +
            "*🗣️ שפה טבעית:*\n" +
            "\"עצור הכל 7743\" | \"מה המצב\" | \"אשר 42 7743\"\n\n" +
            "_דוח: 3h | יומי: 07:00 | שבועי: ראשון 08:00_", chatId);
    }

    // ── Deep Verification & Anomalies ─────────────────────────────

    private async Task HandleDeepCheck(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var hours = parts.Length > 1 && int.TryParse(parts[1], out var h) ? h : 48;
        await SendToGroup($"🔍 מריץ אימות מעמיק ({hours}h)... זה יכול לקחת דקה", chatId);
        try
        {
            var report = await _deepVerify.RunDeepVerification(hours);
            var icon = report.CriticalAnomalies > 0 ? "🔴" : report.TotalAnomalies > 0 ? "🟡" : "🟢";
            var sb = new System.Text.StringBuilder($"{icon} *אימות מעמיק הושלם*\n\n");
            sb.AppendLine($"📦 הזמנות: {report.TotalBookings} | Reservations: {report.TotalReservations}");
            sb.AppendLine($"✅ Innstant אומתו: {report.InnstantVerifiedOk}/{report.InnstantBookingsToVerify}");
            sb.AppendLine($"⚠️ אנומליות: *{report.TotalAnomalies}* ({report.CriticalAnomalies} קריטיות)");
            sb.AppendLine($"⏱️ משך: {report.DurationMs}ms\n");

            if (report.Anomalies.Any())
            {
                sb.AppendLine("*פירוט אנומליות:*");
                foreach (var a in report.Anomalies.OrderByDescending(a => a.Severity == "Critical").Take(10))
                {
                    var aIcon = a.Severity == "Critical" ? "🔴" : "🟡";
                    sb.AppendLine($"{aIcon} [{a.System}] {a.Description}");
                }
                if (report.Anomalies.Count > 10)
                    sb.AppendLine($"_...ועוד {report.Anomalies.Count - 10}_");
            }
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleAnomalies(string chatId)
    {
        var report = _deepVerify.LastReport;
        if (report == null) { await SendToGroup("ℹ️ לא בוצע אימות עדיין. שלח `/deep_check`", chatId); return; }

        if (!report.Anomalies.Any()) { await SendToGroup("✅ אין אנומליות — הכל תקין!", chatId); return; }

        var grouped = report.Anomalies.GroupBy(a => a.Type);
        var sb = new System.Text.StringBuilder($"⚠️ *{report.TotalAnomalies} אנומליות* ({report.CriticalAnomalies} קריטיות)\n\n");
        foreach (var g in grouped)
        {
            var typeLabel = g.Key switch
            {
                AnomalyType.MissingInExternal => "חסר ב-Innstant",
                AnomalyType.PriceMismatch => "פער מחיר",
                AnomalyType.StatusConflict => "סטטוס סותר",
                AnomalyType.DateMismatch => "תאריך שונה",
                AnomalyType.MissingSale => "חסר Reservation",
                AnomalyType.OrphanedRecord => "Reservation יתומה",
                AnomalyType.GhostCancellation => "ביטול רפאי",
                AnomalyType.DuplicateBooking => "הזמנה כפולה",
                AnomalyType.SellingAtLoss => "מכירה בהפסד",
                AnomalyType.ExpiringUnsold => "פג ללא מכירה",
                _ => g.Key.ToString()
            };
            sb.AppendLine($"*{typeLabel}* ({g.Count()}):");
            foreach (var a in g.Take(3))
                sb.AppendLine($"  {(a.Severity == "Critical" ? "🔴" : "🟡")} {a.Description}");
            if (g.Count() > 3) sb.AppendLine($"  _...ועוד {g.Count() - 3}_");
            sb.AppendLine();
        }
        await SendToGroup(sb.ToString(), chatId);
    }

    private async Task HandleGhosts(string chatId)
    {
        try
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_config.GetConnectionString("SqlServer"));
            await conn.OpenAsync();
            using var cmd = new Microsoft.Data.SqlClient.SqlCommand(@"
                SELECT TOP 20 b.PreBookId, b.contentBookingID, b.HotelId, h.Name, b.Price,
                       b.StartDate, b.EndDate, b.CancellationTo, b.DateInsert, b.IsSold
                FROM MED_Book b
                LEFT JOIN Med_Hotels h ON h.HotelId = b.HotelId
                WHERE b.IsActive = 0 AND b.IsSold = 1
                  AND b.CancellationTo >= DATEADD(DAY, -90, GETDATE())
                  AND NOT EXISTS (SELECT 1 FROM MED_CancelBook c WHERE c.PreBookId = b.PreBookId)
                  AND NOT EXISTS (SELECT 1 FROM MED_CancelBookError e WHERE e.PreBookId = b.PreBookId)
                ORDER BY b.DateInsert DESC", conn) { CommandTimeout = 15 };
            using var rdr = await cmd.ExecuteReaderAsync();

            var sb = new System.Text.StringBuilder("👻 *ביטולים רפאיים (Ghost Cancellations):*\n\n");
            int count = 0;
            while (await rdr.ReadAsync())
            {
                count++;
                var preBookId = rdr.GetInt32(0);
                var contentId = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1);
                var hotelId = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2);
                var hotelName = rdr.IsDBNull(3) ? $"Hotel {hotelId}" : rdr.GetString(3);
                var price = rdr.IsDBNull(4) ? 0 : rdr.GetDouble(4);
                var startDate = rdr.IsDBNull(5) ? (DateTime?)null : rdr.GetDateTime(5);
                var cancelTo = rdr.IsDBNull(7) ? (DateTime?)null : rdr.GetDateTime(7);
                var dateInsert = rdr.IsDBNull(8) ? (DateTime?)null : rdr.GetDateTime(8);
                var isPast = startDate.HasValue && startDate.Value < DateTime.UtcNow;

                sb.AppendLine($"🔴 *#{preBookId}* (Innstant: {contentId})");
                sb.AppendLine($"  🏨 {hotelName}");
                sb.AppendLine($"  💰 ${price:N0}");
                sb.AppendLine($"  📅 {startDate:dd/MM/yy}–{rdr.GetDateTime(6):dd/MM/yy} {(isPast ? "_(עבר)_" : "⚠️ *עתידי!*")}");
                sb.AppendLine($"  ⏰ נוצר: {dateInsert:dd/MM/yy} | דדליין ביטול: {cancelTo:dd/MM/yy}");
                sb.AppendLine();
            }

            if (count == 0)
                sb.AppendLine("✅ אין ביטולים רפאיים — הכל תקין!");
            else
                sb.AppendLine($"_סה\"כ: {count} | כובו ב-DB ללא cancel API — ספק עלול לחייב!_");

            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Hotel Card & Search ────────────────────────────────────────

    private async Task HandleHotel(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !int.TryParse(parts[1], out var hotelId))
        { await SendToGroup("שימוש: `/hotel <HotelId>`\nלדוגמה: `/hotel 854881`", chatId); return; }
        try
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_config.GetConnectionString("SqlServer"));
            await conn.OpenAsync();
            using var cmd = new Microsoft.Data.SqlClient.SqlCommand($@"
                SELECT h.Name, h.Innstant_ZenithId, h.isActive,
                    (SELECT COUNT(*) FROM MED_Book b WHERE b.HotelId = @hid AND b.IsActive = 1) as ActiveBookings,
                    (SELECT COUNT(*) FROM MED_Book b WHERE b.HotelId = @hid AND b.IsActive = 1 AND b.IsSold = 1) as SoldBookings,
                    (SELECT ISNULL(SUM(b.Price), 0) FROM MED_Book b WHERE b.HotelId = @hid AND b.IsActive = 1) as TotalBuyCost,
                    (SELECT ISNULL(SUM(r.AmountAfterTax), 0) FROM Med_Reservation r WHERE r.HotelCode = CAST(@hid AS NVARCHAR(20)) AND r.ResStatus IN ('New','Committed')) as TotalSellRevenue,
                    (SELECT COUNT(*) FROM MED_Book b WHERE b.HotelId = @hid AND b.IsActive = 1 AND (b.IsSold = 0 OR b.IsSold IS NULL) AND b.CancellationTo >= GETDATE()) as WasteRooms
                FROM Med_Hotels h WHERE h.HotelId = @hid", conn) { CommandTimeout = 15 };
            cmd.Parameters.AddWithValue("@hid", hotelId);
            using var rdr = await cmd.ExecuteReaderAsync();
            if (!await rdr.ReadAsync()) { await SendToGroup($"❌ מלון {hotelId} לא נמצא", chatId); return; }
            var name = rdr.IsDBNull(0) ? "?" : rdr.GetString(0);
            var zenithId = rdr.IsDBNull(1) ? "N/A" : rdr.GetValue(1).ToString();
            var active = rdr.GetInt32(3); var sold = rdr.GetInt32(4);
            var buyCost = rdr.GetDouble(5); var sellRev = rdr.GetDouble(6); var waste = rdr.GetInt32(7);
            var profit = sellRev - buyCost;
            var margin = buyCost > 0 ? profit / buyCost * 100 : 0;
            var mc = margin > 10 ? "🟢" : margin > 0 ? "🟡" : "🔴";
            await SendToGroup($"🏨 *{name}* (#{hotelId})\nZenith: {zenithId}\n\n📦 הזמנות: *{active}* | נמכרו: *{sold}* | waste: *{waste}*\n💵 עלות: ${buyCost:N0} | הכנסה: ${sellRev:N0}\n{mc} רווח: ${profit:N0} ({margin:F1}%)", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleSearchHotel(string chatId, string text)
    {
        var query = text.Length > 8 ? text[8..].Trim() : "";
        if (string.IsNullOrEmpty(query)) { await SendToGroup("שימוש: `/search <שם מלון>`", chatId); return; }
        try
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_config.GetConnectionString("SqlServer"));
            await conn.OpenAsync();
            using var cmd = new Microsoft.Data.SqlClient.SqlCommand(
                "SELECT TOP 10 HotelId, Name FROM Med_Hotels WHERE Name LIKE @q ORDER BY Name", conn) { CommandTimeout = 10 };
            cmd.Parameters.AddWithValue("@q", $"%{query}%");
            using var rdr = await cmd.ExecuteReaderAsync();
            var sb = new System.Text.StringBuilder($"🔍 *חיפוש: \"{query}\"*\n\n");
            int count = 0;
            while (await rdr.ReadAsync())
            {
                sb.AppendLine($"  🏨 *{rdr.GetInt32(0)}* — {rdr.GetString(1)}");
                count++;
            }
            if (count == 0) sb.AppendLine("לא נמצאו תוצאות");
            else sb.AppendLine($"\nשלח `/hotel <ID>` לפרטים מלאים");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Claude AI Chat ──────────────────────────────────────────

    private async Task HandleAskClaude(string chatId, string text)
    {
        var question = text.Length > 5 ? text[5..].Trim() : "";
        if (string.IsNullOrEmpty(question)) { await SendToGroup("שימוש: `/ask <שאלה>`\nלדוגמה: `/ask למה יש ירידה בהזמנות?`", chatId); return; }
        if (!_claude.IsAvailable) { await SendToGroup("❌ Claude AI לא מוגדר (חסר API key)", chatId); return; }
        await SendToGroup("🤖 חושב... (עם הקשר מוניטור מלא)", chatId);
        try
        {
            var response = await _claude.ChatWithMonitor(question);
            var answer = response.Success ? response.Response : $"שגיאה: {response.Error}";
            if (answer.Length > 3800) answer = answer[..3800] + "\n\n_...תשובה קוצרה_";
            await SendToGroup($"🤖 *Claude AI:*\n\n{answer}", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleAskMonitor(string chatId, string text)
    {
        var question = text.Length > 13 ? text[13..].Trim() : "";
        if (string.IsNullOrEmpty(question)) { await SendToGroup("שימוש: `/ask_monitor <שאלה>`\nלדוגמה:\n`/ask_monitor למה Zenith איטי?`\n`/ask_monitor מה הטרנד של cancel errors?`\n`/ask_monitor האם BuyRooms עובד תקין?`", chatId); return; }
        if (!_claude.IsAvailable) { await SendToGroup("❌ Claude AI לא מוגדר (חסר API key)", chatId); return; }
        await SendToGroup("🤖🖥️ מנתח מוניטור + חושב...", chatId);
        try
        {
            var response = await _claude.ChatWithMonitor(question);
            var answer = response.Success ? response.Response : $"שגיאה: {response.Error}";
            if (answer.Length > 3800) answer = answer[..3800] + "\n\n_...תשובה קוצרה_";
            var meta = response.DurationMs > 0 ? $"\n\n_⏱️ {response.DurationMs}ms | {response.Mode}_" : "";
            await SendToGroup($"🤖🖥️ *Claude + Monitor:*\n\n{answer}{meta}", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleAgentQuery(string chatId, string text)
    {
        try
        {
            var endpoint = "http://127.0.0.1:5050";
            string path;

            if (text.StartsWith("/agent "))
            {
                var name = Uri.EscapeDataString(text[7..].Trim());
                path = $"/agent/{name}";
            }
            else if (text == "/agents") path = "/agents";
            else if (text == "/rooms") path = "/rooms";
            else if (text.Contains("safety")) path = "/safety";
            else if (text.Contains("scans")) path = "/scans";
            else path = "/health";

            await SendToGroup("🔍 מתחבר לסוכנים...", chatId);
            var resp = await _http.GetStringAsync($"{endpoint}{path}");

            if (resp.Length > 3800) resp = resp[..3800] + "\n...";
            await SendToGroup($"🤖 *Agent Response:*\n```\n{resp}\n```", chatId);
        }
        catch (Exception ex)
        {
            await SendToGroup($"❌ Agent API לא זמין: {ex.Message}", chatId);
        }
    }

    private async Task HandleAnalyseMonitor(string chatId)
    {
        if (!_claude.IsAvailable) { await SendToGroup("❌ Claude AI לא מוגדר (חסר API key)", chatId); return; }
        await SendToGroup("🤖🖥️ מריץ סריקה מלאה + ניתוח AI...", chatId);
        try
        {
            var response = await _claude.AnalyseMonitor();
            var answer = response.Success ? response.Response : $"שגיאה: {response.Error}";
            if (answer.Length > 3800) answer = answer[..3800] + "\n\n_...תשובה קוצרה_";
            var meta = response.DurationMs > 0 ? $"\n\n_⏱️ {response.DurationMs}ms | {response.InputTokens}→{response.OutputTokens} tokens | {response.Mode}_" : "";
            await SendToGroup($"🤖🖥️ *ניתוח System Monitor:*\n\n{answer}{meta}", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Pause (Timed Breaker Freeze) ────────────────────────────

    private async Task HandlePause(string chatId, string text, string from)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !int.TryParse(parts[1], out var minutes))
        { await SendToGroup("שימוש: `/pause <minutes> [reason]`\nלדוגמה: `/pause 30 maintenance`", chatId); return; }
        var reason = parts.Length > 2 ? string.Join(" ", parts.Skip(2)) : "Paused via Telegram";
        _failSafe.TripAll($"Pause {minutes}m: {reason} (by {from})", from);
        _pauseUntil = DateTime.UtcNow.AddMinutes(minutes);
        SaveBotState();
        await SendToGroup($"⏸️ *מערכת מוקפאת ל-{minutes} דקות*\nסיבה: {reason}\nע\"י: {from}\nשחרור אוטומטי: {_pauseUntil:HH:mm} UTC", chatId);
    }

    // ── Watch Mode (Real-time Reservation Alerts) ───────────────

    private async Task HandleWatch(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2) { await SendToGroup($"📡 Watch mode: {(_watchEnabled ? $"*פעיל* (min: ${_watchMinAmount:N0})" : "*כבוי*")}\n\nשימוש:\n`/watch on` — הפעל\n`/watch off` — כבה\n`/watch 500` — רק מעל $500", chatId); return; }
        var arg = parts[1].ToLower();
        if (arg == "off") { _watchEnabled = false; SaveBotState(); await SendToGroup("📡 Watch mode *כבוי*", chatId); }
        else if (arg == "on") { _watchEnabled = true; _watchMinAmount = 0; SaveBotState(); await SendToGroup("📡 Watch mode *פעיל* — כל הזמנה חדשה", chatId); }
        else if (double.TryParse(arg, out var min)) { _watchEnabled = true; _watchMinAmount = min; SaveBotState(); await SendToGroup($"📡 Watch mode *פעיל* — הזמנות מעל ${min:N0}", chatId); }
    }

    private async Task CheckNewReservations()
    {
        try
        {
            using var conn = new Microsoft.Data.SqlClient.SqlConnection(_config.GetConnectionString("SqlServer"));
            await conn.OpenAsync();
            using var cmd = new Microsoft.Data.SqlClient.SqlCommand(
                "SELECT COUNT(*) FROM Med_Reservation WHERE DateInsert >= DATEADD(MINUTE, -1, GETDATE())", conn) { CommandTimeout = 5 };
            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            if (count > 0 && count != _lastReservationCount)
            {
                // Fetch details of new reservations
                using var cmd2 = new Microsoft.Data.SqlClient.SqlCommand($@"
                    SELECT TOP 5 r.HotelCode, h.Name, r.AmountAfterTax, r.CurrencyCode, r.Datefrom, r.Dateto, r.ResStatus
                    FROM Med_Reservation r LEFT JOIN Med_Hotels h ON CAST(h.HotelId AS NVARCHAR(20)) = r.HotelCode
                    WHERE r.DateInsert >= DATEADD(MINUTE, -1, GETDATE())
                    {(_watchMinAmount > 0 ? $"AND r.AmountAfterTax >= {_watchMinAmount}" : "")}
                    ORDER BY r.DateInsert DESC", conn) { CommandTimeout = 10 };
                using var rdr = await cmd2.ExecuteReaderAsync();
                while (await rdr.ReadAsync())
                {
                    var hotel = rdr.IsDBNull(1) ? $"Hotel {rdr.GetString(0)}" : rdr.GetString(1);
                    var amount = rdr.IsDBNull(2) ? 0 : rdr.GetDouble(2);
                    var currency = rdr.IsDBNull(3) ? "USD" : rdr.GetString(3);
                    var dates = $"{(rdr.IsDBNull(4) ? "?" : rdr.GetDateTime(4).ToString("dd/MM"))}–{(rdr.IsDBNull(5) ? "?" : rdr.GetDateTime(5).ToString("dd/MM"))}";
                    var status = rdr.IsDBNull(6) ? "?" : rdr.GetString(6);
                    await SendToGroup($"🔔 *הזמנה חדשה!*\n🏨 {hotel}\n💰 {amount:N0} {currency}\n📅 {dates}\nStatus: {status}");
                }
            }
            _lastReservationCount = count;
        }
        catch { }
    }

    // ── Mute, Log, Oncall, Cancel, Schedule ─────────────────────

    private async Task HandleMute(string chatId, string text, string from)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !int.TryParse(parts[1], out var hours))
        { await SendToGroup($"שימוש: `/mute <hours>`\nמצב נוכחי: {(_muteUntil.HasValue && DateTime.UtcNow < _muteUntil ? $"מושתק עד {_muteUntil:HH:mm} UTC" : "פעיל")}", chatId); return; }
        _muteUntil = DateTime.UtcNow.AddHours(hours);
        SaveBotState();
        await SendToGroup($"🔕 *הושתק ל-{hours} שעות* ע\"י {from}\nרק Critical יעבור.\nשחרור: {_muteUntil:HH:mm} UTC", chatId);
    }

    private async Task HandleLog(string chatId, string text, string from)
    {
        var message = text.Length > 5 ? text[5..].Trim() : "";
        if (string.IsNullOrEmpty(message)) { await SendToGroup("שימוש: `/log <הערה>`", chatId); return; }
        _audit.Record($"Telegram:{from}", "ManualLog", message);
        await SendToGroup($"📝 *נרשם ב-Audit:*\nע\"י: {from}\n\"{message}\"", chatId);
    }

    private async Task HandleOncall(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2) { await SendToGroup($"👤 *תורנות:* {_oncallName ?? "לא הוגדר"}\n\nשימוש: `/oncall <שם>`", chatId); return; }
        _oncallName = string.Join(" ", parts.Skip(1));
        SaveBotState();
        await SendToGroup($"👤 *תורנות עודכנה:* {_oncallName}", chatId);
    }

    private async Task HandleCancelBooking(string chatId, string text, string from)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) { await SendToGroup("שימוש: `/cancel <PreBookId> <PIN>`\n⚠️ פעולה זו מבטלת הזמנה ב-Innstant!", chatId); return; }
        if (!int.TryParse(parts[1], out var preBookId)) { await SendToGroup("❌ PreBookId חייב להיות מספר", chatId); return; }
        var pin = parts[2];
        if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
        // Log the cancellation request — actual cancellation requires Innstant API integration
        _audit.Record($"Telegram:{from}", "CancelRequest", $"PreBookId={preBookId}");
        await SendToGroup($"⚠️ *בקשת ביטול נרשמה:*\nPreBookId: {preBookId}\nע\"י: {from}\n\n_ביטול בפועל דורש גישה ל-Innstant Cancel API — יש לבצע דרך medici-backend_", chatId);
    }

    private async Task HandleSchedule(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3) { await SendToGroup($"שימוש: `/schedule HH:MM /command`\nלדוגמה: `/schedule 07:00 /report`\n\n*מתוזמנים ({_scheduledCommands.Count}):*\n" + string.Join("\n", _scheduledCommands.Select(s => $"  {s.hour:D2}:{s.minute:D2} → {s.command}")), chatId); return; }
        var timeParts = parts[1].Split(':');
        if (timeParts.Length != 2 || !int.TryParse(timeParts[0], out var hour) || !int.TryParse(timeParts[1], out var minute))
        { await SendToGroup("❌ פורמט זמן: HH:MM", chatId); return; }
        var cmd = string.Join(" ", parts.Skip(2));
        _scheduledCommands.Add((hour, minute, cmd));
        await SendToGroup($"⏰ *תוזמן:* {hour:D2}:{minute:D2} UTC → `{cmd}`", chatId);
    }

    // ── Weekly Summary ──────────────────────────────────────────

    private async Task SendWeeklySummary(string? targetChatId = null)
    {
        try
        {
            var status = await _data.GetFullStatus();
            var alerts = await _alerting.EvaluateAlerts();
            var sb = new System.Text.StringBuilder();
            sb.AppendLine("📊 *דוח שבועי — MediciMonitor*");
            sb.AppendLine($"📅 שבוע שנסתיים {DateTime.UtcNow:dd/MM/yyyy}");
            sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");
            sb.AppendLine($"\n*📦 הזמנות:* {status.TotalActiveBookings} פעילות");
            sb.AppendLine($"*💰 קנינו:* {status.TotalBought} | *מכרנו:* {status.TotalSold}");
            sb.AppendLine($"*💵 רווח/הפסד:* ${status.ProfitLoss:N0}");
            sb.AppendLine($"*💸 Waste:* {status.WasteRoomsTotal} חדרים (${status.WasteTotalValue:N0})");
            sb.AppendLine($"\n*🚨 התראות:* {alerts.Count(a => a.Severity == "Critical")} קריטיות | {alerts.Count(a => a.Severity == "Warning")} אזהרות");
            var recon = _reconciliation.LastReport;
            if (recon != null)
                sb.AppendLine($"*🔍 התאמות:* {recon.TotalMismatches} אי-התאמות ({recon.CriticalMismatches} קריטיות)");
            sb.AppendLine($"\n*👤 תורנות:* {_oncallName ?? "לא הוגדר"}");
            sb.AppendLine("\n━━━━━━━━━━━━━━━━━━━━");
            sb.AppendLine("_דוח הבא: יום ראשון הבא 08:00 UTC_");
            await SendToGroup(sb.ToString(), targetChatId);
        }
        catch (Exception ex) { _logger.LogWarning("Weekly summary failed: {Err}", ex.Message); }
    }

    // ── Financial & Business Commands ─────────────────────────────

    private async Task HandlePnl(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var period = parts.Length > 1 ? parts[1] : "week";
        try
        {
            var status = await _data.GetFullStatus();
            var msg = $"💵 *P&L — {period}*\n\n" +
                      $"📦 קנינו: *{status.TotalBought}* חדרים\n" +
                      $"🏷️ מכרנו: *{status.TotalSold}* חדרים\n" +
                      $"💰 רווח/הפסד: *${status.ProfitLoss:N0}*\n" +
                      $"💸 waste: {status.WasteRoomsTotal} חדרים (${status.WasteTotalValue:N0})";
            await SendToGroup(msg, chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleWaste(string chatId)
    {
        try
        {
            var status = await _data.GetFullStatus();
            var sb = new StringBuilder("💸 *חדרים לא נמכרו (Room Waste):*\n\n");
            sb.AppendLine($"סה\"כ: *{status.WasteRoomsTotal}* חדרים");
            sb.AppendLine($"שווי: *${status.WasteTotalValue:N0}*");
            if (status.WasteRooms != null)
                foreach (var w in status.WasteRooms.Take(10))
                    sb.AppendLine($"  🏨 {w.HotelName ?? $"Hotel {w.HotelId}"}: ${w.Price:N0}, פג: {w.CancellationTo:dd/MM} ({w.HoursUntilExpiry}h)");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleRecentBookings(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var count = parts.Length > 1 && int.TryParse(parts[1], out var n) ? n : 10;
        try
        {
            var status = await _data.GetFullStatus();
            var sb = new StringBuilder($"📦 *{count} הזמנות אחרונות:*\n\n");
            sb.AppendLine($"פעילות: *{status.TotalActiveBookings}* | תקועות: *{status.StuckCancellations}*");
            sb.AppendLine($"Reservations היום: *{status.ReservationsToday}* | השבוע: *{status.ReservationsThisWeek}*");
            sb.AppendLine($"הכנסה שבועית: *${status.ReservationRevenueThisWeek:N0}*");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleErrors(string chatId)
    {
        try
        {
            var status = await _data.GetFullStatus();
            var sb = new StringBuilder("⚠️ *שגיאות:*\n\n");
            sb.AppendLine($"שגיאות הזמנה (24h): *{status.BookingErrorsLast24h}*");
            sb.AppendLine($"שגיאות ביטול (24h): *{status.CancelErrorsLast24h}*");
            sb.AppendLine($"ביטולים מוצלחים (24h): *{status.CancelSuccessLast24h}*");
            sb.AppendLine($"שגיאות Push: *{status.FailedPushItems?.Count ?? 0}*");
            sb.AppendLine($"שגיאות Queue: *{status.QueueErrors}*");
            if (status.RecentBookingErrors != null)
            {
                sb.AppendLine("\n*שגיאות אחרונות:*");
                foreach (var e in status.RecentBookingErrors.Take(5))
                    sb.AppendLine($"  ❌ #{e.PreBookId}: {(e.Error?.Length > 60 ? e.Error[..60] + "..." : e.Error)}");
            }
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Infrastructure Commands ──────────────────────────────────

    private async Task HandleApiHealth(string chatId)
    {
        try
        {
            var health = await _azure.ComprehensiveApiHealthCheck();
            var sb = new StringBuilder("🌐 *בריאות API:*\n\n");
            foreach (var ep in health)
            {
                var icon = ep.IsHealthy ? "✅" : "❌";
                var name = ep.Endpoint.Split('(')[0].Trim();
                sb.AppendLine($"{icon} *{name}* — {ep.ResponseTimeMs}ms (HTTP {ep.StatusCode})");
            }
            var healthy = health.Count(h => h.IsHealthy);
            sb.AppendLine($"\n📊 *{healthy}/{health.Count}* תקינים");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleSla(string chatId)
    {
        try
        {
            var report = _sla.GetReport();
            var sb = new StringBuilder("📊 *SLA Report:*\n\n");
            sb.AppendLine($"Overall Uptime: *{report.OverallUptime:F2}%*");
            sb.AppendLine($"MTTR: *{report.OverallMTTR:F1} min*");
            sb.AppendLine($"MTTD: *{report.OverallMTTD:F1} min*\n");
            foreach (var ep in report.Endpoints.Take(8))
            {
                var icon = ep.IsUp ? "🟢" : "🔴";
                sb.AppendLine($"{icon} *{ep.Endpoint}*: {ep.UptimePercent:F1}% | {ep.LastResponseTimeMs}ms");
            }
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleDbHealth(string chatId)
    {
        try
        {
            var report = await _dbHealth.GetHealthReport();
            var sb = new StringBuilder("🗄️ *DB Health:*\n\n");
            sb.AppendLine($"חיבור: {(report.IsConnected ? "✅ מחובר" : "❌ מנותק")}");
            if (report.TotalSizeMB > 0) sb.AppendLine($"גודל: *{report.TotalSizeMB:F0} MB*");
            if (report.ActiveConnections > 0) sb.AppendLine($"חיבורים פעילים: *{report.ActiveConnections}*");
            if (report.TotalDeadlocks > 0) sb.AppendLine($"🔴 Deadlocks: *{report.TotalDeadlocks}*");
            if (report.LongRunningQueries?.Any() == true)
            {
                sb.AppendLine("\n*שאילתות ארוכות:*");
                foreach (var q in report.LongRunningQueries.Take(3))
                    sb.AppendLine($"  ⏱️ {q.DurationSeconds}s — {(q.SqlText?.Length > 50 ? q.SqlText[..50] + "..." : q.SqlText)}");
            }
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleWebJobs(string chatId)
    {
        try
        {
            var dashboard = await _webJobs.GetDashboardAsync(false);
            var sb = new StringBuilder("⚙️ *WebJobs:*\n\n");
            sb.AppendLine($"Total: *{dashboard.Summary.TotalJobs}* | Running: *{dashboard.Summary.RunningJobs}* | Stopped: *{dashboard.Summary.StoppedJobs}* | Errors: *{dashboard.Summary.ErrorJobs}*");
            if (dashboard.Apps?.Any() == true)
                foreach (var app in dashboard.Apps.Take(5))
                    sb.AppendLine($"\n📱 *{app.AppName}*: {(app.HasError ? "❌ " + app.ErrorMessage : "✅")} ({app.Jobs.Count} jobs)");
            if (dashboard.Summary.TotalJobs == 0)
                sb.AppendLine("\nℹ️ אין נתוני WebJobs (דורש Azure Managed Identity)");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Scan & Verify Commands ───────────────────────────────────

    private async Task HandleFailSafeScan(string chatId)
    {
        await SendToGroup("🔍 מריץ סריקת FailSafe...", chatId);
        try
        {
            var result = await _failSafe.ScanAsync();
            var icon = result.Status == "CRITICAL" ? "🔴" : result.Status == "WARNING" ? "🟡" : "🟢";
            var sb = new StringBuilder($"{icon} *FailSafe Scan:* {result.Status}\n\n");
            sb.AppendLine(result.Message);
            if (result.Violations?.Any() == true)
            {
                sb.AppendLine("\n*הפרות:*");
                foreach (var v in result.Violations.Take(5))
                    sb.AppendLine($"  {(v.Severity == "Critical" ? "🔴" : "🟡")} {v.RuleName}: {v.Description}");
            }
            var openBreakers = result.Breakers?.Where(b => b.IsOpen).ToList();
            if (openBreakers?.Any() == true)
            {
                sb.AppendLine($"\n*🛑 Breakers פתוחים ({openBreakers.Count}):*");
                foreach (var b in openBreakers)
                    sb.AppendLine($"  🔴 {b.Name}");
            }
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleTrace(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !int.TryParse(parts[1], out var orderId))
        {
            await SendToGroup("שימוש: `/trace <OrderId>`\nלדוגמה: `/trace 12345`", chatId);
            return;
        }
        try
        {
            var trace = await _data.GetSalesOrderTrace(orderId);
            await SendToGroup($"🔍 *Trace Order #{orderId}:*\n```\n{System.Text.Json.JsonSerializer.Serialize(trace, new System.Text.Json.JsonSerializerOptions { WriteIndented = true, DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull }).Substring(0, Math.Min(3500, System.Text.Json.JsonSerializer.Serialize(trace).Length))}\n```", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleVerifyBooking(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2 || !int.TryParse(parts[1], out var bookingId))
        {
            await SendToGroup("שימוש: `/verify <ContentBookingId>`\nלדוגמה: `/verify 3652895`", chatId);
            return;
        }
        await SendToGroup($"🔍 מאמת הזמנה {bookingId} ב-Innstant...", chatId);
        try
        {
            var result = await _innstant.GetBookingDetails(bookingId);
            if (result == null || !result.Found)
                await SendToGroup($"❌ הזמנה {bookingId} *לא נמצאה* ב-Innstant!\n{result?.Error ?? ""}", chatId);
            else
                await SendToGroup($"✅ *הזמנה {bookingId} נמצאה ב-Innstant:*\n\nסטטוס: {result.Status}\nמלון: {result.HotelName}\nDates: {result.CheckIn} → {result.CheckOut}\nמחיר: {result.TotalPrice} {result.Currency}\nאורח: {result.GuestName}\nConfirmation: {result.ConfirmationNumber}", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleTestNotification(string chatId)
    {
        try
        {
            var result = await _notifications.SendTestAsync();
            var channels = string.Join("\n", result.Channels.Select(c => $"  {(c.Success ? "✅" : "❌")} {c.Channel}: {c.Detail}"));
            await SendToGroup($"🔔 *בדיקת התראות:*\n\n{channels}", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Daily Summary ─────────────────────────────────────────────

    private async Task SendDailySummary(string? targetChatId = null)
    {
        try
        {
            var sb = new StringBuilder();
            sb.AppendLine("📋 *דוח יומי — MediciMonitor*");
            sb.AppendLine($"📅 {DateTime.UtcNow.AddDays(-1):dd/MM/yyyy} (אתמול)");
            sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

            var status = await _data.GetFullStatus();
            sb.AppendLine();
            sb.AppendLine("*📦 הזמנות:*");
            sb.AppendLine($"  פעילות: *{status.TotalActiveBookings}*");
            sb.AppendLine($"  תקועות: *{status.StuckCancellations}*");
            sb.AppendLine($"  עתידיות: *{status.FutureBookings}*");
            sb.AppendLine($"  Reservations אתמול: *{status.ReservationsToday}*");

            sb.AppendLine();
            sb.AppendLine("*💰 פיננסי:*");
            sb.AppendLine($"  קנינו: *{status.TotalBought}* חדרים");
            sb.AppendLine($"  מכרנו: *{status.TotalSold}* חדרים");
            sb.AppendLine($"  רווח/הפסד: *${status.ProfitLoss:N0}*");
            sb.AppendLine($"  חדרים לא נמכרו: *{status.WasteRoomsTotal}* (${status.WasteTotalValue:N0})");

            // API Health
            try
            {
                var health = await _azure.ComprehensiveApiHealthCheck();
                var upPct = health.Count > 0 ? health.Count(h => h.IsHealthy) * 100.0 / health.Count : 100;
                sb.AppendLine();
                sb.AppendLine($"*🌐 API Uptime:* {upPct:F0}% ({health.Count(h => h.IsHealthy)}/{health.Count})");
            }
            catch { }

            // Alerts summary
            try
            {
                var alerts = await _alerting.EvaluateAlerts();
                var crit = alerts.Count(a => a.Severity == "Critical");
                var warn = alerts.Count(a => a.Severity == "Warning");
                sb.AppendLine();
                sb.AppendLine($"*🚨 התראות:* 🔴 {crit} קריטיות | 🟡 {warn} אזהרות");
                if (crit > 0)
                    foreach (var a in alerts.Where(a => a.Severity == "Critical").Take(3))
                        sb.AppendLine($"  🔴 {a.Title}");
            }
            catch { }

            // Reconciliation
            var recon = _reconciliation.LastReport;
            if (recon != null)
            {
                sb.AppendLine();
                sb.AppendLine($"*🔍 התאמת הזמנות:* {recon.TotalMismatches} אי-התאמות ({recon.CriticalMismatches} קריטיות)");
            }

            // Breakers
            var openBreakers = _failSafe.GetBreakers().Where(b => b.IsOpen).ToList();
            if (openBreakers.Any())
            {
                sb.AppendLine();
                sb.AppendLine($"*🛑 Breakers פתוחים:* {openBreakers.Count}");
                foreach (var b in openBreakers)
                    sb.AppendLine($"  🔴 {b.Name}: {b.Reason}");
            }

            // System Monitor — 24h trend
            try
            {
                var trend = _monitor.GetTrendAnalysis(24);
                sb.AppendLine();
                var trendIcon = trend.OverallTrend switch { "DEGRADING" => "📉", "IMPROVING" => "📈", _ => "➡️" };
                sb.AppendLine($"*🖥️ System Monitor (24h):*");
                sb.AppendLine($"  {trendIcon} טרנד: *{trend.OverallTrend}*");
                sb.AppendLine($"  סריקות: {trend.TotalRuns} | בריאות: {trend.HealthPct}%");
                sb.AppendLine($"  התראות: מחצית ראשונה={trend.FirstHalfAlerts} → שנייה={trend.SecondHalfAlerts}");
                if (trend.Components.Any())
                {
                    foreach (var (comp, stats) in trend.Components.OrderByDescending(c => c.Value.Total).Take(3))
                    {
                        var esc = stats.ConsecutiveCritical >= 3 ? " ⚠️ *ESCALATION*" : "";
                        sb.AppendLine($"  • {comp}: {stats.Total} התראות{esc}");
                    }
                }

                // Last scan results
                var monReport = _monitor.LastReport;
                if (monReport != null)
                {
                    var monCrit = monReport.Alerts.Count(a => a.Severity is "CRITICAL" or "EMERGENCY");
                    var monWarn = monReport.Alerts.Count(a => a.Severity == "WARNING");
                    sb.AppendLine($"  סריקה אחרונה: {monCrit} קריטיות | {monWarn} אזהרות");
                    if (monReport.Results.TryGetValue("zenith", out var zObj) && zObj is Dictionary<string, object?> z)
                        sb.AppendLine($"  🌐 Zenith: {z.GetValueOrDefault("status")} ({z.GetValueOrDefault("latency_ms")}ms)");
                    if (monReport.Results.TryGetValue("cancel_errors", out var ceObj) && ceObj is Dictionary<string, object?> ce)
                        sb.AppendLine($"  📊 Cancel errors trend: {ce.GetValueOrDefault("trend")}");
                }
            }
            catch { }

            sb.AppendLine();
            sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");
            sb.AppendLine("_/report לדוח מלא | /help לפקודות_");

            await SendToGroup(sb.ToString(), targetChatId);

            // Also send via email
            if (_config["Notifications:EmailEnabled"] == "true" || _config["Notifications:EmailEnabled"] == "True")
            {
                // Email is sent through the notification service which handles SMTP
                _logger.LogInformation("Daily summary sent to Telegram");
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Daily summary failed: {Err}", ex.Message);
        }
    }

    // ── Flagged Items & Approve/Reject ──────────────────────────

    private async Task HandleFlagged(string chatId)
    {
        var items = _failSafe.GetFlaggedItems();
        var pending = items?.Where(f => f.Status == "Pending").ToList() ?? new();

        if (!pending.Any())
        {
            await SendToGroup("✅ אין פריטים ממתינים לאישור", chatId);
            return;
        }

        var sb = new StringBuilder($"📋 *{pending.Count} פריטים ממתינים לאישור:*\n\n");
        foreach (var f in pending.Take(10))
        {
            sb.AppendLine($"*ID {f.Id}* — {f.RuleId}");
            sb.AppendLine($"  {f.Reason}");
            sb.AppendLine($"  סכום: ${f.Amount:N0} | {f.HotelName}");
            sb.AppendLine($"  `/approve {f.Id} <PIN>` | `/reject {f.Id} <PIN>`");
            sb.AppendLine();
        }
        if (pending.Count > 10)
            sb.AppendLine($"_...ועוד {pending.Count - 10}_");

        await SendToGroup(sb.ToString(), chatId);
    }

    private async Task HandleApproveReject(string chatId, string text, string from, bool approve)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 3)
        {
            await SendToGroup($"שימוש: `/{(approve ? "approve" : "reject")} <ID> <PIN>`", chatId);
            return;
        }

        if (!int.TryParse(parts[1], out var id))
        {
            await SendToGroup("❌ ID חייב להיות מספר", chatId);
            return;
        }

        var pin = parts[2];
        if (!_failSafe.ValidatePin(pin))
        {
            await SendToGroup("❌ PIN שגוי!", chatId);
            return;
        }

        if (approve)
        {
            var result = _failSafe.ApproveFlag(id, from);
            if (result != null)
                await SendToGroup($"✅ *פריט {id} אושר* ע\"י {from}", chatId);
            else
                await SendToGroup($"❌ פריט {id} לא נמצא", chatId);
        }
        else
        {
            var note = parts.Length > 3 ? string.Join(" ", parts.Skip(3)) : null;
            var result = _failSafe.RejectFlag(id, from, note);
            if (result != null)
                await SendToGroup($"❌ *פריט {id} נדחה* ע\"י {from}", chatId);
            else
                await SendToGroup($"❌ פריט {id} לא נמצא", chatId);
        }
    }

    // ── Natural Language Kill Switch ─────────────────────────────

    // ── Agent Chat (natural conversation with agents) ──────────────

    private static readonly Dictionary<string, string[]> AgentTriggers = new()
    {
        ["אריה"] = new[] { "אריה", "חדר בקרה", "control room" },
        ["אמיר"] = new[] { "אמיר", "מנכ\"ל", "som" },
        ["שמעון"] = new[] { "שמעון", "בטיחות", "safety" },
        ["דני"] = new[] { "דני", "שגריר", "coordinator" },
        ["יוסי"] = new[] { "יוסי", "מוכר", "seller" },
        ["רוני"] = new[] { "רוני", "השלמות", "completion" },
        ["מיכאל"] = new[] { "מיכאל", "מיפויים", "fixer" },
        ["גבי"] = new[] { "גבי", "autofix" },
        ["יעל"] = new[] { "יעל", "מפקחת", "monitor agent" },
        ["משה"] = new[] { "משה", "kill switch" },
    };

    private string? DetectAgentInMessage(string text)
    {
        var lower = text.ToLower().Trim();
        foreach (var (agent, triggers) in AgentTriggers)
        {
            foreach (var trigger in triggers)
            {
                if (lower.StartsWith(trigger) || lower.StartsWith($"@{trigger}") ||
                    lower.Contains($" {trigger},") || lower.Contains($" {trigger} "))
                    return agent;
            }
        }
        return null;
    }

    /// <summary>
    /// Smart routing — detect the topic and route to the right agent automatically.
    /// Returns null if no topic match (not an agent question).
    /// </summary>
    private static string? RouteToAgent(string text)
    {
        var lower = text.ToLower().Trim();

        // Topic → Agent mapping (checked in order — first match wins)
        // שמעון: safety, risk, exposure, spending, kill switch, refundable
        if (ContainsAny(lower, "בטיחות", "סיכון", "חשיפה", "הוצאות", "spending", "kill switch",
            "refundable", "exposure", "reconcil", "התאמה", "איום", "threat"))
            return "שמעון";

        // אמיר: orders, sales orders, SOM, bookings, miss rate, scans
        if (ContainsAny(lower, "sales order", "הזמנות", "orders", "miss rate", "חסרים",
            "סריקות", "scans", "booking", "הזמנה נכשלה", "failed order", "som"))
            return "אמיר";

        // יוסי: pricing, rooms, selling, margins, competitors
        if (ContainsAny(lower, "מחיר", "תמחור", "pricing", "margin", "חדרים", "rooms",
            "מכירות", "selling", "מתחרה", "competitor", "רווח"))
            return "יוסי";

        // מיכאל: mapping, gaps, venues, ratebycat
        if (ContainsAny(lower, "מיפוי", "mapping", "gaps", "venue", "ratebycat",
            "מיפויים", "gap", "type a", "type b"))
            return "מיכאל";

        // רוני: completion, B2B, visibility, availability, safety wall
        if (ContainsAny(lower, "השלמות", "completion", "b2b", "visibility", "availability",
            "safety wall", "נראות", "זמינות"))
            return "רוני";

        // יעל: monitoring, system, webjobs, zenith, health checks
        if (ContainsAny(lower, "מוניטור", "monitor", "webjob", "zenith", "בדיקות",
            "health check", "system", "בריאות", "data freshness"))
            return "יעל";

        // דני: coordination, prediction, cross-system, integration
        if (ContainsAny(lower, "תיאום", "prediction", "coordinator", "אינטגרציה",
            "integration", "cross", "מערכות"))
            return "דני";

        // גבי: autofix, quick fix, type A
        if (ContainsAny(lower, "autofix", "תיקון מהיר", "type a fix"))
            return "גבי";

        // אריה: general questions about agents, status, who does what
        if (ContainsAny(lower, "סוכן", "סוכנים", "agents", "מי מטפל", "מי אחראי",
            "מי עובד", "מי תקוע", "מה המצב", "סטטוס", "status", "דווח",
            "מה קורה", "עדכון", "תן סטטוס"))
            return "אריה";

        return null;
    }

    private static bool ContainsAny(string text, params string[] patterns)
        => patterns.Any(p => text.Contains(p, StringComparison.OrdinalIgnoreCase));

    private string StripAgentName(string text, string agent)
    {
        var lower = text.Trim();
        // Remove the agent name/trigger from the beginning
        foreach (var trigger in AgentTriggers[agent])
        {
            if (lower.StartsWith(trigger, StringComparison.OrdinalIgnoreCase))
            {
                lower = lower[trigger.Length..].TrimStart(',', ' ', ':', '-', '—');
                break;
            }
            if (lower.StartsWith($"@{trigger}", StringComparison.OrdinalIgnoreCase))
            {
                lower = lower[(trigger.Length + 1)..].TrimStart(',', ' ', ':', '-', '—');
                break;
            }
        }
        return string.IsNullOrWhiteSpace(lower) ? "מה המצב?" : lower;
    }

    private async Task HandleTalkToAgent(string chatId, string text)
    {
        var name = text.Length > 6 ? text[6..].Trim() : "";
        if (string.IsNullOrEmpty(name))
        {
            await SendToGroup("שימוש: `/talk <שם סוכן>`\nלדוגמה: `/talk שמעון`\n\nסוכנים: אריה, אמיר, שמעון, דני, יוסי, רוני, מיכאל, גבי, יעל, משה", chatId);
            return;
        }

        // Resolve agent name
        var agent = DetectAgentInMessage(name) ?? name;
        var quickStats = await FormatAgentQuickStats(agent);
        if (quickStats == null)
        {
            await SendToGroup($"❌ סוכן '{name}' לא נמצא.", chatId);
            return;
        }

        // Show quick stats (no Claude API call)
        await SendToGroup($"📋 *{agent}* — סטטוס מהיר:\n{quickStats}\n\n_כתוב שאלה להמשך שיחה עם {agent}_", chatId);

        // Set active conversation for follow-ups
        if (_claude.IsAvailable)
            await _claude.ChatWithAgent(chatId, agent, "__init__"); // Silent init — sets conversation state
    }

    private async Task<string?> FormatAgentQuickStats(string agentName)
    {
        try
        {
            var encodedName = Uri.EscapeDataString(agentName);
            var json = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("error", out _)) return null;

            var sb = new StringBuilder();
            var skill = root.TryGetProperty("skill", out var s) ? s.GetString() : "?";
            var age = root.TryGetProperty("age_minutes", out var a) ? a.GetDouble() : -1;
            var reportFile = root.TryGetProperty("report_file", out var rf) ? rf.GetString() : "?";

            sb.AppendLine($"  🔧 Skill: {skill}");
            sb.AppendLine($"  ⏱️ דוח אחרון: {(age >= 0 ? $"{(int)age} דקות" : "לא ידוע")}");
            sb.AppendLine($"  📄 קובץ: {reportFile}");

            // Extract key data based on agent
            if (root.TryGetProperty("data", out var data))
            {
                if (data.TryGetProperty("worst_threat", out var wt))
                    sb.AppendLine($"  🛡️ רמת איום: *{wt.GetString()}*");
                if (data.TryGetProperty("checks", out var checks))
                {
                    int passed = 0, total = checks.GetArrayLength();
                    foreach (var c in checks.EnumerateArray())
                        if (c.TryGetProperty("passed", out var p) && p.GetBoolean()) passed++;
                    sb.AppendLine($"  ✅ בדיקות: {passed}/{total} עברו");
                }
                if (data.TryGetProperty("phases", out var phases))
                {
                    if (phases.TryGetProperty("scans", out var scans) && scans.TryGetProperty("miss_rate_pct", out var mr))
                        sb.AppendLine($"  🗺️ Miss rate: *{mr}%*");
                    if (phases.TryGetProperty("orders", out var orders) && orders.TryGetProperty("signals_received", out var sig))
                        sb.AppendLine($"  📡 Signals: {sig}");
                }
                if (data.TryGetProperty("kpis", out var kpis))
                {
                    if (kpis.TryGetProperty("active_rooms", out var ar))
                        sb.AppendLine($"  🏨 חדרים פעילים: *{ar}*");
                    if (kpis.TryGetProperty("sold_rooms", out var sr))
                        sb.AppendLine($"  🏷️ נמכרו: *{sr}*");
                    if (kpis.TryGetProperty("safety_status", out var ss))
                        sb.AppendLine($"  🛡️ בטיחות: {ss}");
                }
                if (data.TryGetProperty("stats", out var stats))
                {
                    if (stats.TryGetProperty("total", out var tot))
                        sb.AppendLine($"  📊 סה\"כ חדרים: {tot}");
                    if (stats.TryGetProperty("updated", out var upd))
                        sb.AppendLine($"  ✏️ עודכנו: {upd}");
                }
            }

            return sb.ToString();
        }
        catch { return null; }
    }

    private async Task HandleAgentChat(string chatId, string agentName, string userMessage)
    {
        // Simple status questions — answer from data directly without Claude
        var simplePatterns = new[] { "מה המצב", "סטטוס", "status", "מה קורה", "עדכון", "דווח" };
        if (simplePatterns.Any(p => userMessage.Trim().Contains(p, StringComparison.OrdinalIgnoreCase)))
        {
            var quickStats = await FormatAgentQuickStats(agentName);
            if (quickStats != null)
            {
                await SendToGroup($"*{agentName}:*\n{quickStats}\n_שאל שאלה ספציפית לתשובה מפורטת (Claude AI)_", chatId);
                return;
            }
        }

        if (!_claude.IsAvailable)
        {
            // Fallback: show data without Claude
            var fallbackStats = await FormatAgentQuickStats(agentName);
            if (fallbackStats != null)
            {
                await SendToGroup($"*{agentName}:*\n{fallbackStats}\n\n_Claude AI לא זמין — מציג נתונים בלבד_", chatId);
                return;
            }
            await SendToGroup("❌ Claude AI לא מוגדר ו-Agent API לא זמין", chatId);
            return;
        }

        // Check if starting new conversation or continuing
        var existing = _claude.GetActiveConversation(chatId);
        var isNew = existing == null || existing.Agent != agentName;
        if (isNew)
            await SendToGroup($"💬 פותח שיחה עם *{agentName}*...\n_כתוב כל הודעה ו{agentName} ימשיך לענות. כתוב \"ביי\" או שם סוכן אחר כדי לעבור._", chatId);

        try
        {
            var response = await _claude.ChatWithAgent(chatId, agentName, userMessage);
            var answer = response.Success ? response.Response : $"שגיאה: {response.Error}";
            if (answer.Length > 3800) answer = answer[..3800] + "\n\n_...תשובה קוצרה_";
            var meta = response.DurationMs > 0 ? $"\n_⏱️ {response.DurationMs}ms_" : "";

            // Detect if the agent mentions another agent → offer handoff buttons
            var mentioned = DetectMentionedAgents(answer, agentName);
            if (mentioned.Any())
            {
                var agentIcons = new Dictionary<string, string>
                {
                    ["שמעון"] = "🛡️", ["אמיר"] = "📋", ["יוסי"] = "💰", ["מיכאל"] = "🗺️",
                    ["יעל"] = "🖥️", ["רוני"] = "🏨", ["דני"] = "🔀", ["גבי"] = "⚡", ["אריה"] = "🏢", ["משה"] = "🔒"
                };
                var buttons = new List<object[]>();
                var row = mentioned.Take(3).Select(m =>
                    Btn($"{agentIcons.GetValueOrDefault(m, "📋")} עבור ל{m}", $"handoff:{agentName}:{m}")).ToArray();
                buttons.Add(row);
                buttons.Add(new[] { Btn($"🗺️ המשך עם {agentName}", $"stay:{agentName}") });

                await SendInlineKeyboard(chatId, $"*{agentName}:*\n\n{answer}{meta}", buttons.ToArray());
            }
            else
            {
                await SendToGroup($"*{agentName}:*\n\n{answer}{meta}", chatId);
            }
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    /// <summary>
    /// Detect if an agent's response mentions other agents by name.
    /// </summary>
    private static List<string> DetectMentionedAgents(string text, string currentAgent)
    {
        var allAgents = new[] { "שמעון", "אמיר", "יוסי", "מיכאל", "יעל", "רוני", "דני", "גבי", "אריה", "משה" };
        return allAgents
            .Where(a => a != currentAgent && text.Contains(a))
            .ToList();
    }

    // ── Natural Language Processing ─────────────────────────────────

    private async Task HandleNaturalLanguage(string chatId, string text, string from)
    {
        // End conversation commands
        var endPatterns = new[] { "ביי", "bye", "סגור", "תודה", "יאללה", "סיום" };
        if (endPatterns.Any(p => text.Trim().Equals(p, StringComparison.OrdinalIgnoreCase) ||
                                  text.Trim().StartsWith(p + " ", StringComparison.OrdinalIgnoreCase)))
        {
            var activeConv = _claude.GetActiveConversation(chatId);
            if (activeConv != null)
            {
                var agentName = activeConv.Agent;
                _claude.EndAllConversations(chatId);
                await SendToGroup($"👋 שיחה עם *{agentName}* נסגרה.", chatId);
                return;
            }
        }

        // Check if message is addressed to a specific agent
        var agent = DetectAgentInMessage(text);
        if (agent != null)
        {
            var message = StripAgentName(text, agent);
            await HandleAgentChat(chatId, agent, message);
            return;
        }

        // Continue active conversation (no agent name mentioned)
        var existingConv = _claude.GetActiveConversation(chatId);
        if (existingConv != null)
        {
            await HandleAgentChat(chatId, existingConv.Agent, text);
            return;
        }

        // Smart suggest — detect topic and offer agent buttons (don't auto-route)
        var routedAgent = RouteToAgent(text);
        if (routedAgent != null)
        {
            await HandleSuggestAgent(chatId, text);
            return;
        }

        var lower = text.ToLower().Trim();

        // Kill switch patterns (Hebrew + English)
        var killPatterns = new[] { "עצור הכל", "תעצור הכל", "עצור את הכל", "kill switch", "killswitch", "עצירת חירום", "חירום", "emergency stop", "stop all", "תפסיק הכל", "הפסק הכל", "freeze", "הקפא" };
        var buyStopPatterns = new[] { "עצור קניות", "תעצור קניות", "עצור רכישות", "stop buying", "הפסק לקנות", "אל תקנה", "תפסיק לקנות" };
        var sellStopPatterns = new[] { "עצור מכירות", "תעצור מכירות", "stop selling", "הפסק למכור", "תפסיק למכור", "אל תמכור" };
        var cancelStopPatterns = new[] { "עצור ביטולים", "תעצור ביטולים", "stop cancels", "הפסק לבטל" };
        var resetPatterns = new[] { "שחרר הכל", "תשחרר הכל", "אפס הכל", "reset all", "חזור לפעילות", "תפעיל הכל", "הפעל הכל" };
        var statusPatterns = new[] { "מה המצב", "מה הסטטוס", "סטטוס", "status", "how are things", "מה קורה", "עדכון" };

        // Status (no PIN needed)
        if (statusPatterns.Any(p => lower.Contains(p)))
        {
            await HandleStatus(chatId);
            return;
        }

        // Extract PIN from message (4 digits)
        var pinMatch = System.Text.RegularExpressions.Regex.Match(text, @"\b(\d{4})\b");
        var pin = pinMatch.Success ? pinMatch.Groups[1].Value : "";

        // Kill all
        if (killPatterns.Any(p => lower.Contains(p)))
        {
            if (string.IsNullOrEmpty(pin))
            {
                await SendToGroup($"⚠️ כדי להפעיל Kill Switch שלח גם את ה-PIN (4 ספרות).\nלדוגמה: \"עצור הכל 7743\"", chatId);
                return;
            }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
            _failSafe.TripAll($"Kill Switch via natural language by {from}: \"{text}\"", from);
            await SendToGroup($"🚨 *KILL SWITCH הופעל!*\nכל ה-circuit breakers נפתחו.\nהופעל ע\"י: {from}\nהודעה: \"{text}\"", chatId);
            return;
        }

        // Stop buying
        if (buyStopPatterns.Any(p => lower.Contains(p)))
        {
            if (string.IsNullOrEmpty(pin)) { await SendToGroup("⚠️ שלח גם PIN. לדוגמה: \"עצור קניות 7743\"", chatId); return; }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
            _failSafe.TripBreaker("BUYING", $"Stopped via Telegram by {from}: \"{text}\"", from);
            await SendToGroup($"🔴 *רכישות נעצרו!*\nBreaker BUYING הופעל ע\"י: {from}", chatId);
            return;
        }

        // Stop selling
        if (sellStopPatterns.Any(p => lower.Contains(p)))
        {
            if (string.IsNullOrEmpty(pin)) { await SendToGroup("⚠️ שלח גם PIN. לדוגמה: \"עצור מכירות 7743\"", chatId); return; }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
            _failSafe.TripBreaker("SELLING", $"Stopped via Telegram by {from}: \"{text}\"", from);
            await SendToGroup($"🔴 *מכירות נעצרו!*\nBreaker SELLING הופעל ע\"י: {from}", chatId);
            return;
        }

        // Stop cancels
        if (cancelStopPatterns.Any(p => lower.Contains(p)))
        {
            if (string.IsNullOrEmpty(pin)) { await SendToGroup("⚠️ שלח גם PIN. לדוגמה: \"עצור ביטולים 7743\"", chatId); return; }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
            _failSafe.TripBreaker("CANCELS", $"Stopped via Telegram by {from}: \"{text}\"", from);
            await SendToGroup($"🔴 *ביטולים נעצרו!*\nBreaker CANCELS הופעל ע\"י: {from}", chatId);
            return;
        }

        // Reset all
        if (resetPatterns.Any(p => lower.Contains(p)))
        {
            if (string.IsNullOrEmpty(pin)) { await SendToGroup("⚠️ שלח גם PIN. לדוגמה: \"שחרר הכל 7743\"", chatId); return; }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
            _failSafe.ResetAll(from);
            await SendToGroup($"✅ *כל ה-breakers שוחררו!*\nהופעל ע\"י: {from}", chatId);
            return;
        }

        // Natural language approve: "אשר 42 7743", "תאשר הזמנה 42"
        var approvePatterns = new[] { "אשר", "תאשר", "approve", "אישור" };
        var rejectPatterns2 = new[] { "דחה", "תדחה", "reject", "דחייה" };
        if (approvePatterns.Any(p => lower.Contains(p)) || rejectPatterns2.Any(p => lower.Contains(p)))
        {
            var isApprove = approvePatterns.Any(p => lower.Contains(p));
            var idMatch = System.Text.RegularExpressions.Regex.Match(text, @"\b(\d{1,6})\b");
            if (!idMatch.Success || !int.TryParse(idMatch.Groups[1].Value, out var flagId))
            {
                await SendToGroup("⚠️ ציין מספר ID. לדוגמה: \"אשר 42 7743\"", chatId);
                return;
            }
            // PIN might be the 4-digit number
            if (string.IsNullOrEmpty(pin)) { await SendToGroup("⚠️ שלח גם PIN. לדוגמה: \"אשר 42 7743\"", chatId); return; }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }

            if (isApprove)
            {
                var result = _failSafe.ApproveFlag(flagId, from);
                await SendToGroup(result != null ? $"✅ *פריט {flagId} אושר* ע\"י {from}" : $"❌ פריט {flagId} לא נמצא", chatId);
            }
            else
            {
                var result = _failSafe.RejectFlag(flagId, from, null);
                await SendToGroup(result != null ? $"❌ *פריט {flagId} נדחה* ע\"י {from}" : $"❌ פריט {flagId} לא נמצא", chatId);
            }
            return;
        }

        // Fallback — nothing matched
        await SendInlineKeyboard(chatId,
            "🤔 לא הבנתי. מה תרצה לעשות?",
            new object[][]
            {
                new[] { Btn("🏢 צוות סוכנים", "team"), Btn("📊 דשבורד", "cmd:dashboard") },
                new[] { Btn("📋 סטטוס", "cmd:status"), Btn("❓ עזרה", "cmd:help") },
            });
    }

    // ── System Monitor Commands ───────────────────────────────────

    private async Task HandleMonitorFull(string chatId)
    {
        await SendToGroup("🔍 מריץ סריקת מערכת מלאה...", chatId);
        try
        {
            var report = await _monitor.RunFullScan();
            var sb = new StringBuilder("🖥️ *סריקת מערכת מלאה*\n");
            sb.AppendLine($"🕐 {report.Timestamp:dd/MM/yyyy HH:mm} UTC");
            sb.AppendLine("━━━━━━━━━━━━━━━━━━━━");

            // Summary per check
            foreach (var (key, val) in report.Results)
            {
                var icon = key switch { "webjob" => "⚙️", "tables" => "🗄️", "mapping" => "🗺️", "skills" => "🛠️", "orders" => "📦", "zenith" => "🌐", "cancellation" => "❌", "cancel_errors" => "📊", _ => "📋" };
                sb.AppendLine($"\n{icon} *{key}:* ✅");
            }

            // Alerts
            if (report.Alerts.Any())
            {
                sb.AppendLine($"\n🚨 *{report.Alerts.Count} התראות:*");
                foreach (var a in report.Alerts.Take(10))
                {
                    var icon = a.Severity switch { "EMERGENCY" => "🔴🔴", "CRITICAL" => "🔴", "WARNING" => "🟡", "ERROR" => "🟠", _ => "ℹ️" };
                    sb.AppendLine($"{icon} [{a.Component}] {a.Message}");
                }
                if (report.Alerts.Count > 10) sb.AppendLine($"_...ועוד {report.Alerts.Count - 10}_");
            }
            else sb.AppendLine("\n✅ *אין התראות — הכל תקין!*");

            // Trend
            if (report.Trend != null)
            {
                var trendIcon = report.Trend.OverallTrend switch { "DEGRADING" => "📉", "IMPROVING" => "📈", _ => "➡️" };
                sb.AppendLine($"\n{trendIcon} *טרנד:* {report.Trend.OverallTrend} | בריאות: {report.Trend.HealthPct}%");
            }

            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleMonitorCheck(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            await SendToGroup("שימוש: `/monitor_check <check>`\nבדיקות: webjob, tables, mapping, skills, orders, zenith, cancellation, cancel\\_errors", chatId);
            return;
        }
        try
        {
            var result = await _monitor.RunSingleCheck(parts[1]);
            var json = System.Text.Json.JsonSerializer.Serialize(result, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            if (json.Length > 3800) json = json[..3800] + "\n...";
            await SendToGroup($"🔍 *בדיקת {parts[1]}:*\n```\n{json}\n```", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleMonitorTables(string chatId)
    {
        try
        {
            var result = await _monitor.RunSingleCheck("tables");
            var tables = result["result"] as Dictionary<string, object?>;
            var sb = new StringBuilder("🗄️ *Table Health:*\n\n");
            if (tables != null)
            {
                var json = System.Text.Json.JsonSerializer.Serialize(tables, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
                if (json.Length > 3800) json = json[..3800] + "\n...";
                sb.Append($"```\n{json}\n```");
            }
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleMonitorMapping(string chatId)
    {
        try
        {
            var result = await _monitor.RunSingleCheck("mapping");
            var sb = new StringBuilder("🗺️ *Mapping Quality:*\n\n");
            var json = System.Text.Json.JsonSerializer.Serialize(result["result"], new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            if (json.Length > 3800) json = json[..3800] + "\n...";
            sb.Append($"```\n{json}\n```");
            await SendToGroup(sb.ToString(), chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleMonitorTrend(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var hours = parts.Length > 1 && int.TryParse(parts[1], out var h) ? h : 24;
        var trend = _monitor.GetTrendAnalysis(hours);
        var trendIcon = trend.OverallTrend switch { "DEGRADING" => "📉", "IMPROVING" => "📈", _ => "➡️" };
        var sb = new StringBuilder($"{trendIcon} *ניתוח טרנד — {hours} שעות:*\n\n");
        sb.AppendLine($"סריקות: {trend.TotalRuns} | בריאות: {trend.HealthPct}%");
        sb.AppendLine($"טרנד: *{trend.OverallTrend}*");
        sb.AppendLine($"התראות: מחצית ראשונה={trend.FirstHalfAlerts} | שנייה={trend.SecondHalfAlerts}");
        if (trend.Components.Any())
        {
            sb.AppendLine("\n*רכיבים:*");
            foreach (var (comp, stats) in trend.Components)
            {
                var esc = stats.ConsecutiveCritical >= 3 ? " ⚠️ ESCALATION" : "";
                sb.AppendLine($"  {comp}: {stats.Total} התראות (CRITICAL רצופים: {stats.ConsecutiveCritical}){esc}");
            }
        }
        await SendToGroup(sb.ToString(), chatId);
    }

    private async Task HandleCancelErrorAnalysis(string chatId)
    {
        await SendToGroup("📊 מנתח שגיאות ביטול...", chatId);
        try
        {
            var result = await _monitor.RunSingleCheck("cancel_errors");
            var json = System.Text.Json.JsonSerializer.Serialize(result["result"], new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            if (json.Length > 3800) json = json[..3800] + "\n...";
            await SendToGroup($"📊 *ניתוח שגיאות ביטול:*\n```\n{json}\n```", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    private async Task HandleZenithProbe(string chatId)
    {
        await SendToGroup("🌐 בודק חיבור Zenith SOAP...", chatId);
        try
        {
            var result = await _monitor.RunSingleCheck("zenith");
            var json = System.Text.Json.JsonSerializer.Serialize(result["result"], new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            await SendToGroup($"🌐 *Zenith SOAP Probe:*\n```\n{json}\n```", chatId);
        }
        catch (Exception ex) { await SendToGroup($"❌ שגיאה: {ex.Message}", chatId); }
    }

    // ── Send Message ─────────────────────────────────────────────

    private async Task SendToGroup(string text, string? targetChatId = null)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/sendMessage";

            // Telegram 4096 char limit
            if (text.Length > 4000) text = text[..4000] + "\n\n_...הודעה קוצרה_";

            var payload = JsonSerializer.Serialize(new
            {
                chat_id = targetChatId ?? _chatId,
                text,
                parse_mode = "Markdown",
                disable_web_page_preview = true
            });
            var content = new StringContent(payload, Encoding.UTF8, "application/json");
            await _http.PostAsync(url, content);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Telegram send error: {Err}", ex.Message);
        }
    }

    // ── Inline Keyboard (buttons) ────────────────────────────────

    private async Task SendInlineKeyboard(string chatId, string text, object[][] buttons)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/sendMessage";
            if (text.Length > 4000) text = text[..4000];

            var keyboard = buttons.Select(row =>
                row.Select(btn => (object)btn).ToArray()
            ).ToArray();

            var payload = JsonSerializer.Serialize(new
            {
                chat_id = chatId,
                text,
                parse_mode = "Markdown",
                disable_web_page_preview = true,
                reply_markup = new { inline_keyboard = keyboard }
            });
            await _http.PostAsync(url, new StringContent(payload, Encoding.UTF8, "application/json"));
        }
        catch (Exception ex) { _logger.LogDebug("Telegram keyboard send error: {Err}", ex.Message); }
    }

    private async Task EditMessageWithKeyboard(string chatId, string messageId, string text, object[][]? buttons = null)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/editMessageText";
            if (text.Length > 4000) text = text[..4000];

            var payload = new Dictionary<string, object?>
            {
                ["chat_id"] = chatId,
                ["message_id"] = messageId,
                ["text"] = text,
                ["parse_mode"] = "Markdown",
                ["disable_web_page_preview"] = true
            };
            if (buttons != null)
                payload["reply_markup"] = new { inline_keyboard = buttons.Select(row => row.Select(btn => (object)btn).ToArray()).ToArray() };

            await _http.PostAsync(url, new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json"));
        }
        catch (Exception ex) { _logger.LogDebug("Telegram edit error: {Err}", ex.Message); }
    }

    private async Task AnswerCallbackQuery(string callbackQueryId, string? text = null)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/answerCallbackQuery";
            var payload = new Dictionary<string, object?> { ["callback_query_id"] = callbackQueryId };
            if (text != null) payload["text"] = text;
            await _http.PostAsync(url, new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json"));
        }
        catch { }
    }

    private static object Btn(string text, string callbackData) =>
        new { text, callback_data = callbackData };

    // ── /team — Agent Team Menu ──────────────────────────────────

    private async Task HandleTeamMenu(string chatId)
    {
        var buttons = new object[][]
        {
            new[] { Btn("🛡️ שמעון - בטיחות", "agent:שמעון"), Btn("📋 אמיר - הזמנות", "agent:אמיר") },
            new[] { Btn("💰 יוסי - מכירות", "agent:יוסי"), Btn("🗺️ מיכאל - מיפויים", "agent:מיכאל") },
            new[] { Btn("🖥️ יעל - מוניטור", "agent:יעל"), Btn("🏨 רוני - השלמות", "agent:רוני") },
            new[] { Btn("🔀 דני - תיאום", "agent:דני"), Btn("⚡ גבי - תיקונים", "agent:גבי") },
            new[] { Btn("🏢 אריה - סה\"כ", "agent:אריה") },
        };

        await SendInlineKeyboard(chatId,
            "🏢 *צוות הסוכנים* — לחץ לדוח מיידי:",
            buttons);
    }

    // ── Callback Query Handler (button clicks) ──────────────────

    private async Task HandleCallbackQuery(JsonElement callbackQuery)
    {
        var callbackId = callbackQuery.GetProperty("id").GetString() ?? "";
        var data = callbackQuery.TryGetProperty("data", out var d) ? d.GetString() ?? "" : "";
        var msg = callbackQuery.TryGetProperty("message", out var m) ? m : default;
        var chatId = msg.ValueKind != JsonValueKind.Undefined
            ? msg.GetProperty("chat").GetProperty("id").GetInt64().ToString()
            : _chatId;
        var messageId = msg.ValueKind != JsonValueKind.Undefined
            ? msg.GetProperty("message_id").GetInt32().ToString()
            : "";

        await AnswerCallbackQuery(callbackId);

        if (data.StartsWith("agent:"))
        {
            var agentName = data[6..];
            await HandleAgentReportView(chatId, agentName, messageId);
        }
        else if (data.StartsWith("ask:"))
        {
            var agentName = data[4..];
            // Set active conversation for Claude chat
            await SendToGroup($"💬 אתה בשיחה עם *{agentName}*.\nכתוב שאלה ו-{agentName} יענה:", chatId);
            if (_claude.IsAvailable)
                await _claude.ChatWithAgent(chatId, agentName, "__init__");
        }
        else if (data.StartsWith("refresh:"))
        {
            var agentName = data[8..];
            await HandleAgentReportView(chatId, agentName, messageId);
        }
        else if (data == "team")
        {
            await HandleTeamMenu(chatId);
        }
        else if (data.StartsWith("handoff:"))
        {
            // Handoff: agent A mentions agent B → switch + inject B's data
            var parts = data[8..].Split(':');
            if (parts.Length == 2)
            {
                var fromAgent = parts[0];
                var toAgent = parts[1];
                // Fetch target agent's report
                var targetData = await FormatAgentQuickStats(toAgent);
                var context = targetData != null
                    ? $"הועברת מ-{fromAgent}. הנה הנתונים של {toAgent}:\n{targetData}"
                    : $"הועברת מ-{fromAgent}";

                await SendToGroup($"🔀 עוברים ל-*{toAgent}*...", chatId);
                await HandleAgentChat(chatId, toAgent, context);
            }
        }
        else if (data.StartsWith("stay:"))
        {
            var agentName = data[5..];
            await SendToGroup($"👍 ממשיכים עם *{agentName}*. כתוב שאלה:", chatId);
        }
        else if (data.StartsWith("suggest:"))
        {
            var agentName = data[8..];
            await HandleAgentReportView(chatId, agentName, "");
        }
        else if (data.StartsWith("cmd:"))
        {
            var cmd = data[4..];
            if (cmd == "dashboard") { await SendDashboardAgents(chatId); await SendDashboardSales(chatId); await SendDashboardRisks(chatId); }
            else if (cmd == "status") await HandleStatus(chatId);
            else if (cmd == "help") await HandleHelp(chatId);
        }
        else if (data.StartsWith("ack:"))
        {
            await SendToGroup($"✅ התראה אושרה.", chatId);
        }
        else if (data.StartsWith("snooze:"))
        {
            await SendToGroup($"😴 התראה מושתקת לשעה.", chatId);
        }
    }

    // ── Agent Report View (instant, no Claude) ──────────────────

    private async Task HandleAgentReportView(string chatId, string agentName, string messageId)
    {
        var report = await FormatAgentQuickStats(agentName);
        if (report == null)
        {
            await SendToGroup($"❌ סוכן '{agentName}' לא זמין", chatId);
            return;
        }

        var buttons = new object[][]
        {
            new[] { Btn("💬 שאל שאלה", $"ask:{agentName}"), Btn("🔄 רענן", $"refresh:{agentName}") },
            new[] { Btn("⬅️ חזרה לצוות", "team") },
        };

        var text = $"*{agentName}*\n━━━━━━━━━━━━━━━━━━\n{report}";

        if (!string.IsNullOrEmpty(messageId))
            await EditMessageWithKeyboard(chatId, messageId, text, buttons);
        else
            await SendInlineKeyboard(chatId, text, buttons);
    }

    // ── Suggest Agent (for free text questions) ──────────────────

    private async Task HandleSuggestAgent(string chatId, string text)
    {
        // Find the best matching agent(s)
        var primary = RouteToAgent(text);
        if (primary == null) primary = "אריה"; // default

        // Build suggestion buttons — primary + אריה (always available)
        var suggestions = new List<(string name, string icon)>();
        var agentIcons = new Dictionary<string, string>
        {
            ["שמעון"] = "🛡️", ["אמיר"] = "📋", ["יוסי"] = "💰", ["מיכאל"] = "🗺️",
            ["יעל"] = "🖥️", ["רוני"] = "🏨", ["דני"] = "🔀", ["גבי"] = "⚡", ["אריה"] = "🏢"
        };

        suggestions.Add((primary, agentIcons.GetValueOrDefault(primary, "📋")));
        if (primary != "אריה")
            suggestions.Add(("אריה", "🏢"));

        var buttons = new List<object[]>();
        var row = suggestions.Select(s => Btn($"{s.icon} {s.name}", $"suggest:{s.name}")).ToArray();
        buttons.Add(row);

        await SendInlineKeyboard(chatId,
            $"🔀 למי להעביר?\n\n_\"{(text.Length > 50 ? text[..50] + "..." : text)}\"_",
            buttons.ToArray());
    }
}
