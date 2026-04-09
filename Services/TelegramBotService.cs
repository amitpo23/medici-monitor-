using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Telegram bot service — sends hourly reports and handles commands (kill switch, status).
/// Polls for commands every 30 seconds and sends comprehensive reports every hour.
/// </summary>
public partial class TelegramBotService : BackgroundService
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

    // ── Dashboards: See TelegramBotService.Dashboards.cs ──
    // ── Commands: See TelegramBotService.Commands.cs ──
    // ── Agent Chat & NLP: See TelegramBotService.AgentChat.cs ──
}
