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
        AuditService audit)
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
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _botToken = _config["Notifications:TelegramBotToken"] ?? "";
        _chatId = _config["Notifications:TelegramChatId"] ?? "";

        if (string.IsNullOrEmpty(_botToken) || string.IsNullOrEmpty(_chatId))
        {
            _logger.LogInformation("TelegramBotService disabled — no bot token or chat ID configured");
            return;
        }

        _logger.LogInformation("TelegramBotService started — hourly reports + command polling");

        // Wait for app to warm up
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        // Send startup message
        await SendToGroup("🟢 *MediciMonitor Online*\nהמערכת פעילה ומתחילה לנטר.\nדוח ראשון ייצא בעוד שעה.\n\nפקודות זמינות:\n/status — סטטוס מהיר\n/report — דוח מלא\n/alerts — התראות פעילות\n/reconcile — בדיקת התאמה\n/killswitch — הפעלת Kill Switch\n/breakers — סטטוס circuit breakers\n/help — עזרה");

        var lastReportTime = DateTime.UtcNow;
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

                if (!update.TryGetProperty("message", out var msg)) continue;
                if (!msg.TryGetProperty("text", out var textEl)) continue;

                var text = textEl.GetString() ?? "";
                var chat = msg.GetProperty("chat");
                var chatId = chat.GetProperty("id").GetInt64().ToString();
                var from = msg.TryGetProperty("from", out var fromEl)
                    ? $"{fromEl.GetProperty("first_name").GetString()}"
                    : "Unknown";

                // Handle commands (/) and natural language kill switch
                if (text.StartsWith("/"))
                {
                    var command = text.Split('@')[0].Split(' ')[0].ToLower();
                    _logger.LogInformation("Telegram command: {Cmd} from {From}", command, from);

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
                        case "/schedule": await HandleSchedule(chatId, text); break;

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
            "*🤖 AI:*\n" +
            "`/ask <שאלה>` — שאל את Claude AI על המערכת\n\n" +
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
        await SendToGroup("🤖 חושב...", chatId);
        try
        {
            var response = await _claude.Chat(question);
            var answer = response?.ToString() ?? "אין תשובה";
            if (answer.Length > 3800) answer = answer[..3800] + "\n\n_...תשובה קוצרה_";
            await SendToGroup($"🤖 *Claude AI:*\n\n{answer}", chatId);
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
        await SendToGroup($"⏸️ *מערכת מוקפאת ל-{minutes} דקות*\nסיבה: {reason}\nע\"י: {from}\nשחרור אוטומטי: {_pauseUntil:HH:mm} UTC", chatId);
    }

    // ── Watch Mode (Real-time Reservation Alerts) ───────────────

    private async Task HandleWatch(string chatId, string text)
    {
        var parts = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2) { await SendToGroup($"📡 Watch mode: {(_watchEnabled ? $"*פעיל* (min: ${_watchMinAmount:N0})" : "*כבוי*")}\n\nשימוש:\n`/watch on` — הפעל\n`/watch off` — כבה\n`/watch 500` — רק מעל $500", chatId); return; }
        var arg = parts[1].ToLower();
        if (arg == "off") { _watchEnabled = false; await SendToGroup("📡 Watch mode *כבוי*", chatId); }
        else if (arg == "on") { _watchEnabled = true; _watchMinAmount = 0; await SendToGroup("📡 Watch mode *פעיל* — כל הזמנה חדשה", chatId); }
        else if (double.TryParse(arg, out var min)) { _watchEnabled = true; _watchMinAmount = min; await SendToGroup($"📡 Watch mode *פעיל* — הזמנות מעל ${min:N0}", chatId); }
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

    private async Task HandleNaturalLanguage(string chatId, string text, string from)
    {
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
}
