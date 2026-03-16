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
    private readonly AzureMonitoringService _azure;
    private readonly DatabaseHealthService _dbHealth;
    private readonly SlaTrackingService _sla;
    private readonly WebJobsMonitoringService _webJobs;
    private readonly NotificationService _notifications;
    private readonly InnstantApiClient _innstant;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };

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
        AzureMonitoringService azure,
        DatabaseHealthService dbHealth,
        SlaTrackingService sla,
        WebJobsMonitoringService webJobs,
        NotificationService notifications,
        InnstantApiClient innstant)
    {
        _config = config;
        _logger = logger;
        _data = data;
        _alerting = alerting;
        _failSafe = failSafe;
        _reconciliation = reconciliation;
        _azure = azure;
        _dbHealth = dbHealth;
        _sla = sla;
        _webJobs = webJobs;
        _notifications = notifications;
        _innstant = innstant;
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
            "📖 *MediciMonitor Bot — כל הפקודות:*\n\n" +
            "*📋 דוחות:*\n" +
            "  `/status` — סטטוס מהיר\n" +
            "  `/report` — דוח מלא\n" +
            "  `/daily_summary` — דוח יומי\n" +
            "  `/alerts` — התראות פעילות\n\n" +
            "*💰 פיננסי:*\n" +
            "  `/pnl [today|week|month]` — רווח והפסד\n" +
            "  `/waste` — חדרים לא נמכרו\n" +
            "  `/bookings [N]` — הזמנות אחרונות\n" +
            "  `/errors` — שגיאות\n\n" +
            "*🔍 בדיקות:*\n" +
            "  `/reconcile` — בדיקת התאמה\n" +
            "  `/scan` — סריקת FailSafe\n" +
            "  `/verify <BookingId>` — אימות ב-Innstant\n" +
            "  `/trace <OrderId>` — מעקב הזמנה\n\n" +
            "*🌐 תשתית:*\n" +
            "  `/health` — בריאות API endpoints\n" +
            "  `/sla` — דוח SLA / uptime\n" +
            "  `/db` — בריאות DB\n" +
            "  `/webjobs` — סטטוס WebJobs\n" +
            "  `/test_notify` — בדיקת ערוצי התראות\n\n" +
            "*🛑 Kill Switch:*\n" +
            "  `/killswitch [all|reset] <PIN>`\n" +
            "  `/trip BUYING <PIN>` | `/reset BUYING <PIN>`\n" +
            "  `/breakers` — סטטוס\n\n" +
            "*📝 אישורים:*\n" +
            "  `/flagged` — פריטים ממתינים\n" +
            "  `/approve <ID> <PIN>` | `/reject <ID> <PIN>`\n\n" +
            "*🗣️ שפה טבעית:*\n" +
            "  \"עצור הכל 7743\" | \"מה המצב\" | \"אשר 42 7743\"\n\n" +
            "_דוח כל 3 שעות | דוח יומי 07:00 UTC_", chatId);
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
