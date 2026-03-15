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
        AzureMonitoringService azure)
    {
        _config = config;
        _logger = logger;
        _data = data;
        _alerting = alerting;
        _failSafe = failSafe;
        _reconciliation = reconciliation;
        _azure = azure;
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

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Poll for commands every 30 seconds
                await PollCommands();

                // Send hourly report
                if (DateTime.UtcNow - lastReportTime >= TimeSpan.FromHours(1))
                {
                    await SendHourlyReport();
                    lastReportTime = DateTime.UtcNow;
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

                // Only respond to commands
                if (!text.StartsWith("/")) continue;

                var command = text.Split('@')[0].Split(' ')[0].ToLower();
                _logger.LogInformation("Telegram command: {Cmd} from {From}", command, from);

                switch (command)
                {
                    case "/status":
                        await HandleStatus(chatId);
                        break;
                    case "/report":
                        await SendHourlyReport(chatId);
                        break;
                    case "/alerts":
                        await HandleAlerts(chatId);
                        break;
                    case "/reconcile":
                        await HandleReconcile(chatId);
                        break;
                    case "/killswitch":
                        await HandleKillSwitch(chatId, text, from);
                        break;
                    case "/breakers":
                        await HandleBreakers(chatId);
                        break;
                    case "/trip":
                        await HandleTrip(chatId, text, from);
                        break;
                    case "/reset":
                        await HandleReset(chatId, text, from);
                        break;
                    case "/help":
                        await HandleHelp(chatId);
                        break;
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
            "📖 *MediciMonitor Bot — פקודות:*\n\n" +
            "📋 `/status` — סטטוס מהיר\n" +
            "📊 `/report` — דוח מלא\n" +
            "🚨 `/alerts` — התראות פעילות\n" +
            "🔍 `/reconcile` — בדיקת התאמת הזמנות\n" +
            "🛑 `/killswitch` — Kill Switch\n" +
            "🔴 `/trip BUYING <PIN>` — הפעל breaker\n" +
            "✅ `/reset BUYING <PIN>` — אפס breaker\n" +
            "📡 `/breakers` — סטטוס breakers\n\n" +
            "_דוח אוטומטי כל שעה_", chatId);
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
