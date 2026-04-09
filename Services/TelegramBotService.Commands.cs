using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

public partial class TelegramBotService
{
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
}
