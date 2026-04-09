using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

public partial class TelegramBotService
{
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
}
