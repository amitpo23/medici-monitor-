using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

public partial class TelegramBotService
{
    // ── Agent Config ─────────────────────────────────────────────

    private static readonly Dictionary<string, string> AgentIcons = new()
    {
        ["שמעון"] = "🛡️", ["אמיר"] = "📋", ["יוסי"] = "💰", ["מיכאל"] = "🗺️",
        ["יעל"] = "🖥️", ["רוני"] = "🏨", ["דני"] = "🔀", ["גבי"] = "⚡",
        ["אריה"] = "🏢", ["משה"] = "🔒"
    };

    private static readonly Dictionary<string, string[]> AgentTriggers = new()
    {
        ["אריה"] = new[] { "אריה", "חדר בקרה", "control room" },
        ["אמיר"] = new[] { "אמיר", "som" },
        ["שמעון"] = new[] { "שמעון", "safety" },
        ["דני"] = new[] { "דני", "coordinator" },
        ["יוסי"] = new[] { "יוסי", "seller" },
        ["רוני"] = new[] { "רוני", "completion" },
        ["מיכאל"] = new[] { "מיכאל", "fixer" },
        ["גבי"] = new[] { "גבי", "autofix" },
        ["יעל"] = new[] { "יעל", "monitor agent" },
        ["משה"] = new[] { "משה", "kill switch" },
    };

    // Auto-mute during conversation — suppress push notifications
    private DateTime? _conversationMuteUntil;

    private bool IsConversationMuted =>
        _conversationMuteUntil.HasValue && DateTime.UtcNow < _conversationMuteUntil.Value;

    // ── /team — Agent Menu (instant, no Claude) ──────────────────

    private async Task HandleTeamMenu(string chatId)
    {
        var buttons = new object[][]
        {
            new[] { Btn("🛡️ שמעון", "agent:שמעון"), Btn("📋 אמיר", "agent:אמיר"), Btn("💰 יוסי", "agent:יוסי") },
            new[] { Btn("🗺️ מיכאל", "agent:מיכאל"), Btn("🖥️ יעל", "agent:יעל"), Btn("🏨 רוני", "agent:רוני") },
            new[] { Btn("🔀 דני", "agent:דני"), Btn("⚡ גבי", "agent:גבי"), Btn("🏢 אריה", "agent:אריה") },
        };

        await SendInlineKeyboard(chatId, "🏢 *צוות סוכנים* — לחץ לדוח מיידי:", buttons);
    }

    // ── Agent Report View (instant, ~100ms, no Claude) ───────────

    private async Task HandleAgentReportView(string chatId, string agentName, string messageId)
    {
        var report = await FormatAgentQuickStats(agentName);
        if (report == null)
        {
            await SendToGroup($"❌ סוכן '{agentName}' לא זמין", chatId);
            return;
        }

        var icon = AgentIcons.GetValueOrDefault(agentName, "📋");
        var buttons = new object[][]
        {
            new[] { Btn("💬 שאל שאלה", $"ask:{agentName}"), Btn("🔄 רענן", $"refresh:{agentName}") },
            new[] { Btn("⬅️ חזרה", "team") },
        };

        var text = $"{icon} *{agentName}*\n━━━━━━━━━━━━━━━━━━\n{report}";

        if (!string.IsNullOrEmpty(messageId))
            await EditMessageWithKeyboard(chatId, messageId, text, buttons);
        else
            await SendInlineKeyboard(chatId, text, buttons);
    }

    // ── Format Agent Quick Stats (data only, no Claude) ──────────

    private async Task<string?> FormatAgentQuickStats(string agentName)
    {
        try
        {
            var encodedName = Uri.EscapeDataString(agentName);
            var json = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            if (root.TryGetProperty("error", out _)) return null;

            var sb = new StringBuilder();
            var age = root.TryGetProperty("age_minutes", out var a) ? a.GetDouble() : -1;
            var ageStr = age >= 0 ? (age < 60 ? $"{(int)age} דקות" : $"{age / 60:F1} שעות") : "לא ידוע";
            sb.AppendLine($"⏱️ דוח מלפני {ageStr}");

            if (root.TryGetProperty("data", out var data))
            {
                // Safety (שמעון)
                if (data.TryGetProperty("worst_threat", out var wt))
                {
                    var threatIcon = wt.GetString() switch { "OK" => "🟢", "MEDIUM" => "🟡", "HIGH" => "🟠", "CRITICAL" => "🔴", _ => "❓" };
                    sb.AppendLine($"{threatIcon} רמת איום: *{wt.GetString()}*");
                }
                if (data.TryGetProperty("checks", out var checks))
                {
                    int passed = 0, total = checks.GetArrayLength();
                    foreach (var c in checks.EnumerateArray())
                    {
                        var checkPassed = c.TryGetProperty("passed", out var p) && p.GetBoolean();
                        if (checkPassed) passed++;
                        var checkName = c.TryGetProperty("check", out var cn) ? cn.GetString() ?? "" : "";
                        var checkMsg = c.TryGetProperty("message", out var cm) ? cm.GetString() ?? "" : "";
                        var checkIcon = checkPassed ? "✅" : "❌";
                        if (checkMsg.Length > 50) checkMsg = checkMsg[..50] + "...";
                        sb.AppendLine($"  {checkIcon} {checkName}: {checkMsg}");
                    }
                }

                // SOM (אמיר)
                if (data.TryGetProperty("phases", out var phases))
                {
                    if (phases.TryGetProperty("scans", out var scans))
                    {
                        var missRate = scans.TryGetProperty("miss_rate_pct", out var mr) ? mr.GetDouble() : -1;
                        var activeOrders = scans.TryGetProperty("active_orders", out var ao) ? ao.GetInt32() : 0;
                        var staleOrders = scans.TryGetProperty("stale_orders", out var so) ? so.GetInt32() : 0;
                        if (missRate >= 0) sb.AppendLine($"🗺️ Miss rate: *{missRate:F1}%* | Orders: {activeOrders} (stale: {staleOrders})");
                    }
                    if (phases.TryGetProperty("bookings", out var bookings))
                    {
                        var uncovered = bookings.TryGetProperty("uncovered", out var uc) ? uc.GetInt32() : 0;
                        if (uncovered > 0) sb.AppendLine($"⚠️ *{uncovered} הזמנות ללא כיסוי!*");
                    }
                }

                // Control Room (אריה)
                if (data.TryGetProperty("kpis", out var kpis))
                {
                    var rooms = kpis.TryGetProperty("active_rooms", out var ar) ? ar.GetInt32() : 0;
                    var sold = kpis.TryGetProperty("sold_rooms", out var sr) ? sr.GetInt32() : 0;
                    var safety = kpis.TryGetProperty("safety_status", out var ss) ? ss.GetString() : "?";
                    sb.AppendLine($"🏨 חדרים: {rooms} ({sold} נמכרו) | בטיחות: {safety}");
                }

                // Room Seller (יוסי)
                if (data.TryGetProperty("stats", out var stats))
                {
                    var total = stats.TryGetProperty("total", out var t) ? t.GetInt32() : 0;
                    var updated = stats.TryGetProperty("updated", out var u) ? u.GetInt32() : 0;
                    var skipped = stats.TryGetProperty("skipped", out var sk) ? sk.GetInt32() : 0;
                    sb.AppendLine($"📊 חדרים: {total} | עודכנו: {updated} | דולגו: {skipped}");
                }

                // Mapping Fixer (מיכאל)
                if (data.TryGetProperty("phases", out var mPhases) && mPhases.TryGetProperty("phase2", out var p2))
                {
                    var gapTotal = p2.TryGetProperty("total", out var gt) ? gt.GetInt32() : 0;
                    var autoFix = p2.TryGetProperty("auto_fixable", out var af) ? af.GetInt32() : 0;
                    if (gapTotal > 0) sb.AppendLine($"🔧 Gaps: {gapTotal} (auto-fixable: {autoFix})");
                }
            }

            return sb.ToString();
        }
        catch { return null; }
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
            await HandleAgentReportView(chatId, data[6..], messageId);

        else if (data.StartsWith("ask:"))
        {
            var agentName = data[4..];
            // Auto-mute pushes for 15 minutes during conversation
            _conversationMuteUntil = DateTime.UtcNow.AddMinutes(15);
            await SendToGroup($"💬 שיחה עם *{agentName}* — כתוב שאלה.\n_התראות מושתקות ל-15 דקות._", chatId);
            if (_claude.IsAvailable)
                await _claude.ChatWithAgent(chatId, agentName, "__init__");
        }

        else if (data.StartsWith("refresh:"))
            await HandleAgentReportView(chatId, data[8..], messageId);

        else if (data == "team")
            await HandleTeamMenu(chatId);

        else if (data.StartsWith("handoff:"))
        {
            var parts = data[8..].Split(':');
            if (parts.Length == 2)
            {
                var toAgent = parts[1];
                var targetData = await FormatAgentQuickStats(toAgent);
                await HandleAgentReportView(chatId, toAgent, "");
            }
        }

        else if (data.StartsWith("stay:"))
            await SendToGroup($"👍 כתוב שאלה:", chatId);

        else if (data.StartsWith("suggest:"))
            await HandleAgentReportView(chatId, data[8..], "");

        else if (data.StartsWith("cmd:"))
        {
            var cmd = data[4..];
            if (cmd == "dashboard") { await SendDashboardAgents(chatId); await SendDashboardSales(chatId); await SendDashboardRisks(chatId); }
            else if (cmd == "status") await HandleStatus(chatId);
            else if (cmd == "help") await HandleHelp(chatId);
        }

        else if (data.StartsWith("ack:"))
            await SendToGroup("✅ התראה אושרה.", chatId);

        else if (data.StartsWith("snooze:"))
            await SendToGroup("😴 התראה מושתקת לשעה.", chatId);

        else if (data == "endchat")
        {
            _claude.EndAllConversations(chatId);
            _conversationMuteUntil = null;
            await SendToGroup("👋 שיחה נסגרה. התראות חזרו.", chatId);
        }
    }

    // ── Agent Chat (Claude conversation) ─────────────────────────

    private async Task HandleAgentChat(string chatId, string agentName, string userMessage)
    {
        if (!_claude.IsAvailable)
        {
            var fallback = await FormatAgentQuickStats(agentName);
            if (fallback != null)
                await SendInlineKeyboard(chatId, $"*{agentName}:*\n{fallback}\n\n_Claude AI לא זמין_",
                    new[] { new[] { Btn("⬅️ צוות", "team") } });
            else
                await SendToGroup("❌ Claude AI לא זמין", chatId);
            return;
        }

        // Keep conversation mute active
        _conversationMuteUntil = DateTime.UtcNow.AddMinutes(15);

        try
        {
            var response = await _claude.ChatWithAgent(chatId, agentName, userMessage);
            var answer = response.Success ? response.Response : $"שגיאה: {response.Error}";
            if (answer.Length > 3500) answer = answer[..3500] + "\n...";
            var meta = response.DurationMs > 0 ? $"\n_⏱️ {response.DurationMs / 1000.0:F1}s_" : "";

            // Detect mentions of other agents → offer handoff
            var mentioned = DetectMentionedAgents(answer, agentName);

            var buttons = new List<object[]>();
            if (mentioned.Any())
            {
                var row = mentioned.Take(2).Select(m =>
                    Btn($"{AgentIcons.GetValueOrDefault(m, "📋")} {m}", $"agent:{m}")).ToArray();
                buttons.Add(row);
            }
            buttons.Add(new[] { Btn("🔄 דוח", $"agent:{agentName}"), Btn("👋 סיים", "endchat") });

            await SendInlineKeyboard(chatId, $"*{agentName}:*\n\n{answer}{meta}", buttons.ToArray());
        }
        catch (Exception ex)
        {
            await SendInlineKeyboard(chatId, $"❌ שגיאה: {ex.Message}",
                new[] { new[] { Btn("🔄 נסה שוב", $"ask:{agentName}"), Btn("⬅️ צוות", "team") } });
        }
    }

    private static List<string> DetectMentionedAgents(string text, string currentAgent)
    {
        return AgentIcons.Keys
            .Where(a => a != currentAgent && text.Contains(a))
            .ToList();
    }

    // ── Natural Language Handler (simplified) ────────────────────

    private async Task HandleNaturalLanguage(string chatId, string text, string from)
    {
        // 1. End conversation
        var endPatterns = new[] { "ביי", "bye", "סגור", "תודה", "סיום" };
        if (endPatterns.Any(p => text.Trim().StartsWith(p, StringComparison.OrdinalIgnoreCase)))
        {
            var conv = _claude.GetActiveConversation(chatId);
            if (conv != null)
            {
                _claude.EndAllConversations(chatId);
                _conversationMuteUntil = null;
                await SendToGroup($"👋 שיחה עם *{conv.Agent}* נסגרה.", chatId);
                return;
            }
        }

        // 2. Direct agent name → start chat
        var agent = DetectAgentInMessage(text);
        if (agent != null)
        {
            var message = StripAgentName(text, agent);
            await HandleAgentChat(chatId, agent, message);
            return;
        }

        // 3. Active conversation → continue
        var existing = _claude.GetActiveConversation(chatId);
        if (existing != null)
        {
            await HandleAgentChat(chatId, existing.Agent, text);
            return;
        }

        // 4. Kill switch / emergency patterns (keep these — critical safety)
        var lower = text.ToLower().Trim();
        var pinMatch = System.Text.RegularExpressions.Regex.Match(text, @"\b(\d{4})\b");
        var pin = pinMatch.Success ? pinMatch.Groups[1].Value : "";

        if (lower.Contains("עצור הכל") || lower.Contains("kill switch") || lower.Contains("חירום"))
        {
            if (string.IsNullOrEmpty(pin)) { await SendToGroup("⚠️ שלח גם PIN: \"עצור הכל 7743\"", chatId); return; }
            if (!_failSafe.ValidatePin(pin)) { await SendToGroup("❌ PIN שגוי!", chatId); return; }
            _failSafe.TripAll($"Kill Switch by {from}: \"{text}\"", from);
            await SendToGroup($"🚨 *KILL SWITCH!* הופעל ע\"י {from}", chatId);
            return;
        }

        // 5. Everything else → simple fallback with buttons
        await SendInlineKeyboard(chatId,
            "מה תרצה לעשות?",
            new object[][]
            {
                new[] { Btn("🏢 צוות סוכנים", "team"), Btn("📊 דשבורד", "cmd:dashboard") },
                new[] { Btn("📋 סטטוס", "cmd:status"), Btn("❓ עזרה", "cmd:help") },
            });
    }

    // ── Agent Detection Helpers ──────────────────────────────────

    private string? DetectAgentInMessage(string text)
    {
        var lower = text.ToLower().Trim();
        foreach (var (agent, triggers) in AgentTriggers)
            foreach (var trigger in triggers)
                if (lower.StartsWith(trigger) || lower.StartsWith($"@{trigger}"))
                    return agent;
        return null;
    }

    private string StripAgentName(string text, string agent)
    {
        var result = text.Trim();
        foreach (var trigger in AgentTriggers[agent])
        {
            if (result.StartsWith(trigger, StringComparison.OrdinalIgnoreCase))
                return result[trigger.Length..].TrimStart(',', ' ', ':', '-', '—');
            if (result.StartsWith($"@{trigger}", StringComparison.OrdinalIgnoreCase))
                return result[(trigger.Length + 1)..].TrimStart(',', ' ', ':', '-', '—');
        }
        return string.IsNullOrWhiteSpace(result) ? "מה המצב?" : result;
    }

    // ── Telegram Helpers ─────────────────────────────────────────

    private async Task SendToGroup(string text, string? targetChatId = null)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/sendMessage";
            if (text.Length > 4000) text = text[..4000] + "\n...";
            var payload = JsonSerializer.Serialize(new
            {
                chat_id = targetChatId ?? _chatId,
                text, parse_mode = "Markdown", disable_web_page_preview = true
            });
            await _http.PostAsync(url, new StringContent(payload, Encoding.UTF8, "application/json"));
        }
        catch (Exception ex) { _logger.LogDebug("Send error: {Err}", ex.Message); }
    }

    private async Task SendInlineKeyboard(string chatId, string text, object[][] buttons)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/sendMessage";
            if (text.Length > 4000) text = text[..4000];
            var payload = JsonSerializer.Serialize(new
            {
                chat_id = chatId, text, parse_mode = "Markdown", disable_web_page_preview = true,
                reply_markup = new { inline_keyboard = buttons.Select(r => r.Select(b => (object)b).ToArray()).ToArray() }
            });
            await _http.PostAsync(url, new StringContent(payload, Encoding.UTF8, "application/json"));
        }
        catch (Exception ex) { _logger.LogDebug("Keyboard send error: {Err}", ex.Message); }
    }

    private async Task EditMessageWithKeyboard(string chatId, string messageId, string text, object[][]? buttons = null)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/editMessageText";
            if (text.Length > 4000) text = text[..4000];
            var p = new Dictionary<string, object?> { ["chat_id"] = chatId, ["message_id"] = messageId, ["text"] = text, ["parse_mode"] = "Markdown", ["disable_web_page_preview"] = true };
            if (buttons != null) p["reply_markup"] = new { inline_keyboard = buttons.Select(r => r.Select(b => (object)b).ToArray()).ToArray() };
            await _http.PostAsync(url, new StringContent(JsonSerializer.Serialize(p), Encoding.UTF8, "application/json"));
        }
        catch (Exception ex) { _logger.LogDebug("Edit error: {Err}", ex.Message); }
    }

    private async Task AnswerCallbackQuery(string callbackQueryId, string? text = null)
    {
        try
        {
            var url = $"https://api.telegram.org/bot{_botToken}/answerCallbackQuery";
            var p = new Dictionary<string, object?> { ["callback_query_id"] = callbackQueryId };
            if (text != null) p["text"] = text;
            await _http.PostAsync(url, new StringContent(JsonSerializer.Serialize(p), Encoding.UTF8, "application/json"));
        }
        catch { }
    }

    private static object Btn(string text, string callbackData) =>
        new { text, callback_data = callbackData };
}
