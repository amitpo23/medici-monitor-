using System.Text;
using System.Text.Json;

namespace MediciMonitor.Services;

public partial class TelegramBotService
{
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
        {
            var wasExpired = existing == null && agentName == agentName; // new session
            await SendToGroup($"💬 פותח שיחה עם *{agentName}*...\n_כתוב כל הודעה ו{agentName} ימשיך לענות. כתוב \"ביי\" או שם סוכן אחר כדי לעבור._", chatId);
        }

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
