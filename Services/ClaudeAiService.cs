using System.Text;
using System.Text.Json;
using System.Diagnostics;
using System.Net.Http.Headers;
using Anthropic.SDK;
using Anthropic.SDK.Messaging;

namespace MediciMonitor.Services;

/// <summary>
/// AI-powered analysis using Claude.
/// Three modes: (1) API Key → SDK  (2) OAuth token → direct API  (3) Claude CLI → OAuth (local only)
/// </summary>
public class ClaudeAiService
{
    private readonly ILogger<ClaudeAiService> _logger;
    private readonly DataService _data;
    private readonly AzureMonitoringService _azure;
    private readonly SystemMonitorService _monitor;
    private readonly IConfiguration _config;
    private AnthropicClient? _client;

    private enum AiMode { None, Sdk, OAuth, Cli }
    private AiMode _mode = AiMode.None;
    private string _cliPath = "";

    // OAuth token fields
    private string? _oauthAccessToken;
    private string? _oauthRefreshToken;
    private long _oauthExpiresAt;   // ms since epoch
    private static readonly HttpClient _http = new();
    private const string AnthropicApiUrl = "https://api.anthropic.com/v1/messages";
    private const string AnthropicTokenUrl = "https://platform.claude.com/v1/oauth/token";

    private string _model = "claude-sonnet-4-20250514";
    private int _maxTokens = 2048;

    public ClaudeAiService(
        ILogger<ClaudeAiService> logger,
        DataService data,
        AzureMonitoringService azure,
        SystemMonitorService monitor,
        IConfiguration config)
    {
        _logger = logger;
        _data = data;
        _azure = azure;
        _monitor = monitor;
        _config = config;
        Init();
    }

    // ─── Initialisation (SDK → OAuth token → CLI fallback) ────────

    private void Init()
    {
        _model = _config["Claude:Model"] ?? _model;
        if (int.TryParse(_config["Claude:MaxTokens"], out var mt) && mt > 0)
            _maxTokens = mt;

        // Priority 1: API Key → Anthropic SDK
        try
        {
            var apiKey = _config["Claude:ApiKey"]
                         ?? Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY");
            if (!string.IsNullOrWhiteSpace(apiKey))
            {
                _client = new AnthropicClient(apiKey);
                _mode = AiMode.Sdk;
                _logger.LogInformation("Claude AI → SDK mode (API Key), model {Model}", _model);
                return;
            }
        }
        catch (Exception ex) { _logger.LogWarning("SDK init failed: {Err}", ex.Message); }

        // Priority 2: OAuth token (from config, env, or macOS keychain)
        try
        {
            if (TryLoadOAuthToken())
            {
                _mode = AiMode.OAuth;
                var expiresIn = TimeSpan.FromMilliseconds(Math.Max(0, _oauthExpiresAt - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
                _logger.LogInformation("Claude AI → OAuth mode (Max subscription), token expires in {H}h {M}m",
                    (int)expiresIn.TotalHours, expiresIn.Minutes);
                return;
            }
        }
        catch (Exception ex) { _logger.LogDebug("OAuth token load failed: {Err}", ex.Message); }

        // Priority 3: Claude CLI (OAuth from Max subscription — local only)
        try
        {
            var cliPath = _config["Claude:CliPath"] ?? "claude";
            var psi = new ProcessStartInfo
            {
                FileName = cliPath, Arguments = "--version",
                UseShellExecute = false, RedirectStandardOutput = true,
                RedirectStandardError = true, CreateNoWindow = true
            };
            using var proc = Process.Start(psi);
            if (proc != null)
            {
                proc.WaitForExit(5000);
                if (proc.ExitCode == 0)
                {
                    _cliPath = cliPath;
                    _mode = AiMode.Cli;
                    var ver = proc.StandardOutput.ReadToEnd().Trim();
                    _logger.LogInformation("Claude AI → CLI mode (OAuth), version {Ver}", ver);
                    return;
                }
            }
        }
        catch (Exception ex) { _logger.LogDebug("CLI detect failed: {Err}", ex.Message); }

        _logger.LogWarning("Claude AI unavailable — no API key, no OAuth token, no CLI found");
    }

    // ─── OAuth token loading (config / env / macOS keychain) ──────

    private bool TryLoadOAuthToken()
    {
        // Source 1: Config / env vars
        var accessToken = _config["Claude:OAuthAccessToken"]
                          ?? Environment.GetEnvironmentVariable("CLAUDE_OAUTH_ACCESS_TOKEN");
        var refreshToken = _config["Claude:OAuthRefreshToken"]
                           ?? Environment.GetEnvironmentVariable("CLAUDE_OAUTH_REFRESH_TOKEN");

        // Source 2: macOS keychain (local dev)
        if (string.IsNullOrWhiteSpace(accessToken))
        {
            try
            {
                var psi = new ProcessStartInfo
                {
                    FileName = "security",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };
                psi.ArgumentList.Add("find-generic-password");
                psi.ArgumentList.Add("-s");
                psi.ArgumentList.Add("Claude Code-credentials");
                psi.ArgumentList.Add("-w");

                using var proc = Process.Start(psi);
                if (proc != null)
                {
                    var json = proc.StandardOutput.ReadToEnd().Trim();
                    proc.WaitForExit(5000);
                    if (proc.ExitCode == 0 && json.StartsWith("{"))
                    {
                        using var doc = JsonDocument.Parse(json);
                        var oauth = doc.RootElement.GetProperty("claudeAiOauth");
                        accessToken = oauth.GetProperty("accessToken").GetString();
                        refreshToken = oauth.GetProperty("refreshToken").GetString();
                        _oauthExpiresAt = oauth.GetProperty("expiresAt").GetInt64();
                        _logger.LogInformation("OAuth token loaded from macOS keychain");
                    }
                }
            }
            catch (Exception ex) { _logger.LogDebug("Keychain read failed: {Err}", ex.Message); }
        }

        if (string.IsNullOrWhiteSpace(accessToken))
            return false;

        _oauthAccessToken = accessToken;
        _oauthRefreshToken = refreshToken;

        // If expiresAt not set (from env/config), set to 1 hour from now as default
        if (_oauthExpiresAt == 0)
            _oauthExpiresAt = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds();

        return true;
    }

    // ─── OAuth token refresh ──────────────────────────────────────

    private async Task<bool> RefreshOAuthTokenAsync()
    {
        if (string.IsNullOrWhiteSpace(_oauthRefreshToken))
        {
            _logger.LogWarning("Cannot refresh OAuth token — no refresh token available");
            return false;
        }

        try
        {
            var body = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "refresh_token"),
                new KeyValuePair<string, string>("refresh_token", _oauthRefreshToken),
                new KeyValuePair<string, string>("client_id", "9d1c250a-e61b-44d9-88ed-5944d1962f5e")
            });

            var resp = await _http.PostAsync(AnthropicTokenUrl, body);
            var json = await resp.Content.ReadAsStringAsync();

            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogWarning("OAuth refresh failed: {Status} {Body}", resp.StatusCode, json);
                return false;
            }

            using var doc = JsonDocument.Parse(json);
            _oauthAccessToken = doc.RootElement.GetProperty("access_token").GetString();
            if (doc.RootElement.TryGetProperty("refresh_token", out var rt))
                _oauthRefreshToken = rt.GetString();
            var expiresIn = doc.RootElement.TryGetProperty("expires_in", out var ei) ? ei.GetInt64() : 3600;
            _oauthExpiresAt = DateTimeOffset.UtcNow.AddSeconds(expiresIn).ToUnixTimeMilliseconds();

            _logger.LogInformation("OAuth token refreshed, expires in {Sec}s", expiresIn);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OAuth token refresh failed");
            return false;
        }
    }

    public bool IsAvailable => _mode != AiMode.None;
    public string Mode => _mode.ToString();

    // ─── System prompt (Hebrew, Medici context) ───────────────────

    private static readonly string SystemPrompt = @"
אתה מומחה DevOps ו-BI בשם Claude, משולב בלוח הבקרה של מערכת Medici Hotels.
המערכת מנהלת הזמנות מלונות — שירות ASP.NET על Azure, עם Azure SQL, WebJobs ו-Azure Monitor.

כללים:
• ענה תמיד בעברית (מונחים טכניים באנגלית בתוך גרשיים).
• תן תשובות קצרות, ממוקדות ומעשיות.
• כשמנתח התראה או שגיאה — הסבר מה קרה, מה הסיבה הכי סבירה, ומה לעשות עכשיו.
• כשמסכם מצב — השתמש בנקודות עם אימוג'ים (✅ ⚠️ 🔴 📊 💡).
• אל תמציא נתונים — אם אין לך מספיק מידע, אמור זאת.
".Trim();

    // ─── 1. Analyse Azure Monitor Alerts ─────────────────────────

    public async Task<AiAnalysisResult> AnalyseAlerts()
    {
        if (!IsAvailable) return Unavailable();
        try
        {
            var alerts = await _azure.GetFiredAzureMonitorAlerts();
            if (alerts.Count == 0)
                return new AiAnalysisResult { Success = true, Response = "✅ אין התראות Azure Monitor פעילות כרגע. הכל תקין." };

            var alertJson = JsonSerializer.Serialize(alerts.Take(10), JsonOpts);

            var prompt = $@"להלן התראות Azure Monitor שנורו ב-24 שעות האחרונות:
```json
{alertJson}
```

נתח את ההתראות:
1. סיכום כללי (כמה פעילות, כמה נפתרו)
2. לכל התראה פעילה (Fired) — הסבר מה קרה, חומרת הבעיה, והמלצה מיידית
3. האם יש מגמה מדאיגה?
4. סדר עדיפויות — מה לטפל קודם?";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AnalyseAlerts failed");
            return Error(ex);
        }
    }

    // ─── 2. Error Root-Cause Analysis ────────────────────────────

    public async Task<AiAnalysisResult> AnalyseErrors()
    {
        if (!IsAvailable) return Unavailable();
        try
        {
            var status = await _data.GetFullStatus();

            // Collect all error types into one list
            var errorSummaries = new List<object>();
            foreach (var e in status.RecentBookingErrors.Take(10))
                errorSummaries.Add(new { Type = "BookingError", e.PreBookId, e.Error, e.Code, e.DateInsert });
            foreach (var e in status.RecentCancelErrors.Take(5))
                errorSummaries.Add(new { Type = "CancelError", e.PreBookId, e.Error, e.DateInsert });
            foreach (var e in status.RecentBackOfficeErrors.Take(5))
                errorSummaries.Add(new { Type = "BackOfficeError", e.ErrorLog, e.DateCreate });

            if (errorSummaries.Count == 0)
                return new AiAnalysisResult { Success = true, Response = "✅ אין שגיאות אחרונות. המערכת פועלת ללא תקלות." };

            var errJson = JsonSerializer.Serialize(errorSummaries, JsonOpts);

            var prompt = $@"להלן 20 השגיאות האחרונות ממערכת Medici Hotels:
```json
{errJson}
```

מידע מערכת:
- DB מחובר: {status.DbConnected}
- שגיאות הזמנות 24ש: {status.BookingErrorsLast24h}
- שגיאות ביטול 24ש: {status.CancelErrorsLast24h}
- שגיאות BackOffice 24ש: {status.BackOfficeErrorsLast24h}

נתח את השגיאות:
1. קבץ לפי סוג/תבנית
2. זהה את שורש הבעיה (root cause) העיקרי
3. האם יש correlation בין שגיאות?
4. מה ההמלצה המיידית?
5. רמת חומרה (קריטי / אזהרה / מידע)";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AnalyseErrors failed");
            return Error(ex);
        }
    }

    // ─── 3. Booking Anomaly Detection ────────────────────────────

    public async Task<AiAnalysisResult> AnalyseBookings()
    {
        if (!IsAvailable) return Unavailable();
        try
        {
            var status = await _data.GetFullStatus();
            var summary = new
            {
                status.TotalActiveBookings,
                status.RoomsBoughtToday,
                status.BoughtToday,
                status.SoldToday,
                status.BoughtTodayValue,
                status.SoldTodayValue,
                status.ProfitLoss,
                status.ConversionRate,
                status.BookingErrorsLast24h,
                status.CancelErrorsLast24h,
                status.StuckCancellations,
                status.PriceDriftCount,
                status.PriceDriftTotalImpact,
                status.WasteRoomsTotal,
                status.WasteTotalValue,
                status.DbConnected,
                PriceDrifts = status.PriceDrifts?.Take(10),
                ConversionByHotel = status.ConversionByHotel?.Take(10)
            };

            var json = JsonSerializer.Serialize(summary, JsonOpts);

            var prompt = $@"להלן מצב ההזמנות הנוכחי ב-Medici Hotels:
```json
{json}
```

נתח את הנתונים:
1. האם יש אנומליות בקצב ההזמנות?
2. האם אחוז הכשלונות חריג?
3. ניתוח הכנסות — האם המספרים הגיוניים?
4. סחף מחירים (price drift) — האם יש סיכון?
5. סיכום מצב + ציון (1-10) לבריאות המערכת
6. המלצות לשיפור";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AnalyseBookings failed");
            return Error(ex);
        }
    }

    // ─── 4. Daily Briefing ───────────────────────────────────────

    public async Task<AiAnalysisResult> GenerateBriefing()
    {
        if (!IsAvailable) return Unavailable();
        try
        {
            // Gather data from multiple sources
            var status = await _data.GetFullStatus();
            var alerts = await _azure.GetFiredAzureMonitorAlerts();
            var health = await _azure.ComprehensiveApiHealthCheck();

            var briefData = new
            {
                System = new
                {
                    status.DbConnected,
                    status.TotalActiveBookings,
                    status.RoomsBoughtToday,
                    status.BoughtToday,
                    status.SoldToday,
                    status.BoughtTodayValue,
                    status.SoldTodayValue,
                    status.ProfitLoss,
                    status.BookingErrorsLast24h,
                    status.CancelErrorsLast24h,
                    status.StuckCancellations,
                    status.QueuePending,
                    status.QueueErrors
                },
                MonitorAlerts = alerts.Take(5).Select(a => new
                {
                    a.Name,
                    a.Severity,
                    a.MonitorCondition,
                    a.FiredTime,
                    a.TargetResourceName
                }),
                ApiHealth = health.Take(5).Select(h => new
                {
                    h.Endpoint,
                    h.IsHealthy,
                    h.ResponseTimeMs
                })
            };

            var json = JsonSerializer.Serialize(briefData, JsonOpts);

            var prompt = $@"צור סיכום יומי (daily briefing) למנהל המערכת של Medici Hotels.
הנתונים:
```json
{json}
```

פורמט הסיכום:
📋 **סיכום יומי — {DateTime.UtcNow:dd/MM/yyyy}**

1. 🏥 מצב המערכת (ציון 1-10)
2. 📊 הזמנות ומכירות
3. ⚠️ התראות ובעיות פתוחות
4. 🌐 בריאות APIs ושירותים
5. 💡 המלצות לפעולה מיידית
6. 📈 מגמות לשים לב אליהן";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GenerateBriefing failed");
            return Error(ex);
        }
    }

    // ─── 5. Analyse System Monitor ──────────────────────────────

    public async Task<AiAnalysisResult> AnalyseMonitor()
    {
        if (!IsAvailable) return Unavailable();
        try
        {
            var report = _monitor.LastReport ?? await _monitor.RunFullScan();
            var trend = _monitor.GetTrendAnalysis(24);

            var monitorData = new
            {
                report.Timestamp,
                AlertCount = report.Alerts.Count,
                Alerts = report.Alerts.Select(a => new { a.Severity, a.Component, a.Message }),
                Trend = new { trend.OverallTrend, trend.HealthPct, trend.TotalRuns, trend.FirstHalfAlerts, trend.SecondHalfAlerts,
                    EscalationComponents = trend.Components.Where(c => c.Value.ConsecutiveCritical >= 3).Select(c => new { Component = c.Key, c.Value.ConsecutiveCritical }) },
                Checks = report.Results.Keys.ToList(),
                report.Results
            };

            var json = JsonSerializer.Serialize(monitorData, JsonOpts);

            var prompt = $@"להלן תוצאות סריקת System Monitor מלאה של מערכת Medici Hotels (13 בדיקות):
```json
{json}
```

הסריקה כוללת: WebJob, Tables, Mapping, Skills, Orders, Zenith SOAP, Cancellation, Cancel Errors, BuyRooms, Reservations, Price Override Pipeline, Data Freshness, Booking Sales.

נתח את התוצאות:
1. 🏥 **מצב כללי** — ציון 1-10 לבריאות המערכת
2. 🔴 **בעיות קריטיות** — מה דורש טיפול מיידי?
3. 🟡 **אזהרות** — מה כדאי לשים לב אליו?
4. 📈 **טרנד** — האם המערכת משתפרת או מידרדרת?
5. 🛒 **BuyRooms & Sales** — האם הרכישות והמכירות תקינות? conversion rate?
6. 🌐 **Zenith & Reservations** — האם הממשק עם Zenith תקין? callbacks מגיעים?
7. 🗺️ **Mapping** — האם יש בעיות mapping שמונעות סריקות?
8. 📡 **Data Freshness** — האם הנתונים עדכניים?
9. 💡 **המלצות** — 3 דברים לעשות עכשיו
10. ⚠️ **ESCALATION** — האם יש רכיבים עם CRITICAL חוזרים?";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AnalyseMonitor failed");
            return Error(ex);
        }
    }

    // ─── 6. Chat with full Monitor context ────────────────────────

    public async Task<AiAnalysisResult> ChatWithMonitor(string userMessage)
    {
        if (!IsAvailable) return Unavailable();
        if (string.IsNullOrWhiteSpace(userMessage))
            return new AiAnalysisResult { Success = false, Error = "הודעה ריקה" };

        try
        {
            // Gather rich context from multiple sources
            var status = await _data.GetFullStatus();
            var report = _monitor.LastReport;
            var trend = _monitor.GetTrendAnalysis(24);

            var context = new
            {
                SystemStatus = new
                {
                    status.DbConnected, status.TotalActiveBookings, status.BoughtToday, status.SoldToday,
                    status.BoughtTodayValue, status.SoldTodayValue, status.ProfitLoss,
                    status.BookingErrorsLast24h, status.CancelErrorsLast24h, status.StuckCancellations,
                    status.WasteRoomsTotal, status.WasteTotalValue, status.QueuePending,
                    status.SalesOfficePending, status.SalesOfficeInProgress, status.SalesOfficeFailed,
                    status.ReservationsToday, status.BuyRoomsHealthy
                },
                Monitor = report != null ? new
                {
                    report.Timestamp,
                    AlertCount = report.Alerts.Count,
                    CriticalAlerts = report.Alerts.Where(a => a.Severity is "CRITICAL" or "EMERGENCY")
                        .Select(a => $"{a.Component}: {a.Message}").ToList(),
                    WarningAlerts = report.Alerts.Where(a => a.Severity == "WARNING")
                        .Select(a => $"{a.Component}: {a.Message}").ToList(),
                    Checks = report.Results.Keys.ToList(),
                    Results = report.Results
                } : null as object,
                Trend = new { trend.OverallTrend, trend.HealthPct, trend.TotalRuns,
                    trend.FirstHalfAlerts, trend.SecondHalfAlerts }
            };

            var json = JsonSerializer.Serialize(context, JsonOpts);

            var prompt = $@"הקשר מלא של מערכת Medici Hotels (סטטוס + System Monitor + טרנדים):
```json
{json}
```

10 תהליכים פועלים: (1) SalesOffice Scanning (2) BuyRoom (3) Reservation Callback (4) Auto-Cancellation (5) Sales Management (6) MediMap (7) Price Override (8) Insert Opp (9) System Monitor (10) Hotel Data Explorer.

13 בדיקות מוניטור: WebJob, Tables, Mapping, Skills, Orders, Zenith, Cancellation, CancelErrors, BuyRooms, Reservations, PriceOverridePipeline, DataFreshness, BookingSales.

שאלת המשתמש: {userMessage}

ענה בעברית. אם השאלה דורשת חקירה ספציפית — ציין איזו בדיקה (/monitor_check <name>) או פקודה (/hotel, /trace, /verify) כדאי להריץ.
אם יש בעיה — הסבר מה הגורם, מה ההשפעה העסקית, ומה לעשות עכשיו.";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ChatWithMonitor failed");
            return Error(ex);
        }
    }

    // ─── 7. Chat with Agent (personality + live data) ─────────────

    private static readonly Dictionary<string, (string Role, string Personality)> AgentProfiles = new()
    {
        ["אריה"] = ("מנהל חדר בקרה + מזכיר ראשי (Control Room)", @"אתה אריה, מנהל חדר הבקרה והמזכיר הראשי של הארגון. אתה מכיר את כל 26 הסוכנים, יודע מי עובד על מה, ומפנה את המנכ""ל לסוכן הנכון.

הנה מי עושה מה:
• אמיר (SOM) — מנהל Sales Orders, סריקות, miss rate, הזמנות, רכישות. כל שאלה על orders/bookings/scans.
• שמעון (Safety Officer) — בטיחות, kill switch, חשיפה כספית, spending, reconciliation. כל שאלה על סיכונים/בטיחות.
• יוסי (Room Seller) — תמחור חדרים, 7% מתחת למתחרה, margins. כל שאלה על מחירים/חדרים/מכירות.
• רוני (Hotel Completion) — מוודא שחדרים מופיעים ב-B2B, safety wall, נראות. כל שאלה על חשיפה/B2B.
• מיכאל (Mapping Fixer) — מיפויים, gaps מסוג A/B/C/D/E, venues. כל שאלה על mapping/gaps.
• גבי (Autofix Worker) — תיקון מהיר של Type A gaps כל 10 דקות.
• יעל (System Monitor) — מוניטור מערכת, 13 בדיקות, WebJobs, Zenith, data freshness. כל שאלה על ניטור/בריאות מערכת.
• דני (Coordinator) — תיאום בין Hotels↔Prediction↔Monitor. כל שאלה על אינטגרציה בין מערכות.
• משה (Kill Switch) — כפתור חירום, עוצר הכל כשיש אנומליה.
• אלי (BuyRoom) — רוכש חדרים מספקים (WebJob).
• נתן (Lastprice Rebuy) — קונה מחדש כשהמחיר יורד 10%.
• עמית (SalesOffice Scanning) — סורק WebJob.
• שרה (Reservation Callback) — מטפלת ב-Zenith callbacks.
• רינה (Auto Cancellation) — מבטלת חדרים לפני deadline.

כשמישהו שואל שאלה — תענה ממה שאתה יודע, ואם זה תחום של סוכן אחר תגיד ""תכתוב לי את שם הסוכן (למשל: שמעון מה המצב?) ואני אעביר אליו"".
אתה מסודר, פקטואלי ותמציתי."),
        ["אמיר"] = ("מנכ\"ל — מנהל Sales Orders (SOM)", "אתה אמיר, המנכ\"ל. אתה מנהל את כל מחזור החיים של Sales Orders — מסריקה ועד הזמנה. אתה מכיר כל order, כל miss rate, וכל booking. אתה עסקי, ישיר ומתמקד בתוצאות."),
        ["שמעון"] = ("קצין בטיחות (Safety Officer)", "אתה שמעון, קצין הבטיחות. אתה אחראי על kill switch, חשיפה כספית, spending, ו-reconciliation. אתה זהיר, מדויק ולא מתפשר על בטיחות. כשיש סיכון — אתה מזהיר מיד."),
        ["דני"] = ("מתאם מערכות (Coordinator)", "אתה דני, השגריר בין 3 הפרויקטים — Hotels, Prediction, Monitor. אתה יודע מה קורה בכל מערכת ואיך הן מתחברות."),
        ["יוסי"] = ("סוכן מכירות (Room Seller)", "אתה יוסי, סוכן המכירות. אתה מתמחר חדרים 7% מתחת למתחרה עם מינימום $30 רווח. אתה מכיר כל מלון, כל מחיר, וכל margin."),
        ["רוני"] = ("מנהל השלמות (Hotel Completion)", "אתה רוני, מנהל ההשלמות. אתה מוודא שכל חדר שקנינו באמת מופיע ב-B2B ואפשר למכור אותו. Safety wall זה התחום שלך."),
        ["מיכאל"] = ("מתקן מיפויים (Mapping Fixer)", "אתה מיכאל, מתקן המיפויים. אתה מסווג gaps ל-5 סוגים (A/B/C/D/E) ומתקן אוטומטית מה שאפשר. אתה מתודי ומדויק."),
        ["גבי"] = ("מתקן מהיר (Autofix Worker)", "אתה גבי, המתקן המהיר. אתה מטפל ב-Type A gaps מ-MappingMisses כל 10 דקות. אתה מהיר, יעיל ומדווח על כל תיקון."),
        ["יעל"] = ("מפקחת (System Monitor)", "אתה יעל, המפקחת. אתה מריצה 13 בדיקות על 7 תחומים — WebJobs, Tables, Mapping, Zenith, BuyRooms, Reservations, Data Freshness. את מדויקת ולא מפספסת שום דבר."),
        ["משה"] = ("שומר (Kill Switch)", "אתה משה, השומר. אתה מפעיל את כפתור החירום כשיש אנומליה. אתה ממוקד, רציני, ולא מהסס לעצור הכל כשצריך."),
    };

    // ── Agent Conversation State ─────────────────────────────────
    // Key = "chatId:agentName", Value = conversation messages + metadata
    public class AgentConversation
    {
        public string Agent { get; set; } = "";
        public List<(string Role, string Content)> Messages { get; } = new();
        public DateTime LastActivity { get; set; } = DateTime.UtcNow;
        public string? LastDataSnapshot { get; set; }
    }

    private readonly Dictionary<string, AgentConversation> _agentConversations = new();
    private static readonly TimeSpan ConversationTimeout = TimeSpan.FromMinutes(30);
    private const int MaxConversationTurns = 20;

    public AgentConversation? GetActiveConversation(string chatId)
    {
        // Find any active conversation for this chat
        foreach (var (key, conv) in _agentConversations)
        {
            if (key.StartsWith($"{chatId}:") && DateTime.UtcNow - conv.LastActivity < ConversationTimeout)
                return conv;
        }
        return null;
    }

    public void EndConversation(string chatId, string agentName)
    {
        _agentConversations.Remove($"{chatId}:{agentName}");
    }

    public void EndAllConversations(string chatId)
    {
        var keys = _agentConversations.Keys.Where(k => k.StartsWith($"{chatId}:")).ToList();
        foreach (var k in keys) _agentConversations.Remove(k);
    }

    public async Task<AiAnalysisResult> ChatWithAgent(string chatId, string agentName, string userMessage)
    {
        if (!IsAvailable) return Unavailable();
        if (string.IsNullOrWhiteSpace(userMessage))
            return new AiAnalysisResult { Success = false, Error = "הודעה ריקה" };

        try
        {
            var convKey = $"{chatId}:{agentName}";

            // Clean expired conversations
            var expired = _agentConversations
                .Where(kv => DateTime.UtcNow - kv.Value.LastActivity > ConversationTimeout)
                .Select(kv => kv.Key).ToList();
            foreach (var k in expired) _agentConversations.Remove(k);

            // Get or create conversation
            if (!_agentConversations.TryGetValue(convKey, out var conv))
            {
                // Starting new conversation — switch away from any other active agent
                var otherKeys = _agentConversations.Keys
                    .Where(k => k.StartsWith($"{chatId}:") && k != convKey).ToList();
                foreach (var k in otherKeys) _agentConversations.Remove(k);

                conv = new AgentConversation { Agent = agentName };
                _agentConversations[convKey] = conv;
            }

            conv.LastActivity = DateTime.UtcNow;

            // Fetch fresh agent data every few turns or on first message
            if (conv.Messages.Count == 0 || conv.Messages.Count % 6 == 0)
            {
                try
                {
                    var encodedName = Uri.EscapeDataString(agentName);
                    conv.LastDataSnapshot = await _http.GetStringAsync($"http://127.0.0.1:5050/agent/{encodedName}");
                }
                catch { conv.LastDataSnapshot ??= "{}"; }
            }

            // Add user message
            conv.Messages.Add(("user", userMessage));

            // Trim if too long (keep first 2 + last N)
            if (conv.Messages.Count > MaxConversationTurns)
            {
                var first = conv.Messages.Take(2).ToList();
                var recent = conv.Messages.Skip(conv.Messages.Count - (MaxConversationTurns - 2)).ToList();
                conv.Messages.Clear();
                conv.Messages.AddRange(first);
                conv.Messages.Add(("user", "[...חלק מהשיחה קוצר...]"));
                conv.Messages.AddRange(recent);
            }

            // Build system prompt
            var (role, personality) = AgentProfiles.TryGetValue(agentName, out var profile)
                ? profile
                : ($"סוכן {agentName}", $"אתה {agentName}, סוכן במערכת Medici Hotels.");

            var agentSystemPrompt = $@"
{personality}

אתה חלק ממערכת של 26 סוכנים שמנהלים הזמנות מלונות במיאמי.
תפקידך: {role}.
אתה נמצא בשיחה ישירה עם המנכ""ל (עמית) דרך טלגרם.

כללים:
• ענה תמיד בעברית, מונחים טכניים באנגלית.
• דבר בגוף ראשון — ""אני בדקתי"", ""אני רואה"", ""אצלי במערכת"".
• היצמד לנתונים האמיתיים שלך — אל תמציא מספרים.
• אם שואלים על תחום של סוכן אחר — הפנה אליו בשמו.
• אתה בשיחה רציפה — זכור מה דובר קודם וענה בהקשר.
• תן תשובות קצרות, ישירות ומעשיות.
• כשיש בעיה — תגיד מה קורה, למה, ומה ההמלצה שלך.
• אם המנכ""ל נותן לך הוראה — אשר שהבנת ותגיד מה תעשה.

הנתונים העדכניים שלך:
```json
{conv.LastDataSnapshot ?? "{}"}
```".Trim();

            // Build multi-turn messages for Claude
            var result = await AskMultiTurn(agentSystemPrompt, conv.Messages);

            // Add assistant response to history
            if (result.Success && !string.IsNullOrEmpty(result.Response))
                conv.Messages.Add(("assistant", result.Response));

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ChatWithAgent failed for {Agent}", agentName);
            return Error(ex);
        }
    }

    // ── Multi-turn conversation call ────────────────────────────

    private async Task<AiAnalysisResult> AskMultiTurn(string systemPrompt, List<(string Role, string Content)> messages)
    {
        if (_mode == AiMode.Sdk)
            return await AskMultiTurnViaSdk(systemPrompt, messages);
        if (_mode == AiMode.OAuth)
            return await AskMultiTurnViaOAuth(systemPrompt, messages);
        if (_mode == AiMode.Cli)
        {
            // CLI doesn't support multi-turn natively — flatten to single prompt
            var flat = string.Join("\n\n", messages.Select(m =>
                m.Role == "user" ? $"המנכ\"ל: {m.Content}" : $"אתה: {m.Content}"));
            return await AskViaCli(flat, systemPrompt);
        }
        return Unavailable();
    }

    private async Task<AiAnalysisResult> AskMultiTurnViaSdk(string systemPrompt, List<(string Role, string Content)> messages)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var msgList = messages.Select(m =>
                new Message(m.Role == "user" ? RoleType.User : RoleType.Assistant, m.Content)).ToList();

            var parameters = new MessageParameters
            {
                Messages = msgList,
                MaxTokens = _maxTokens,
                Model = _model,
                Stream = false,
                Temperature = 0.3m,
                System = new List<SystemMessage> { new SystemMessage(systemPrompt) }
            };

            var result = await _client!.Messages.GetClaudeMessageAsync(parameters);
            sw.Stop();

            return new AiAnalysisResult
            {
                Success = true,
                Response = result.Message?.ToString() ?? "",
                Model = _model,
                Mode = "SDK",
                InputTokens = result.Usage?.InputTokens ?? 0,
                OutputTokens = result.Usage?.OutputTokens ?? 0,
                DurationMs = sw.ElapsedMilliseconds
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "Multi-turn SDK call failed");
            return new AiAnalysisResult { Success = false, Error = ex.Message, DurationMs = sw.ElapsedMilliseconds };
        }
    }

    private async Task<AiAnalysisResult> AskMultiTurnViaOAuth(string systemPrompt, List<(string Role, string Content)> messages)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (now >= _oauthExpiresAt - 60_000)
            {
                if (!await RefreshOAuthTokenAsync())
                    throw new Exception("OAuth token expired and refresh failed");
            }

            var payload = new
            {
                model = _model,
                max_tokens = _maxTokens,
                temperature = 0.3,
                system = systemPrompt,
                messages = messages.Select(m => new { role = m.Role, content = m.Content }).ToArray()
            };

            var json = JsonSerializer.Serialize(payload);
            using var req = new HttpRequestMessage(HttpMethod.Post, AnthropicApiUrl)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
            req.Headers.Add("anthropic-version", "2023-06-01");
            req.Headers.Add("anthropic-beta", "oauth-2025-04-20");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _oauthAccessToken);

            var resp = await _http.SendAsync(req);
            var body = await resp.Content.ReadAsStringAsync();

            if (resp.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                if (await RefreshOAuthTokenAsync())
                {
                    using var req2 = new HttpRequestMessage(HttpMethod.Post, AnthropicApiUrl)
                    {
                        Content = new StringContent(json, Encoding.UTF8, "application/json")
                    };
                    req2.Headers.Add("anthropic-version", "2023-06-01");
                    req2.Headers.Add("anthropic-beta", "oauth-2025-04-20");
                    req2.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _oauthAccessToken);
                    resp = await _http.SendAsync(req2);
                    body = await resp.Content.ReadAsStringAsync();
                }
            }

            if (!resp.IsSuccessStatusCode)
                throw new Exception($"Anthropic API {resp.StatusCode}: {body}");

            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            var text = "";
            if (root.TryGetProperty("content", out var content) && content.GetArrayLength() > 0)
                text = content[0].GetProperty("text").GetString() ?? "";

            int inTok = 0, outTok = 0;
            if (root.TryGetProperty("usage", out var usage))
            {
                inTok = usage.TryGetProperty("input_tokens", out var it) ? it.GetInt32() : 0;
                outTok = usage.TryGetProperty("output_tokens", out var ot) ? ot.GetInt32() : 0;
            }

            sw.Stop();
            return new AiAnalysisResult
            {
                Success = true,
                Response = text,
                Model = _model,
                Mode = "OAuth (Max)",
                InputTokens = inTok,
                OutputTokens = outTok,
                DurationMs = sw.ElapsedMilliseconds
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "Multi-turn OAuth call failed");
            return new AiAnalysisResult { Success = false, Error = ex.Message, DurationMs = sw.ElapsedMilliseconds };
        }
    }

    // ─── 8. Free-form Chat (basic) ────────────────────────────────

    public async Task<AiAnalysisResult> Chat(string userMessage)
    {
        if (!IsAvailable) return Unavailable();
        if (string.IsNullOrWhiteSpace(userMessage))
            return new AiAnalysisResult { Success = false, Error = "הודעה ריקה" };

        try
        {
            // Provide current system context for the chat
            var status = await _data.GetFullStatus();
            var brief = new
            {
                status.DbConnected,
                status.TotalActiveBookings,
                status.BoughtToday,
                status.SoldToday,
                status.BoughtTodayValue,
                status.SoldTodayValue,
                status.BookingErrorsLast24h,
                status.StuckCancellations,
                status.QueuePending
            };

            var context = JsonSerializer.Serialize(brief, JsonOpts);

            var prompt = $@"מצב המערכת הנוכחי:
```json
{context}
```

שאלת המשתמש: {userMessage}

ענה בהתבסס על הנתונים ועל הידע שלך על מערכות מלונאות, Azure, ו-DevOps.";

            return await Ask(prompt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Chat failed");
            return Error(ex);
        }
    }

    // ─── Core call to Claude (three modes) ──────────────────────

    private async Task<AiAnalysisResult> Ask(string userPrompt)
        => await AskWithSystem(SystemPrompt, userPrompt);

    private async Task<AiAnalysisResult> AskWithSystem(string systemPrompt, string userPrompt)
    {
        if (_mode == AiMode.Sdk)
            return await AskViaSdk(userPrompt, systemPrompt);
        if (_mode == AiMode.OAuth)
            return await AskViaOAuth(userPrompt, systemPrompt);
        if (_mode == AiMode.Cli)
            return await AskViaCli(userPrompt, systemPrompt);
        return Unavailable();
    }

    // ── Mode 1: Anthropic SDK (API Key) ──

    private async Task<AiAnalysisResult> AskViaSdk(string userPrompt, string? systemPrompt = null)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var messages = new List<Message>
            {
                new Message(RoleType.User, userPrompt)
            };

            var parameters = new MessageParameters
            {
                Messages = messages,
                MaxTokens = _maxTokens,
                Model = _model,
                Stream = false,
                Temperature = 0.3m,
                System = new List<SystemMessage> { new SystemMessage(systemPrompt ?? SystemPrompt) }
            };

            var result = await _client!.Messages.GetClaudeMessageAsync(parameters);
            sw.Stop();

            return new AiAnalysisResult
            {
                Success = true,
                Response = result.Message?.ToString() ?? "",
                Model = _model,
                Mode = "SDK",
                InputTokens = result.Usage?.InputTokens ?? 0,
                OutputTokens = result.Usage?.OutputTokens ?? 0,
                DurationMs = sw.ElapsedMilliseconds
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "Claude SDK call failed");
            return new AiAnalysisResult { Success = false, Error = ex.Message, DurationMs = sw.ElapsedMilliseconds };
        }
    }

    // ── Mode 2: OAuth token → direct Anthropic API ──

    private async Task<AiAnalysisResult> AskViaOAuth(string userPrompt, string? systemPrompt = null)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            // Refresh token if expired (with 60s margin)
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (now >= _oauthExpiresAt - 60_000)
            {
                _logger.LogInformation("OAuth token expired, refreshing...");
                if (!await RefreshOAuthTokenAsync())
                    throw new Exception("OAuth token expired and refresh failed");
            }

            var payload = new
            {
                model = _model,
                max_tokens = _maxTokens,
                temperature = 0.3,
                system = systemPrompt ?? SystemPrompt,
                messages = new[] { new { role = "user", content = userPrompt } }
            };

            var json = JsonSerializer.Serialize(payload);
            using var req = new HttpRequestMessage(HttpMethod.Post, AnthropicApiUrl)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
            req.Headers.Add("anthropic-version", "2023-06-01");
            req.Headers.Add("anthropic-beta", "oauth-2025-04-20");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _oauthAccessToken);

            var resp = await _http.SendAsync(req);
            var body = await resp.Content.ReadAsStringAsync();

            if (resp.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                // Token invalid — try refresh once
                if (await RefreshOAuthTokenAsync())
                {
                    req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _oauthAccessToken);
                    using var req2 = new HttpRequestMessage(HttpMethod.Post, AnthropicApiUrl)
                    {
                        Content = new StringContent(json, Encoding.UTF8, "application/json")
                    };
                    req2.Headers.Add("anthropic-version", "2023-06-01");
                    req2.Headers.Add("anthropic-beta", "oauth-2025-04-20");
                    req2.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _oauthAccessToken);
                    resp = await _http.SendAsync(req2);
                    body = await resp.Content.ReadAsStringAsync();
                }
            }

            if (!resp.IsSuccessStatusCode)
                throw new Exception($"Anthropic API {resp.StatusCode}: {body}");

            // Parse response
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            var text = "";
            if (root.TryGetProperty("content", out var content) && content.GetArrayLength() > 0)
                text = content[0].GetProperty("text").GetString() ?? "";

            int inTok = 0, outTok = 0;
            if (root.TryGetProperty("usage", out var usage))
            {
                inTok = usage.TryGetProperty("input_tokens", out var it) ? it.GetInt32() : 0;
                outTok = usage.TryGetProperty("output_tokens", out var ot) ? ot.GetInt32() : 0;
            }

            sw.Stop();
            return new AiAnalysisResult
            {
                Success = true,
                Response = text,
                Model = _model,
                Mode = "OAuth (Max)",
                InputTokens = inTok,
                OutputTokens = outTok,
                DurationMs = sw.ElapsedMilliseconds
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "Claude OAuth API call failed");
            return new AiAnalysisResult { Success = false, Error = ex.Message, DurationMs = sw.ElapsedMilliseconds };
        }
    }

    // ── Mode 3: Claude CLI (OAuth / Max subscription) ──

    private async Task<AiAnalysisResult> AskViaCli(string userPrompt, string? systemPrompt = null)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var fullPrompt = $"[System]\n{systemPrompt ?? SystemPrompt}\n\n[User]\n{userPrompt}";

            var psi = new ProcessStartInfo
            {
                FileName = _cliPath,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = false,
                CreateNoWindow = true
            };

            psi.ArgumentList.Add("--print");
            psi.ArgumentList.Add(fullPrompt);
            psi.ArgumentList.Add("--model");
            psi.ArgumentList.Add(_model);
            psi.ArgumentList.Add("--max-turns");
            psi.ArgumentList.Add("1");

            using var proc = Process.Start(psi);
            if (proc == null) throw new Exception("Failed to start claude CLI");

            var outputTask = proc.StandardOutput.ReadToEndAsync();
            var errorTask = proc.StandardError.ReadToEndAsync();

            var exited = proc.WaitForExit(120_000);
            if (!exited) { proc.Kill(); throw new TimeoutException("Claude CLI timed out after 120s"); }

            var output = await outputTask;
            var stderr = await errorTask;
            sw.Stop();

            if (proc.ExitCode != 0)
                throw new Exception($"CLI exit code {proc.ExitCode}: {stderr.Trim()}");

            return new AiAnalysisResult
            {
                Success = true,
                Response = output.Trim(),
                Model = _model,
                Mode = "CLI (OAuth)",
                DurationMs = sw.ElapsedMilliseconds
            };
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "Claude CLI call failed");
            return new AiAnalysisResult { Success = false, Error = ex.Message, DurationMs = sw.ElapsedMilliseconds };
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────

    private static AiAnalysisResult Unavailable() => new()
    {
        Success = false,
        Error = "Claude AI לא זמין. אפשרויות: (1) הגדר Claude:ApiKey (2) הגדר Claude:OAuthAccessToken (3) התקן Claude CLI מקומית"
    };

    private static AiAnalysisResult Error(Exception ex) => new()
    {
        Success = false,
        Error = "שגיאת AI: " + ex.Message
    };

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
}

// ─── Response model ──────────────────────────────────────────────

public class AiAnalysisResult
{
    public bool Success { get; set; }
    public string Response { get; set; } = "";
    public string? Error { get; set; }
    public string? Model { get; set; }
    public string? Mode { get; set; }  // "SDK", "OAuth (Max)", or "CLI (OAuth)"
    public int InputTokens { get; set; }
    public int OutputTokens { get; set; }
    public long DurationMs { get; set; }
}
