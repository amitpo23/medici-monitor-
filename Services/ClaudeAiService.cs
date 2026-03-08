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
        IConfiguration config)
    {
        _logger = logger;
        _data = data;
        _azure = azure;
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

    // ─── 5. Free-form Chat ───────────────────────────────────────

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
    {
        if (_mode == AiMode.Sdk)
            return await AskViaSdk(userPrompt);
        if (_mode == AiMode.OAuth)
            return await AskViaOAuth(userPrompt);
        if (_mode == AiMode.Cli)
            return await AskViaCli(userPrompt);
        return Unavailable();
    }

    // ── Mode 1: Anthropic SDK (API Key) ──

    private async Task<AiAnalysisResult> AskViaSdk(string userPrompt)
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
                System = new List<SystemMessage> { new SystemMessage(SystemPrompt) }
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

    private async Task<AiAnalysisResult> AskViaOAuth(string userPrompt)
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
                system = SystemPrompt,
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

    private async Task<AiAnalysisResult> AskViaCli(string userPrompt)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var fullPrompt = $"[System]\n{SystemPrompt}\n\n[User]\n{userPrompt}";

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
