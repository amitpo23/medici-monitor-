using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MediciMonitor.Services;

/// <summary>
/// Monitors Azure WebJobs across all configured App Services via Kudu REST API.
/// Uses Azure CLI managed identity for authentication (az rest).
/// Provides real-time status, history, and log access for triggered & continuous WebJobs.
/// </summary>
public class WebJobsMonitoringService
{
    private readonly ILogger<WebJobsMonitoringService> _logger;
    private readonly object _lock = new();

    // Cached state
    private WebJobsDashboard _lastDashboard = new();
    private DateTime _lastRefresh = DateTime.MinValue;
    private readonly List<WebJobEvent> _events = new();
    private const int MaxEvents = 1000;

    // App Services to monitor — add more as needed
    private readonly List<WebJobsAppTarget> _targets = new()
    {
        new() { AppName = "medici-backend", DisplayName = "Production Backend", ScmHost = "medici-backend.scm.azurewebsites.net", SubscriptionId = "2da025cc-dfe5-450f-a18f-10549a3907e3", ResourceGroup = "Medici-RG" }
    };

    public WebJobsMonitoringService(ILogger<WebJobsMonitoringService> logger) => _logger = logger;

    // ── Public: Full Dashboard ──────────────────────────────────

    public async Task<WebJobsDashboard> GetDashboardAsync(bool forceRefresh = false)
    {
        // Cache for 30 seconds to avoid hammering Kudu
        if (!forceRefresh && (DateTime.UtcNow - _lastRefresh).TotalSeconds < 30 && _lastDashboard.Apps.Any())
            return _lastDashboard;

        var dashboard = new WebJobsDashboard { Timestamp = DateTime.UtcNow };

        foreach (var target in _targets)
        {
            var appResult = await FetchAppWebJobs(target);
            dashboard.Apps.Add(appResult);
        }

        // Compute summary
        var allJobs = dashboard.Apps.SelectMany(a => a.Jobs).ToList();
        dashboard.Summary = new WebJobsSummary
        {
            TotalJobs = allJobs.Count,
            RunningJobs = allJobs.Count(j => j.Status.Equals("Running", StringComparison.OrdinalIgnoreCase)),
            StoppedJobs = allJobs.Count(j => j.Status.Equals("Stopped", StringComparison.OrdinalIgnoreCase)),
            ErrorJobs = allJobs.Count(j => j.Status.Contains("Error", StringComparison.OrdinalIgnoreCase) || j.Status.Equals("Failed", StringComparison.OrdinalIgnoreCase)),
            ContinuousJobs = allJobs.Count(j => j.Type == "continuous"),
            TriggeredJobs = allJobs.Count(j => j.Type == "triggered"),
            TotalApps = dashboard.Apps.Count,
            HealthyApps = dashboard.Apps.Count(a => !a.HasError)
        };

        // Detect changes and record events
        DetectChanges(dashboard);

        lock (_lock)
        {
            _lastDashboard = dashboard;
            _lastRefresh = DateTime.UtcNow;
        }

        return dashboard;
    }

    // ── Public: Single Job Detail ────────────────────────────────

    public async Task<WebJobDetail?> GetJobDetailAsync(string appName, string jobName, string jobType)
    {
        var target = _targets.FirstOrDefault(t => t.AppName.Equals(appName, StringComparison.OrdinalIgnoreCase));
        if (target == null) return null;

        var detail = new WebJobDetail { AppName = appName, JobName = jobName, JobType = jobType };

        // Fetch job info via management API
        var endpoint = jobType == "triggered"
            ? $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/triggeredwebjobs/{jobName}?api-version=2023-12-01"
            : $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/continuouswebjobs/{jobName}?api-version=2023-12-01";

        var (ok, json) = await AzRest("get", endpoint);
        if (ok && !string.IsNullOrEmpty(json))
        {
            try
            {
                var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty("properties", out var props))
                {
                    detail.Status = GetStr(props, "status") ?? "Unknown";
                    detail.ExtraInfo = GetStr(props, "detailed_status") ?? "";
                    detail.Url = GetStr(props, "url") ?? "";
                }
            }
            catch { /* parse error — ignore */ }
        }

        // Fetch history for triggered jobs
        if (jobType == "triggered")
        {
            var histEndpoint = $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/triggeredwebjobs/{jobName}/history?api-version=2023-12-01";
            var (ok2, json2) = await AzRest("get", histEndpoint);
            if (ok2 && !string.IsNullOrEmpty(json2))
            {
                try
                {
                    var doc = JsonDocument.Parse(json2);
                    if (doc.RootElement.TryGetProperty("value", out var runs))
                    {
                        detail.History = runs.EnumerateArray().Take(20).Select(r =>
                        {
                            var p = r.TryGetProperty("properties", out var props2) ? props2 : r;
                            return new WebJobRunHistory
                            {
                                Id = GetStr(p, "id") ?? "",
                                Status = GetStr(p, "status") ?? "Unknown",
                                StartTime = p.TryGetProperty("start_time", out var st) && st.ValueKind != JsonValueKind.Null ? st.GetDateTime() : null,
                                EndTime = p.TryGetProperty("end_time", out var et) && et.ValueKind != JsonValueKind.Null ? et.GetDateTime() : null,
                                Duration = GetStr(p, "duration") ?? "",
                                OutputUrl = GetStr(p, "output_url") ?? "",
                                ErrorUrl = GetStr(p, "error_url") ?? "",
                                Trigger = GetStr(p, "trigger") ?? ""
                            };
                        }).ToList();
                    }
                }
                catch { /* parse error */ }
            }
        }

        detail.RecentLog = await FetchJobLog(target, jobName, jobType);

        return detail;
    }

    // ── Public: Trigger a WebJob ─────────────────────────────────

    public async Task<WebJobActionResult> TriggerJobAsync(string appName, string jobName)
    {
        var target = _targets.FirstOrDefault(t => t.AppName.Equals(appName, StringComparison.OrdinalIgnoreCase));
        if (target == null) return new() { Success = false, Message = $"App '{appName}' not found in targets" };

        var endpoint = $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/triggeredwebjobs/{jobName}/run?api-version=2023-12-01";
        var (ok, response) = await AzRest("post", endpoint);

        RecordEvent(appName, jobName, ok ? "Triggered" : "TriggerFailed", ok ? "Manual trigger" : response);
        return new() { Success = ok, Message = ok ? $"WebJob '{jobName}' triggered successfully" : $"Failed to trigger: {response}" };
    }

    // ── Public: Start/Stop Continuous WebJob ────────────────────

    public async Task<WebJobActionResult> SetJobStateAsync(string appName, string jobName, string action)
    {
        var target = _targets.FirstOrDefault(t => t.AppName.Equals(appName, StringComparison.OrdinalIgnoreCase));
        if (target == null) return new() { Success = false, Message = $"App '{appName}' not found in targets" };

        if (action != "start" && action != "stop")
            return new() { Success = false, Message = "Action must be 'start' or 'stop'" };

        var endpoint = $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/continuouswebjobs/{jobName}/{action}?api-version=2023-12-01";
        var (ok, response) = await AzRest("post", endpoint);

        RecordEvent(appName, jobName, action == "start" ? "Started" : "Stopped", ok ? $"Manual {action}" : response);
        return new() { Success = ok, Message = ok ? $"WebJob '{jobName}' {action}ed successfully" : $"Failed: {response}" };
    }

    // ── Public: Events Timeline ──────────────────────────────────

    public List<WebJobEvent> GetEvents(int last = 50)
    {
        lock (_lock)
        {
            return _events.OrderByDescending(e => e.Timestamp).Take(last).ToList();
        }
    }

    // ── Public: Get Targets ──────────────────────────────────────

    public List<WebJobsAppTarget> GetTargets() => _targets.ToList();

    public void AddTarget(string appName, string displayName)
    {
        if (_targets.Any(t => t.AppName.Equals(appName, StringComparison.OrdinalIgnoreCase))) return;
        _targets.Add(new WebJobsAppTarget
        {
            AppName = appName,
            DisplayName = displayName,
            ScmHost = $"{appName}.scm.azurewebsites.net",
            SubscriptionId = "2da025cc-dfe5-450f-a18f-10549a3907e3",
            ResourceGroup = "Medici-RG"
        });
    }

    // ── Private: Fetch all WebJobs for an App via Management API ─

    private async Task<WebJobsAppStatus> FetchAppWebJobs(WebJobsAppTarget target)
    {
        var app = new WebJobsAppStatus { AppName = target.AppName, DisplayName = target.DisplayName };

        // Use Azure Management API to list webjobs:
        // GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Web/sites/{site}/webjobs?api-version=2023-12-01
        var mgmtUrl = $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/webjobs?api-version=2023-12-01";

        var (ok, json) = await AzRest("get", mgmtUrl);
        if (!ok)
        {
            // Fallback: try triggered + continuous separately
            var (ok2, json2) = await AzRest("get", $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/triggeredwebjobs?api-version=2023-12-01");
            var (ok3, json3) = await AzRest("get", $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/continuouswebjobs?api-version=2023-12-01");

            if (!ok2 && !ok3)
            {
                app.HasError = true;
                app.ErrorMessage = "Failed to connect to Azure Management API — ensure az CLI is logged in";
                _logger.LogWarning("WebJobs: Failed to fetch from {App} via management API", target.AppName);
                return app;
            }

            if (ok2) ParseMgmtWebJobs(json2, "triggered", app);
            if (ok3) ParseMgmtWebJobs(json3, "continuous", app);
            return app;
        }

        try
        {
            var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("value", out var valueArr))
            {
                foreach (var item in valueArr.EnumerateArray())
                {
                    var props = item.GetProperty("properties");
                    var jobType = GetStr(props, "web_job_type") ?? GetStr(props, "webJobType") ?? GetStr(props, "type") ?? "unknown";
                    // Normalize: ARM API may return "continuous" in URL
                    if (jobType == "unknown")
                    {
                        var jobUrl = GetStr(props, "url") ?? "";
                        if (jobUrl.Contains("/continuouswebjobs/", StringComparison.OrdinalIgnoreCase)) jobType = "continuous";
                        else if (jobUrl.Contains("/triggeredwebjobs/", StringComparison.OrdinalIgnoreCase)) jobType = "triggered";
                    }
                    app.Jobs.Add(new WebJobInfo
                    {
                        Name = GetStr(props, "name") ?? GetStr(item, "name") ?? "?",
                        Type = jobType,
                        Status = GetStr(props, "status") ?? "Unknown",
                        DetailedStatus = GetStr(props, "detailed_status") ?? "",
                        Url = GetStr(props, "url") ?? "",
                        ExtraInfoUrl = GetStr(props, "extra_info_url") ?? "",
                        HistoryUrl = GetStr(props, "history_url") ?? "",
                        RunCommand = GetStr(props, "run_command") ?? "",
                        UsingSdk = props.TryGetProperty("using_sdk", out var sdk) && sdk.GetBoolean(),
                        LatestRun = ParseLatestRun(props)
                    });
                }
            }
        }
        catch (Exception ex)
        {
            app.HasError = true;
            app.ErrorMessage = $"Parse error: {ex.Message}";
            _logger.LogWarning("WebJobs: Parse error for {App}: {Err}", target.AppName, ex.Message);
        }

        return app;
    }

    private void ParseMgmtWebJobs(string json, string type, WebJobsAppStatus app)
    {
        try
        {
            var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("value", out var valueArr))
            {
                foreach (var item in valueArr.EnumerateArray())
                {
                    var props = item.GetProperty("properties");
                    app.Jobs.Add(new WebJobInfo
                    {
                        Name = GetStr(props, "name") ?? GetStr(item, "name") ?? "?",
                        Type = type,
                        Status = GetStr(props, "status") ?? "Unknown",
                        DetailedStatus = GetStr(props, "detailed_status") ?? "",
                        Url = GetStr(props, "url") ?? "",
                        ExtraInfoUrl = GetStr(props, "extra_info_url") ?? "",
                        HistoryUrl = GetStr(props, "history_url") ?? "",
                        RunCommand = GetStr(props, "run_command") ?? "",
                        UsingSdk = props.TryGetProperty("using_sdk", out var sdk) && sdk.GetBoolean(),
                        LatestRun = ParseLatestRun(props)
                    });
                }
            }
        }
        catch (Exception ex) { _logger.LogWarning("WebJobs: Parse error ({Type}): {Err}", type, ex.Message); }
    }

    private static WebJobRunHistory? ParseLatestRun(JsonElement props)
    {
        if (!props.TryGetProperty("latest_run", out var lr) || lr.ValueKind == JsonValueKind.Null) return null;
        return new WebJobRunHistory
        {
            Id = GetStr(lr, "id") ?? "",
            Status = GetStr(lr, "status") ?? "Unknown",
            StartTime = lr.TryGetProperty("start_time", out var st) && st.ValueKind != JsonValueKind.Null ? st.GetDateTime() : null,
            EndTime = lr.TryGetProperty("end_time", out var et) && et.ValueKind != JsonValueKind.Null ? et.GetDateTime() : null,
            Duration = GetStr(lr, "duration") ?? "",
            OutputUrl = GetStr(lr, "output_url") ?? "",
            ErrorUrl = GetStr(lr, "error_url") ?? ""
        };
    }

    private static string? GetStr(JsonElement el, string prop)
    {
        if (el.TryGetProperty(prop, out var val) && val.ValueKind == JsonValueKind.String) return val.GetString();
        return null;
    }

    // ── Private: Fetch Job Log via Management API ──────────────

    private async Task<string> FetchJobLog(WebJobsAppTarget target, string jobName, string jobType)
    {
        try
        {
            if (jobType == "triggered")
            {
                // Get triggered job history via management API
                var histUrl = $"https://management.azure.com/subscriptions/{target.SubscriptionId}/resourceGroups/{target.ResourceGroup}/providers/Microsoft.Web/sites/{target.AppName}/triggeredwebjobs/{jobName}/history?api-version=2023-12-01";
                var (ok, json) = await AzRest("get", histUrl);
                if (ok && !string.IsNullOrEmpty(json))
                {
                    try
                    {
                        var doc = JsonDocument.Parse(json);
                        if (doc.RootElement.TryGetProperty("value", out var runs) && runs.GetArrayLength() > 0)
                        {
                            var firstRun = runs[0];
                            if (firstRun.TryGetProperty("properties", out var props))
                            {
                                var outputUrl = GetStr(props, "output_url");
                                if (!string.IsNullOrEmpty(outputUrl))
                                {
                                    var (ok2, log) = await AzRest("get", outputUrl);
                                    if (ok2) return log.Length > 5000 ? log[^5000..] : log;
                                }
                            }
                        }
                    }
                    catch { /* parse error */ }
                }
            }
        }
        catch (Exception ex) { _logger.LogWarning("WebJobs: Log fetch error for {Job}: {Err}", jobName, ex.Message); }
        return "";
    }

    // ── Private: Azure REST API calls via Managed Identity ─────

    private async Task<(bool success, string output)> AzRest(string method, string url)
    {
        try
        {
            // Try managed identity first (runs on Azure App Service)
            var token = await GetManagedIdentityToken();
            if (!string.IsNullOrEmpty(token))
            {
                return await CallArmApi(method, url, token);
            }

            // Fallback: try az CLI (for local dev)
            var args = $"rest --method {method} --url \"{url}\"";
            return await RunAzCli(args);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("AzRest error for {Url}: {Err}", url, ex.Message);
            return (false, "");
        }
    }

    private string? _cachedToken;
    private DateTime _tokenExpiry = DateTime.MinValue;

    private async Task<string?> GetManagedIdentityToken()
    {
        // Return cached token if still valid (5 min buffer)
        if (_cachedToken != null && DateTime.UtcNow < _tokenExpiry.AddMinutes(-5))
            return _cachedToken;

        try
        {
            // Azure App Service provides IDENTITY_ENDPOINT and IDENTITY_HEADER
            var identityEndpoint = Environment.GetEnvironmentVariable("IDENTITY_ENDPOINT");
            var identityHeader = Environment.GetEnvironmentVariable("IDENTITY_HEADER");

            if (!string.IsNullOrEmpty(identityEndpoint) && !string.IsNullOrEmpty(identityHeader))
            {
                // App Service Managed Identity
                using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
                var tokenUrl = $"{identityEndpoint}?resource=https://management.azure.com/&api-version=2019-08-01";
                var request = new HttpRequestMessage(HttpMethod.Get, tokenUrl);
                request.Headers.Add("X-IDENTITY-HEADER", identityHeader);
                var response = await http.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    var doc = JsonDocument.Parse(json);
                    _cachedToken = doc.RootElement.GetProperty("access_token").GetString();
                    if (doc.RootElement.TryGetProperty("expires_on", out var expiresOn))
                    {
                        if (long.TryParse(expiresOn.GetString(), out var epoch))
                            _tokenExpiry = DateTimeOffset.FromUnixTimeSeconds(epoch).UtcDateTime;
                        else
                            _tokenExpiry = DateTime.UtcNow.AddHours(1);
                    }
                    return _cachedToken;
                }
            }

            // Fallback: Azure VM IMDS endpoint
            using var http2 = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var imdsUrl = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/";
            var req2 = new HttpRequestMessage(HttpMethod.Get, imdsUrl);
            req2.Headers.Add("Metadata", "true");
            var resp2 = await http2.SendAsync(req2);

            if (resp2.IsSuccessStatusCode)
            {
                var json = await resp2.Content.ReadAsStringAsync();
                var doc = JsonDocument.Parse(json);
                _cachedToken = doc.RootElement.GetProperty("access_token").GetString();
                _tokenExpiry = DateTime.UtcNow.AddHours(1);
                return _cachedToken;
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Managed identity token fetch failed (expected in local dev): {Err}", ex.Message);
        }

        return null;
    }

    private async Task<(bool success, string output)> CallArmApi(string method, string url, string token)
    {
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            var request = new HttpRequestMessage(
                method.Equals("post", StringComparison.OrdinalIgnoreCase) ? HttpMethod.Post : HttpMethod.Get,
                url);
            request.Headers.Add("Authorization", $"Bearer {token}");
            var response = await http.SendAsync(request);

            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                return (true, content);
            }

            var errBody = await response.Content.ReadAsStringAsync();
            _logger.LogWarning("ARM API {Method} {Url} returned {Code}: {Body}", method, url, (int)response.StatusCode, errBody.Length > 500 ? errBody[..500] : errBody);
            return (false, $"HTTP {(int)response.StatusCode}");
        }
        catch (Exception ex)
        {
            _logger.LogWarning("CallArmApi error: {Err}", ex.Message);
            return (false, ex.Message);
        }
    }

    private async Task<(bool success, string output)> RunAzCli(string args)
    {
        try
        {
            // Try Windows path first (Azure App Service), then fall back to 'az' on PATH
            var azPaths = new[] { @"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd", "az" };
            foreach (var azPath in azPaths)
            {
                try
                {
                    var psi = new ProcessStartInfo
                    {
                        FileName = azPath,
                        Arguments = args,
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        CreateNoWindow = true
                    };
                    using var proc = Process.Start(psi);
                    if (proc != null)
                    {
                        var output = await proc.StandardOutput.ReadToEndAsync();
                        await proc.WaitForExitAsync();
                        if (proc.ExitCode == 0) return (true, output);
                    }
                }
                catch { /* try next path */ }
            }
        }
        catch (Exception ex) { _logger.LogWarning("az cli error: {Err}", ex.Message); }
        return (false, "");
    }

    // ── Private: Change Detection ────────────────────────────────

    private void DetectChanges(WebJobsDashboard newDashboard)
    {
        lock (_lock)
        {
            if (!_lastDashboard.Apps.Any()) return;

            var oldJobs = _lastDashboard.Apps.SelectMany(a => a.Jobs.Select(j => (a.AppName, j))).ToDictionary(x => $"{x.AppName}/{x.j.Name}");
            var newJobs = newDashboard.Apps.SelectMany(a => a.Jobs.Select(j => (a.AppName, j))).ToDictionary(x => $"{x.AppName}/{x.j.Name}");

            foreach (var (key, (app, job)) in newJobs)
            {
                if (oldJobs.TryGetValue(key, out var old))
                {
                    if (old.j.Status != job.Status)
                    {
                        RecordEvent(app, job.Name, "StatusChanged", $"{old.j.Status} → {job.Status}");
                    }
                }
                else
                {
                    RecordEvent(app, job.Name, "Discovered", $"New job detected: {job.Type}");
                }
            }

            foreach (var (key, (app, job)) in oldJobs)
            {
                if (!newJobs.ContainsKey(key))
                {
                    RecordEvent(app, job.Name, "Removed", "Job no longer present");
                }
            }
        }
    }

    private void RecordEvent(string appName, string jobName, string eventType, string detail)
    {
        lock (_lock)
        {
            _events.Add(new WebJobEvent
            {
                Timestamp = DateTime.UtcNow,
                AppName = appName,
                JobName = jobName,
                EventType = eventType,
                Detail = detail
            });
            while (_events.Count > MaxEvents) _events.RemoveAt(0);
        }
    }}

// ═══════════════════════════════════════════════════════════════
//  WebJobs Models
// ═══════════════════════════════════════════════════════════════

public class WebJobsDashboard
{
    public DateTime Timestamp { get; set; }
    public WebJobsSummary Summary { get; set; } = new();
    public List<WebJobsAppStatus> Apps { get; set; } = new();
}

public class WebJobsSummary
{
    public int TotalJobs { get; set; }
    public int RunningJobs { get; set; }
    public int StoppedJobs { get; set; }
    public int ErrorJobs { get; set; }
    public int ContinuousJobs { get; set; }
    public int TriggeredJobs { get; set; }
    public int TotalApps { get; set; }
    public int HealthyApps { get; set; }
}

public class WebJobsAppTarget
{
    public string AppName { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string ScmHost { get; set; } = "";
    public string SubscriptionId { get; set; } = "";
    public string ResourceGroup { get; set; } = "";
}

public class WebJobsAppStatus
{
    public string AppName { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public bool HasError { get; set; }
    public string? ErrorMessage { get; set; }
    public List<WebJobInfo> Jobs { get; set; } = new();
}

public class WebJobInfo
{
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public string Status { get; set; } = "";
    public string DetailedStatus { get; set; } = "";
    public string Url { get; set; } = "";
    public string ExtraInfoUrl { get; set; } = "";
    public string HistoryUrl { get; set; } = "";
    public string RunCommand { get; set; } = "";
    public bool UsingSdk { get; set; }
    public WebJobRunHistory? LatestRun { get; set; }
}

public class WebJobRunHistory
{
    public string Id { get; set; } = "";
    public string Status { get; set; } = "";
    public DateTime? StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string Duration { get; set; } = "";
    public string OutputUrl { get; set; } = "";
    public string ErrorUrl { get; set; } = "";
    public string Trigger { get; set; } = "";
}

public class WebJobDetail
{
    public string AppName { get; set; } = "";
    public string JobName { get; set; } = "";
    public string JobType { get; set; } = "";
    public string Status { get; set; } = "";
    public string ExtraInfo { get; set; } = "";
    public string Url { get; set; } = "";
    public string RecentLog { get; set; } = "";
    public List<WebJobRunHistory> History { get; set; } = new();
}

public class WebJobActionResult
{
    public bool Success { get; set; }
    public string Message { get; set; } = "";
}

public class WebJobEvent
{
    public DateTime Timestamp { get; set; }
    public string AppName { get; set; } = "";
    public string JobName { get; set; } = "";
    public string EventType { get; set; } = "";
    public string Detail { get; set; } = "";
}
