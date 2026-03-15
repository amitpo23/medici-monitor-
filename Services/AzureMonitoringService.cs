using RestSharp;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Enhanced: API health checking + Azure resource status via CLI + SSL certificate monitoring
/// + DNS health + endpoint-level response time tracking.
/// </summary>
public class AzureMonitoringService
{
    private readonly RestClient _restClient = new();
    private readonly ILogger<AzureMonitoringService> _logger;

    private readonly Dictionary<string, List<EndpointMetric>> _endpointMetrics = new();
    private readonly object _metricsLock = new();
    private const int MaxMetricsPerEndpoint = 1440;

    // ── Health check cache (shared across SLA, Alerts, Historical) ──
    private List<ApiHealthStatus>? _cachedHealthResult;
    private DateTime _healthCacheTime = DateTime.MinValue;
    private static readonly TimeSpan HealthCacheTtl = TimeSpan.FromSeconds(60);
    private readonly SemaphoreSlim _healthCacheLock = new(1, 1);

    public AzureMonitoringService(ILogger<AzureMonitoringService> logger) => _logger = logger;

    // ── Health Check ──────────────────────────────────────────────

    public async Task<List<ApiHealthStatus>> ComprehensiveApiHealthCheck()
    {
        // Return cached result if fresh enough (prevents duplicate calls from SLA, Alerts, Historical)
        if (_cachedHealthResult != null && DateTime.UtcNow - _healthCacheTime < HealthCacheTtl)
            return _cachedHealthResult;

        await _healthCacheLock.WaitAsync();
        try
        {
            // Double-check after acquiring lock
            if (_cachedHealthResult != null && DateTime.UtcNow - _healthCacheTime < HealthCacheTtl)
                return _cachedHealthResult;

            var endpoints = new (string url, string name, bool isInternal, HashSet<HttpStatusCode> ok)[]
            {
                ("https://medici-backend.azurewebsites.net/healthcheck", "Production Backend Health", false, new() { HttpStatusCode.OK }),
                ("https://medici-backend.azurewebsites.net/ZenithApi/HelloZenith", "Zenith API", false, new() { HttpStatusCode.OK }),
                ("https://medici-backend-dev-f9h6hxgncha9fbbp.eastus2-01.azurewebsites.net/", "Dev Backend Reachability", false, new() { HttpStatusCode.OK, HttpStatusCode.Moved, HttpStatusCode.Redirect }),
                ("https://medici-backend.azurewebsites.net/swagger", "API Documentation", false, new() { HttpStatusCode.OK, HttpStatusCode.Unauthorized, HttpStatusCode.Forbidden, HttpStatusCode.Moved, HttpStatusCode.Redirect }),
                ("https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration", "Azure AD Connectivity", true, new() { HttpStatusCode.OK })
            };

            var tasks = endpoints.Select(e => CheckEndpoint(e.url, e.name, e.isInternal, e.ok)).ToList();
            tasks.Add(CheckSqlTcpConnectivity("medici-sql-server.database.windows.net", 1433, "SQL Server TCP"));
            var results = (await Task.WhenAll(tasks)).ToList();

            lock (_metricsLock)
            {
                foreach (var r in results)
                {
                    var name = r.Endpoint.Split('(')[0].Trim();
                    if (!_endpointMetrics.ContainsKey(name))
                        _endpointMetrics[name] = new List<EndpointMetric>();
                    _endpointMetrics[name].Add(new EndpointMetric { Timestamp = DateTime.UtcNow, ResponseTimeMs = r.ResponseTimeMs, IsHealthy = r.IsHealthy, StatusCode = r.StatusCode });
                    while (_endpointMetrics[name].Count > MaxMetricsPerEndpoint) _endpointMetrics[name].RemoveAt(0);
                }
            }

            _cachedHealthResult = results;
            _healthCacheTime = DateTime.UtcNow;
            return results;
        }
        finally
        {
            _healthCacheLock.Release();
        }
    }

    private async Task<ApiHealthStatus> CheckEndpoint(string url, string name, bool isInternal, HashSet<HttpStatusCode> expected)
    {
        var s = new ApiHealthStatus { Endpoint = $"{name} ({url})", LastChecked = DateTime.UtcNow };
        try
        {
            var sw = Stopwatch.StartNew();
            var req = new RestRequest(url, Method.Get) { Timeout = TimeSpan.FromSeconds(isInternal ? 5 : 15) };
            var resp = await _restClient.ExecuteAsync(req);
            sw.Stop();
            s.ResponseTimeMs = (int)sw.ElapsedMilliseconds;
            s.StatusCode = (int)resp.StatusCode;
            s.IsHealthy = resp.StatusCode != 0 && expected.Contains(resp.StatusCode);
            if (!s.IsHealthy) s.ErrorMessage = $"{resp.StatusCode}: {resp.ErrorMessage ?? resp.Content}";
        }
        catch (Exception ex) { s.IsHealthy = false; s.ErrorMessage = ex.Message; s.StatusCode = 0; s.ResponseTimeMs = -1; }
        return s;
    }

    private async Task<ApiHealthStatus> CheckSqlTcpConnectivity(string host, int port, string name)
    {
        var s = new ApiHealthStatus { Endpoint = $"{name} (tcp://{host}:{port})", LastChecked = DateTime.UtcNow };
        var sw = Stopwatch.StartNew();
        try
        {
            using var client = new TcpClient();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await client.ConnectAsync(host, port, cts.Token);
            sw.Stop();
            s.IsHealthy = true; s.StatusCode = 200; s.ResponseTimeMs = (int)sw.ElapsedMilliseconds;
        }
        catch (Exception ex) { sw.Stop(); s.IsHealthy = false; s.StatusCode = 0; s.ResponseTimeMs = (int)sw.ElapsedMilliseconds; s.ErrorMessage = ex.Message; }
        return s;
    }

    // ── SSL Certificate Monitoring (cached — certs change rarely) ──

    private List<SslCertInfo>? _cachedSslResult;
    private DateTime _sslCacheTime = DateTime.MinValue;
    private static readonly TimeSpan SslCacheTtl = TimeSpan.FromMinutes(10);

    public async Task<List<SslCertInfo>> CheckSslCertificates()
    {
        if (_cachedSslResult != null && DateTime.UtcNow - _sslCacheTime < SslCacheTtl)
            return _cachedSslResult;

        var hosts = new[] { "medici-backend.azurewebsites.net", "medici-backend-dev-f9h6hxgncha9fbbp.eastus2-01.azurewebsites.net", "medici-monitor-dashboard.azurewebsites.net", "medici-sql-server.database.windows.net" };
        var tasks = hosts.Select(CheckSslCert);
        _cachedSslResult = (await Task.WhenAll(tasks)).ToList();
        _sslCacheTime = DateTime.UtcNow;
        return _cachedSslResult;
    }

    private async Task<SslCertInfo> CheckSslCert(string host)
    {
        var info = new SslCertInfo { Host = host, CheckedAt = DateTime.UtcNow };
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(host, 443);
            using var sslStream = new SslStream(client.GetStream(), false, (sender, cert, chain, errors) =>
            {
                if (cert is X509Certificate2 c2)
                {
                    info.Subject = c2.Subject; info.Issuer = c2.Issuer;
                    info.ValidFrom = c2.NotBefore; info.ValidTo = c2.NotAfter;
                    info.DaysUntilExpiry = (int)(c2.NotAfter - DateTime.UtcNow).TotalDays;
                    info.Thumbprint = c2.Thumbprint; info.IsValid = errors == SslPolicyErrors.None;
                }
                return true;
            });
            await sslStream.AuthenticateAsClientAsync(host);
            info.IsAccessible = true;
        }
        catch (Exception ex) { info.IsAccessible = false; info.Error = ex.Message; }
        return info;
    }

    // ── DNS Health Check (cached — DNS changes rarely) ──────────

    private List<DnsCheckResult>? _cachedDnsResult;
    private DateTime _dnsCacheTime = DateTime.MinValue;
    private static readonly TimeSpan DnsCacheTtl = TimeSpan.FromMinutes(10);

    public async Task<List<DnsCheckResult>> CheckDnsHealth()
    {
        if (_cachedDnsResult != null && DateTime.UtcNow - _dnsCacheTime < DnsCacheTtl)
            return _cachedDnsResult;

        var hosts = new[] { "medici-backend.azurewebsites.net", "medici-sql-server.database.windows.net", "login.microsoftonline.com" };
        var tasks = hosts.Select(async h =>
        {
            var result = new DnsCheckResult { Host = h, CheckedAt = DateTime.UtcNow };
            var sw = Stopwatch.StartNew();
            try
            {
                var addresses = await Dns.GetHostAddressesAsync(h);
                sw.Stop();
                result.ResolvedAddresses = addresses.Select(a => a.ToString()).ToList();
                result.ResolutionTimeMs = (int)sw.ElapsedMilliseconds;
                result.IsResolved = addresses.Length > 0;
            }
            catch (Exception ex) { sw.Stop(); result.IsResolved = false; result.Error = ex.Message; result.ResolutionTimeMs = (int)sw.ElapsedMilliseconds; }
            return result;
        });
        _cachedDnsResult = (await Task.WhenAll(tasks)).ToList();
        _dnsCacheTime = DateTime.UtcNow;
        return _cachedDnsResult;
    }

    // ── Endpoint-Level Metrics ──────────────────────────────────

    public Dictionary<string, EndpointMetricsSummary> GetEndpointMetrics()
    {
        lock (_metricsLock)
        {
            var result = new Dictionary<string, EndpointMetricsSummary>();
            foreach (var (name, metrics) in _endpointMetrics)
            {
                if (!metrics.Any()) continue;
                var recent = metrics.Where(m => m.Timestamp >= DateTime.UtcNow.AddHours(-1)).ToList();
                var rts = recent.Where(m => m.ResponseTimeMs > 0).Select(m => m.ResponseTimeMs).OrderBy(t => t).ToList();
                result[name] = new EndpointMetricsSummary
                {
                    Endpoint = name, TotalChecks = metrics.Count, RecentChecks = recent.Count,
                    SuccessRate = recent.Any() ? Math.Round(recent.Count(m => m.IsHealthy) * 100.0 / recent.Count, 1) : 100,
                    AvgResponseMs = rts.Any() ? Math.Round(rts.Average(), 1) : 0,
                    P50ResponseMs = rts.Any() ? GetPercentile(rts, 50) : 0,
                    P95ResponseMs = rts.Any() ? GetPercentile(rts, 95) : 0,
                    P99ResponseMs = rts.Any() ? GetPercentile(rts, 99) : 0,
                    MinResponseMs = rts.Any() ? rts.Min() : 0,
                    MaxResponseMs = rts.Any() ? rts.Max() : 0,
                    LastChecked = metrics.Last().Timestamp, LastResponseMs = metrics.Last().ResponseTimeMs,
                    IsCurrentlyHealthy = metrics.Last().IsHealthy
                };
            }
            return result;
        }
    }

    // ── Azure Resources via CLI ──────────────────────────────────

    public async Task<List<AzureResourceStatus>> GetDetailedAzureStatus()
    {
        var resources = new List<AzureResourceStatus>();
        try
        {
            var (ok, json) = await RunAzCli("resource list --output json");
            if (ok)
            {
                var list = JsonSerializer.Deserialize<List<AzRes>>(json);
                if (list != null) resources.AddRange(list.Where(r => IsRelevant(r.name ?? "")).Select(r => new AzureResourceStatus { ResourceName = r.name ?? "?", ResourceType = ShortType(r.type ?? ""), Status = "Running", Location = r.location ?? "?", LastUpdated = DateTime.UtcNow }));
            }
            var (ok2, json2) = await RunAzCli("webapp list --query \"[?contains(name, 'medici')].{name:name, state:state}\" --output json");
            if (ok2)
            {
                var apps = JsonSerializer.Deserialize<List<AppInfo>>(json2);
                if (apps != null) foreach (var a in apps) { var ex = resources.FirstOrDefault(r => r.ResourceName == a.name); if (ex != null) ex.Status = a.state ?? "?"; }
            }
            var (ok3, json3) = await RunAzCli("sql db list --server medici-sql-server --resource-group Medici-RG --query \"[].{name:name, status:status, tier:currentServiceObjectiveName}\" --output json");
            if (ok3)
            {
                var dbs = JsonSerializer.Deserialize<List<DbInfo>>(json3);
                if (dbs != null) resources.AddRange(dbs.Select(d => new AzureResourceStatus { ResourceName = $"Database: {d.name}", ResourceType = $"SQL Database ({d.tier})", Status = d.status ?? "?", Location = "West Europe", LastUpdated = DateTime.UtcNow }));
            }
        }
        catch (Exception ex) { resources.Add(new AzureResourceStatus { ResourceName = "Azure CLI Error", ResourceType = "Error", Status = ex.Message, Location = "N/A", LastUpdated = DateTime.UtcNow }); }
        return resources;
    }

    public async Task<List<AzureAlert>> GetActiveAlerts()
    {
        var alerts = new List<AzureAlert>();
        try
        {
            var (ok, json) = await RunAzCli("monitor activity-log list --max-events 10 --output json");
            if (ok)
            {
                var activities = JsonSerializer.Deserialize<List<AzActivity>>(json);
                if (activities != null)
                    alerts.AddRange(activities.Where(a => IsImportant(a.eventName ?? "", a.level ?? "")).Select(a => new AzureAlert { Timestamp = DateTime.TryParse(a.eventTimestamp, out var dt) ? dt : DateTime.UtcNow, Level = a.level ?? "Info", Message = a.eventName ?? "Unknown", Source = a.caller ?? "Azure System", ResourceId = (a.resourceId ?? "").Split('/').LastOrDefault() ?? "" }));
            }
        }
        catch (Exception ex) { _logger.LogError("GetActiveAlerts error: {Err}", ex.Message); }
        return alerts;
    }

    public async Task<AzurePerformanceMetrics> GetPerformanceMetrics()
    {
        var m = new AzurePerformanceMetrics { Timestamp = DateTime.UtcNow };
        try { await RunAzCli($@"monitor metrics list --resource /subscriptions/2da025cc-dfe5-450f-a18f-10549a3907e3/resourceGroups/Medici-RG/providers/Microsoft.Web/sites/medici-backend --metric ""CpuTime,Requests,ResponseTime,MemoryWorkingSet"" --start-time {DateTime.UtcNow.AddHours(-1):yyyy-MM-ddTHH:mm:ssZ} --interval PT1M --output json"); }
        catch (Exception ex) { _logger.LogError("GetPerformanceMetrics error: {Err}", ex.Message); }
        return m;
    }

    // ── Azure Monitor Fired Alerts (via ARM REST API + Managed Identity) ──

    private string? _cachedToken;
    private DateTime _tokenExpiry = DateTime.MinValue;
    private readonly HttpClient _armHttp = new() { Timeout = TimeSpan.FromSeconds(30) };

    public async Task<List<AzureMonitorFiredAlert>> GetFiredAzureMonitorAlerts()
    {
        var alerts = new List<AzureMonitorFiredAlert>();
        try
        {
            var token = await GetManagedIdentityToken();
            if (string.IsNullOrEmpty(token))
            {
                _logger.LogWarning("Could not get managed identity token for Azure Monitor alerts");
                return alerts;
            }

            // Alert Management REST API — list all alerts in the subscription
            var url = "https://management.azure.com/subscriptions/2da025cc-dfe5-450f-a18f-10549a3907e3/providers/Microsoft.AlertsManagement/alerts?api-version=2019-05-05-preview&timeRange=1d";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Add("Authorization", $"Bearer {token}");
            var response = await _armHttp.SendAsync(request);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Azure Monitor alerts GET returned {Code}", (int)response.StatusCode);
                return alerts;
            }

            var json = await response.Content.ReadAsStringAsync();
            var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty("value", out var valueArr))
            {
                foreach (var item in valueArr.EnumerateArray())
                {
                    try
                    {
                        var props = item.GetProperty("properties");
                        var essentials = props.GetProperty("essentials");

                        var alert = new AzureMonitorFiredAlert
                        {
                            AlertId = GetStr(item, "id") ?? "",
                            Name = GetStr(item, "name") ?? "",
                            Severity = GetStr(essentials, "severity") ?? "",
                            MonitorCondition = GetStr(essentials, "monitorCondition") ?? "",
                            AlertState = GetStr(essentials, "alertState") ?? "",
                            TargetResource = GetStr(essentials, "targetResource") ?? "",
                            TargetResourceType = GetStr(essentials, "targetResourceType") ?? "",
                            TargetResourceGroup = GetStr(essentials, "targetResourceGroup") ?? "",
                            SignalType = GetStr(essentials, "signalType") ?? "",
                            MonitorService = GetStr(essentials, "monitorService") ?? "",
                            Description = GetStr(essentials, "description") ?? "",
                            AlertRule = GetStr(essentials, "alertRule") ?? "",
                        };

                        if (essentials.TryGetProperty("startDateTime", out var startDt) && startDt.ValueKind == JsonValueKind.String)
                            alert.FiredTime = DateTime.TryParse(startDt.GetString(), out var dt) ? dt : null;

                        if (essentials.TryGetProperty("lastModifiedDateTime", out var modDt) && modDt.ValueKind == JsonValueKind.String)
                            alert.LastModified = DateTime.TryParse(modDt.GetString(), out var dt2) ? dt2 : null;

                        // Extract short resource name
                        var targetRes = alert.TargetResource;
                        if (!string.IsNullOrEmpty(targetRes))
                            alert.TargetResourceName = targetRes.Split('/').LastOrDefault() ?? targetRes;

                        alerts.Add(alert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug("Error parsing alert entry: {Err}", ex.Message);
                    }
                }
            }

            // Sort: Fired first, then by time descending
            alerts = alerts
                .OrderByDescending(a => a.MonitorCondition == "Fired")
                .ThenByDescending(a => a.FiredTime)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError("GetFiredAzureMonitorAlerts error: {Err}", ex.Message);
        }
        return alerts;
    }

    private async Task<string?> GetManagedIdentityToken()
    {
        if (_cachedToken != null && DateTime.UtcNow < _tokenExpiry.AddMinutes(-5))
            return _cachedToken;

        try
        {
            var identityEndpoint = Environment.GetEnvironmentVariable("IDENTITY_ENDPOINT");
            var identityHeader = Environment.GetEnvironmentVariable("IDENTITY_HEADER");

            if (!string.IsNullOrEmpty(identityEndpoint) && !string.IsNullOrEmpty(identityHeader))
            {
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
            _logger.LogDebug("Managed identity token fetch failed: {Err}", ex.Message);
        }
        return null;
    }

    private static string? GetStr(JsonElement el, string prop)
    {
        if (el.TryGetProperty(prop, out var val) && val.ValueKind == JsonValueKind.String) return val.GetString();
        return null;
    }

    // ── Helpers ───────────────────────────────────────────────────

    private async Task<(bool success, string output)> RunAzCli(string args)
    {
        try
        {
            var psi = new ProcessStartInfo { FileName = @"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd", Arguments = args, UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
            using var proc = Process.Start(psi);
            if (proc != null) { var output = await proc.StandardOutput.ReadToEndAsync(); await proc.WaitForExitAsync(); if (proc.ExitCode == 0) return (true, output); }
        }
        catch (Exception ex) { _logger.LogWarning("az cli error: {Err}", ex.Message); }
        return (false, "");
    }

    private static int GetPercentile(List<int> sorted, int percentile) { int idx = (int)Math.Ceiling(percentile / 100.0 * sorted.Count) - 1; return sorted[Math.Max(0, Math.Min(idx, sorted.Count - 1))]; }
    private static string ShortType(string t) { var p = t.Split('/'); return p.Length > 1 ? p[^1] : t; }
    private static bool IsRelevant(string n) => new[] { "medici", "backend", "sql", "database", "app" }.Any(k => n.Contains(k, StringComparison.OrdinalIgnoreCase));
    private static bool IsImportant(string evt, string lvl) => new[] { "error", "warning", "critical", "failure", "restart", "scale" }.Any(k => evt.Contains(k, StringComparison.OrdinalIgnoreCase)) || new[] { "Error", "Warning", "Critical" }.Any(l => lvl.Equals(l, StringComparison.OrdinalIgnoreCase));

    private class AzRes { public string? name { get; set; } public string? type { get; set; } public string? location { get; set; } }
    private class AppInfo { public string? name { get; set; } public string? state { get; set; } }
    private class DbInfo { public string? name { get; set; } public string? status { get; set; } public string? tier { get; set; } }
    private class AzActivity { public string? eventTimestamp { get; set; } public string? level { get; set; } public string? eventName { get; set; } public string? caller { get; set; } public string? resourceId { get; set; } }
}

// ── New Models ──

public class SslCertInfo
{
    public string Host { get; set; } = "";
    public bool IsAccessible { get; set; }
    public bool IsValid { get; set; }
    public string? Subject { get; set; }
    public string? Issuer { get; set; }
    public DateTime? ValidFrom { get; set; }
    public DateTime? ValidTo { get; set; }
    public int DaysUntilExpiry { get; set; }
    public string? Thumbprint { get; set; }
    public DateTime CheckedAt { get; set; }
    public string? Error { get; set; }
}

public class DnsCheckResult
{
    public string Host { get; set; } = "";
    public bool IsResolved { get; set; }
    public List<string> ResolvedAddresses { get; set; } = new();
    public int ResolutionTimeMs { get; set; }
    public DateTime CheckedAt { get; set; }
    public string? Error { get; set; }
}

public class EndpointMetric
{
    public DateTime Timestamp { get; set; }
    public int ResponseTimeMs { get; set; }
    public bool IsHealthy { get; set; }
    public int StatusCode { get; set; }
}

public class EndpointMetricsSummary
{
    public string Endpoint { get; set; } = "";
    public int TotalChecks { get; set; }
    public int RecentChecks { get; set; }
    public double SuccessRate { get; set; }
    public double AvgResponseMs { get; set; }
    public double P50ResponseMs { get; set; }
    public double P95ResponseMs { get; set; }
    public double P99ResponseMs { get; set; }
    public int MinResponseMs { get; set; }
    public int MaxResponseMs { get; set; }
    public DateTime LastChecked { get; set; }
    public int LastResponseMs { get; set; }
    public bool IsCurrentlyHealthy { get; set; }
}
