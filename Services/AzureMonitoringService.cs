using RestSharp;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace MediciMonitor.Services;

/// <summary>
/// Comprehensive API health checking + Azure resource status via CLI.
/// Ported from Medici-Control-Panel AdvancedAzureMonitoringService.
/// </summary>
public class AzureMonitoringService
{
    private readonly RestClient _restClient = new();
    private readonly ILogger<AzureMonitoringService> _logger;

    public AzureMonitoringService(ILogger<AzureMonitoringService> logger) => _logger = logger;

    // ── Health Check ──────────────────────────────────────────────

    public async Task<List<ApiHealthStatus>> ComprehensiveApiHealthCheck()
    {
        var endpoints = new (string url, string name, bool isInternal, HashSet<HttpStatusCode> ok)[]
        {
            ("https://medici-backend.azurewebsites.net/healthcheck",
             "Production Backend Health", false,
             new() { HttpStatusCode.OK }),

            ("https://medici-backend.azurewebsites.net/ZenithApi/HelloZenith",
             "Zenith API", false,
             new() { HttpStatusCode.OK }),

            ("https://medici-backend-dev-f9h6hxgncha9fbbp.eastus2-01.azurewebsites.net/",
             "Dev Backend Reachability", false,
             new() { HttpStatusCode.OK, HttpStatusCode.Moved, HttpStatusCode.Redirect }),

            ("https://medici-backend.azurewebsites.net/swagger",
             "API Documentation", false,
             new() { HttpStatusCode.OK, HttpStatusCode.Unauthorized, HttpStatusCode.Forbidden,
                     HttpStatusCode.Moved, HttpStatusCode.Redirect }),

            ("https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration",
             "Azure AD Connectivity", true,
             new() { HttpStatusCode.OK })
        };

        var tasks = endpoints.Select(e => CheckEndpoint(e.url, e.name, e.isInternal, e.ok)).ToList();
        tasks.Add(CheckSqlTcpConnectivity("medici-sql-server.database.windows.net", 1433, "SQL Server TCP"));
        return (await Task.WhenAll(tasks)).ToList();
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
                if (list != null)
                    resources.AddRange(list.Where(r => IsRelevant(r.name ?? "")).Select(r => new AzureResourceStatus
                    {
                        ResourceName = r.name ?? "?", ResourceType = ShortType(r.type ?? ""),
                        Status = "Running", Location = r.location ?? "?", LastUpdated = DateTime.UtcNow
                    }));
            }

            var (ok2, json2) = await RunAzCli("webapp list --query \"[?contains(name, 'medici')].{name:name, state:state}\" --output json");
            if (ok2)
            {
                var apps = JsonSerializer.Deserialize<List<AppInfo>>(json2);
                if (apps != null)
                    foreach (var a in apps)
                    {
                        var ex = resources.FirstOrDefault(r => r.ResourceName == a.name);
                        if (ex != null) ex.Status = a.state ?? "?";
                    }
            }

            var (ok3, json3) = await RunAzCli("sql db list --server medici-sql-server --resource-group Medici-RG --query \"[].{name:name, status:status, tier:currentServiceObjectiveName}\" --output json");
            if (ok3)
            {
                var dbs = JsonSerializer.Deserialize<List<DbInfo>>(json3);
                if (dbs != null)
                    resources.AddRange(dbs.Select(d => new AzureResourceStatus
                    {
                        ResourceName = $"Database: {d.name}", ResourceType = $"SQL Database ({d.tier})",
                        Status = d.status ?? "?", Location = "West Europe", LastUpdated = DateTime.UtcNow
                    }));
            }
        }
        catch (Exception ex)
        {
            resources.Add(new AzureResourceStatus { ResourceName = "Azure CLI Error", ResourceType = "Error", Status = ex.Message, Location = "N/A", LastUpdated = DateTime.UtcNow });
        }
        return resources;
    }

    // ── Azure Alerts ─────────────────────────────────────────────

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
                    alerts.AddRange(activities
                        .Where(a => IsImportant(a.eventName ?? "", a.level ?? ""))
                        .Select(a => new AzureAlert
                        {
                            Timestamp = DateTime.TryParse(a.eventTimestamp, out var dt) ? dt : DateTime.UtcNow,
                            Level = a.level ?? "Info",
                            Message = a.eventName ?? "Unknown",
                            Source = a.caller ?? "Azure System",
                            ResourceId = (a.resourceId ?? "").Split('/').LastOrDefault() ?? ""
                        }));
            }
        }
        catch (Exception ex) { _logger.LogError("GetActiveAlerts error: {Err}", ex.Message); }
        return alerts;
    }

    // ── Performance Metrics ──────────────────────────────────────

    public async Task<AzurePerformanceMetrics> GetPerformanceMetrics()
    {
        var m = new AzurePerformanceMetrics { Timestamp = DateTime.UtcNow };
        try
        {
            var (ok, _) = await RunAzCli($@"monitor metrics list --resource /subscriptions/2da025cc-dfe5-450f-a18f-10549a3907e3/resourceGroups/Medici-RG/providers/Microsoft.Web/sites/medici-backend --metric ""CpuTime,Requests,ResponseTime,MemoryWorkingSet"" --start-time {DateTime.UtcNow.AddHours(-1):yyyy-MM-ddTHH:mm:ssZ} --interval PT1M --output json");
            // parse values if available (placeholder — would need real JSON parsing)
        }
        catch (Exception ex) { _logger.LogError("GetPerformanceMetrics error: {Err}", ex.Message); }
        return m;
    }

    // ── Helpers ───────────────────────────────────────────────────

    private async Task<(bool success, string output)> RunAzCli(string args)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = @"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
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
        catch (Exception ex) { _logger.LogWarning("az cli error: {Err}", ex.Message); }
        return (false, "");
    }

    private static string ShortType(string t) { var p = t.Split('/'); return p.Length > 1 ? p[^1] : t; }
    private static bool IsRelevant(string n) => new[] { "medici", "backend", "sql", "database", "app" }.Any(k => n.Contains(k, StringComparison.OrdinalIgnoreCase));
    private static bool IsImportant(string evt, string lvl) =>
        new[] { "error", "warning", "critical", "failure", "restart", "scale" }.Any(k => evt.Contains(k, StringComparison.OrdinalIgnoreCase)) ||
        new[] { "Error", "Warning", "Critical" }.Any(l => lvl.Equals(l, StringComparison.OrdinalIgnoreCase));

    // DTOs for JSON deserialization
    private class AzRes { public string? name { get; set; } public string? type { get; set; } public string? location { get; set; } }
    private class AppInfo { public string? name { get; set; } public string? state { get; set; } }
    private class DbInfo { public string? name { get; set; } public string? status { get; set; } public string? tier { get; set; } }
    private class AzActivity { public string? eventTimestamp { get; set; } public string? level { get; set; } public string? eventName { get; set; } public string? caller { get; set; } public string? resourceId { get; set; } }
}
