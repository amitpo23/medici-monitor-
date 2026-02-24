using MediciMonitor;
using MediciMonitor.Services;

var builder = WebApplication.CreateBuilder(args);

// ── Logging: in-memory buffer + rolling file ──
var logBuffer = new LogBuffer(2000);
builder.Services.AddSingleton<LogBuffer>(logBuffer);
var logDir = Path.Combine(AppContext.BaseDirectory, "Logs");
builder.Logging.AddProvider(new InMemoryLoggerProvider(logBuffer, logDir));

// ── Register services ──
builder.Services.AddSingleton<DataService>();
builder.Services.AddSingleton<AzureMonitoringService>();
builder.Services.AddSingleton<BusinessIntelligenceService>();
builder.Services.AddSingleton<EmergencyResponseService>();
builder.Services.AddSingleton<HistoricalDataService>();
builder.Services.AddSingleton<AlertingService>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();
app.UseCors();
app.UseStaticFiles();

// ── Start auto-capture (snapshots every 15 min) ──
app.Services.GetRequiredService<HistoricalDataService>().StartAutoCapture(15);

// ═══════════════════════════════════════════════════════════════
//  Business data
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/status", async (DataService svc) => Results.Ok(await svc.GetFullStatus()));

// ═══════════════════════════════════════════════════════════════
//  Azure Monitoring
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/azure/health", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.ComprehensiveApiHealthCheck()));

app.MapGet("/api/azure/resources", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.GetDetailedAzureStatus()));

app.MapGet("/api/azure/alerts", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.GetActiveAlerts()));

app.MapGet("/api/azure/performance", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.GetPerformanceMetrics()));

// ═══════════════════════════════════════════════════════════════
//  Business Intelligence
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/bi/{period?}", async (BusinessIntelligenceService svc, string? period) =>
    Results.Ok(await svc.GetBusinessIntelligence(period ?? "today")));

// ═══════════════════════════════════════════════════════════════
//  Emergency Response
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/emergency/status", async (EmergencyResponseService svc) =>
    Results.Ok(await svc.GetEmergencyStatus()));

app.MapPost("/api/emergency/action/{actionType}", async (EmergencyResponseService svc, string actionType, HttpRequest req) =>
{
    bool confirmed = req.Query.ContainsKey("confirmed");
    return Results.Ok(await svc.ExecuteEmergencyAction(actionType, confirmed));
});

// ═══════════════════════════════════════════════════════════════
//  Historical Data & Trends
// ═══════════════════════════════════════════════════════════════
app.MapPost("/api/history/snapshot", async (HistoricalDataService svc) =>
    Results.Ok(await svc.CaptureCurrentSnapshot()));

app.MapGet("/api/history/trends/{period?}", async (HistoricalDataService svc, string? period) =>
    Results.Ok(await svc.GetTrendAnalysis(period ?? "24h")));

app.MapGet("/api/history/report/{period?}", async (HistoricalDataService svc, string? period) =>
    Results.Ok(await svc.GetPerformanceReport(period ?? "7d")));

// ═══════════════════════════════════════════════════════════════
//  Alerting
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/alerts", async (AlertingService svc) =>
    Results.Ok(await svc.EvaluateAlerts()));

app.MapGet("/api/alerts/summary", async (AlertingService svc) =>
{
    var alerts = await svc.EvaluateAlerts();
    return Results.Ok(new { Alerts = alerts, Summary = svc.GenerateSummary(alerts) });
});

// ═══════════════════════════════════════════════════════════════
//  Logs
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/logs", (HttpContext ctx) =>
{
    var buf = ctx.RequestServices.GetRequiredService<LogBuffer>();
    var req = ctx.Request;
    var level = req.Query["level"].FirstOrDefault();
    var category = req.Query["category"].FirstOrDefault();
    var search = req.Query["search"].FirstOrDefault();
    int? last = int.TryParse(req.Query["last"].FirstOrDefault(), out var n) ? n : null;
    DateTime? since = DateTime.TryParse(req.Query["since"].FirstOrDefault(), out var dt) ? dt : null;

    var entries = buf.Query(level, category, search, last, since);
    return Results.Ok(new
    {
        total = buf.GetAll().Count,
        filtered = entries.Count,
        entries
    });
});

app.MapGet("/api/logs/stats", (HttpContext ctx) =>
{
    var buf = ctx.RequestServices.GetRequiredService<LogBuffer>();
    var all = buf.GetAll();
    var last5m = all.Where(e => e.Timestamp >= DateTime.UtcNow.AddMinutes(-5)).ToList();
    return Results.Ok(new
    {
        totalEntries = all.Count,
        last5Minutes = last5m.Count,
        byLevel = all.GroupBy(e => e.Level).ToDictionary(g => g.Key, g => g.Count()),
        byCategory = all.GroupBy(e => e.Category).ToDictionary(g => g.Key, g => g.Count()),
        errors = all.Count(e => e.Level == "Error" || e.Level == "Critical"),
        warnings = all.Count(e => e.Level == "Warning"),
        latestError = all.LastOrDefault(e => e.Level == "Error" || e.Level == "Critical"),
        logFilePath = Path.Combine(AppContext.BaseDirectory, "Logs", $"medici-monitor-{DateTime.UtcNow:yyyyMMdd}.log")
    });
});

// ── Dashboard ──
app.MapGet("/", () => Results.Redirect("/index.html"));

app.Run();
