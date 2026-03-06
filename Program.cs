using MediciMonitor;
using MediciMonitor.Services;

var builder = WebApplication.CreateBuilder(args);

// ── Logging: in-memory buffer (10K) + rolling file ──
var logBuffer = new LogBuffer(10000);
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
builder.Services.AddSingleton<NotificationService>();
builder.Services.AddSingleton<DatabaseHealthService>();
builder.Services.AddSingleton<SlaTrackingService>();
builder.Services.AddSingleton<AuditService>();
builder.Services.AddSingleton<IncidentManagementService>();
builder.Services.AddSingleton<FailSafeService>();
builder.Services.AddHostedService<AlertNotificationService>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();
app.UseCors();
app.UseStaticFiles();

// ── Wire up cross-service dependencies ──
var alertingSvc = app.Services.GetRequiredService<AlertingService>();
var notificationSvc = app.Services.GetRequiredService<NotificationService>();
alertingSvc.SetNotificationService(notificationSvc);

builder.Configuration.GetSection("Notifications").Bind(notificationSvc.Config);
var configuredThresholds = builder.Configuration.GetSection("AlertThresholds").Get<AlertThresholds>();
if (configuredThresholds != null)
    alertingSvc.Thresholds = configuredThresholds;

// ── Start background services ──
app.Services.GetRequiredService<HistoricalDataService>().StartAutoCapture(15);
app.Services.GetRequiredService<SlaTrackingService>().StartTracking(60);

// ═══════════════════════════════════════════════════════════════
//  Health Endpoints (self-monitoring)
// ═══════════════════════════════════════════════════════════════
app.MapGet("/healthz", () => Results.Ok(new { status = "Healthy", timestamp = DateTime.UtcNow, service = "MediciMonitor v2.2" }));
app.MapGet("/readyz", async (DataService svc) =>
{
    try
    {
        var status = await svc.GetFullStatus();
        return Results.Ok(new { status = status.DbConnected ? "Ready" : "Degraded", dbConnected = status.DbConnected, timestamp = DateTime.UtcNow });
    }
    catch (Exception ex) { return Results.Ok(new { status = "NotReady", error = ex.Message, timestamp = DateTime.UtcNow }); }
});

// ═══════════════════════════════════════════════════════════════
//  Business data
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/status", async (DataService svc, AuditService audit, HttpContext ctx) =>
{
    audit.RecordFromHttp(ctx, "ViewStatus");
    return Results.Ok(await svc.GetFullStatus());
});

app.MapGet("/api/salesorder/diagnostics", async (DataService svc, AuditService audit, HttpContext ctx) =>
{
    audit.RecordFromHttp(ctx, "SalesOrderDiagnostics");
    return Results.Ok(await svc.GetSalesOrderDiagnostics());
});

app.MapGet("/api/salesorder/trace/{orderId:int}", async (DataService svc, AuditService audit, HttpContext ctx, int orderId) =>
{
    audit.RecordFromHttp(ctx, "SalesOrderTrace", orderId.ToString());
    return Results.Ok(await svc.GetSalesOrderTrace(orderId));
});

// ═══════════════════════════════════════════════════════════════
//  Azure Monitoring
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/azure/health", async (AzureMonitoringService svc, AuditService audit, HttpContext ctx) =>
{
    audit.RecordFromHttp(ctx, "AzureHealth");
    return Results.Ok(await svc.ComprehensiveApiHealthCheck());
});

app.MapGet("/api/azure/resources", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.GetDetailedAzureStatus()));

app.MapGet("/api/azure/alerts", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.GetActiveAlerts()));

app.MapGet("/api/azure/performance", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.GetPerformanceMetrics()));

app.MapGet("/api/azure/ssl", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.CheckSslCertificates()));

app.MapGet("/api/azure/dns", async (AzureMonitoringService svc) =>
    Results.Ok(await svc.CheckDnsHealth()));

app.MapGet("/api/azure/endpoint-metrics", (AzureMonitoringService svc) =>
    Results.Ok(svc.GetEndpointMetrics()));

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

app.MapGet("/api/history/compare", async (HistoricalDataService svc, HttpContext ctx) =>
{
    var p1 = ctx.Request.Query["period1"].FirstOrDefault() ?? "24h";
    var p2 = ctx.Request.Query["period2"].FirstOrDefault() ?? "7d";
    return Results.Ok(await svc.ComparePeriods(p1, p2));
});

app.MapGet("/api/history/export/{period?}", async (HistoricalDataService svc, string? period) =>
    Results.Text(await svc.ExportCsv(period ?? "24h"), "text/csv"));

// ═══════════════════════════════════════════════════════════════
//  Alerting (enhanced)
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/alerts", async (AlertingService svc) =>
    Results.Ok(await svc.EvaluateAlerts()));

app.MapGet("/api/alerts/summary", async (AlertingService svc) =>
{
    var alerts = await svc.EvaluateAlerts();
    return Results.Ok(new { Alerts = alerts, Summary = svc.GenerateSummary(alerts) });
});

app.MapGet("/api/alerts/history", (AlertingService svc, HttpContext ctx) =>
{
    int last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : 100;
    var severity = ctx.Request.Query["severity"].FirstOrDefault();
    return Results.Ok(svc.GetHistory(last, severity));
});

app.MapPost("/api/alerts/{alertId}/acknowledge", (AlertingService svc, string alertId) =>
    Results.Ok(new { success = svc.Acknowledge(alertId), alertId }));

app.MapPost("/api/alerts/{alertId}/snooze", (AlertingService svc, string alertId, HttpContext ctx) =>
{
    int minutes = int.TryParse(ctx.Request.Query["minutes"].FirstOrDefault(), out var m) ? m : 60;
    return Results.Ok(new { success = svc.Snooze(alertId, minutes), alertId, snoozedMinutes = minutes });
});

app.MapPost("/api/alerts/{alertId}/unacknowledge", (AlertingService svc, string alertId) =>
    Results.Ok(new { success = svc.Unacknowledge(alertId), alertId }));

app.MapGet("/api/alerts/thresholds", (AlertingService svc) =>
    Results.Ok(svc.GetThresholds()));

app.MapPut("/api/alerts/thresholds", async (AlertingService svc, HttpContext ctx) =>
{
    var thresholds = await ctx.Request.ReadFromJsonAsync<AlertThresholds>();
    if (thresholds == null) return Results.BadRequest("Invalid thresholds");
    return Results.Ok(svc.UpdateThresholds(thresholds));
});

// ═══════════════════════════════════════════════════════════════
//  Notifications
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/notifications/config", (NotificationService svc) =>
    Results.Ok(svc.GetConfig()));

app.MapPut("/api/notifications/config", async (NotificationService svc, HttpContext ctx) =>
{
    var config = await ctx.Request.ReadFromJsonAsync<NotificationConfig>();
    if (config == null) return Results.BadRequest("Invalid config");
    svc.Config = config;
    return Results.Ok(new { success = true, config = svc.Config });
});

app.MapPost("/api/notifications/test", async (NotificationService svc) =>
    Results.Ok(await svc.SendTestAsync()));

app.MapGet("/api/notifications/history", (NotificationService svc, HttpContext ctx) =>
{
    int last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : 50;
    return Results.Ok(svc.GetHistory(last));
});

// ═══════════════════════════════════════════════════════════════
//  Database Health
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/db-health", async (DatabaseHealthService svc, AuditService audit, HttpContext ctx) =>
{
    audit.RecordFromHttp(ctx, "DbHealth");
    return Results.Ok(await svc.GetHealthReport());
});

// ═══════════════════════════════════════════════════════════════
//  SLA Tracking
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/sla", (SlaTrackingService svc, AuditService audit, HttpContext ctx) =>
{
    audit.RecordFromHttp(ctx, "SlaReport");
    return Results.Ok(svc.GetReport());
});

app.MapGet("/api/sla/{endpoint}", (SlaTrackingService svc, string endpoint) =>
{
    var entry = svc.GetEndpointSla(endpoint);
    return entry != null ? Results.Ok(entry) : Results.NotFound(new { error = $"Endpoint '{endpoint}' not found" });
});

// ═══════════════════════════════════════════════════════════════
//  Audit Trail
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/audit", (AuditService svc, HttpContext ctx) =>
{
    var action = ctx.Request.Query["action"].FirstOrDefault();
    var endpoint = ctx.Request.Query["endpoint"].FirstOrDefault();
    int last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : 100;
    DateTime? since = DateTime.TryParse(ctx.Request.Query["since"].FirstOrDefault(), out var dt) ? dt : null;
    svc.RecordFromHttp(ctx, "ViewAudit");
    return Results.Ok(svc.GetReport(action, endpoint, last, since));
});

// ═══════════════════════════════════════════════════════════════
//  Incident Management
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/incidents", (IncidentManagementService svc, HttpContext ctx) =>
{
    var status = ctx.Request.Query["status"].FirstOrDefault();
    int last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : 50;
    return Results.Ok(svc.GetReport(status, last));
});

app.MapGet("/api/incidents/{id:int}", (IncidentManagementService svc, int id) =>
{
    var incident = svc.GetById(id);
    return incident != null ? Results.Ok(incident) : Results.NotFound(new { error = "Incident not found" });
});

app.MapPost("/api/incidents", async (IncidentManagementService svc, HttpContext ctx) =>
{
    var body = await ctx.Request.ReadFromJsonAsync<CreateIncidentRequest>();
    if (body == null) return Results.BadRequest("Invalid body");
    var incident = svc.Create(body.Title, body.Description, body.Severity ?? "Warning");
    return Results.Ok(incident);
});

app.MapPut("/api/incidents/{id:int}", async (IncidentManagementService svc, int id, HttpContext ctx) =>
{
    var body = await ctx.Request.ReadFromJsonAsync<UpdateIncidentRequest>();
    if (body == null) return Results.BadRequest("Invalid body");
    var incident = svc.Update(id, body.Status, body.Note);
    return incident != null ? Results.Ok(incident) : Results.NotFound(new { error = "Incident not found" });
});

app.MapPost("/api/incidents/{id:int}/postmortem", async (IncidentManagementService svc, int id, HttpContext ctx) =>
{
    var body = await ctx.Request.ReadFromJsonAsync<PostmortemRequest>();
    if (body == null) return Results.BadRequest("Invalid body");
    var incident = svc.AddPostmortem(id, body.RootCause, body.Resolution, body.Prevention);
    return incident != null ? Results.Ok(incident) : Results.NotFound(new { error = "Incident not found" });
});

// ═══════════════════════════════════════════════════════════════
//  Logs (enhanced)
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
    return Results.Ok(buf.GetStats());
});

app.MapGet("/api/logs/export/csv", (HttpContext ctx) =>
{
    var buf = ctx.RequestServices.GetRequiredService<LogBuffer>();
    var level = ctx.Request.Query["level"].FirstOrDefault();
    var category = ctx.Request.Query["category"].FirstOrDefault();
    var search = ctx.Request.Query["search"].FirstOrDefault();
    int? last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : null;
    return Results.Text(buf.ExportCsv(level, category, search, last), "text/csv");
});

app.MapGet("/api/logs/export/json", (HttpContext ctx) =>
{
    var buf = ctx.RequestServices.GetRequiredService<LogBuffer>();
    var level = ctx.Request.Query["level"].FirstOrDefault();
    var category = ctx.Request.Query["category"].FirstOrDefault();
    var search = ctx.Request.Query["search"].FirstOrDefault();
    int? last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : null;
    return Results.Text(buf.ExportJson(level, category, search, last), "application/json");
});

// ═══════════════════════════════════════════════════════════════
//  Fail-Safe & Kill-Switch
// ═══════════════════════════════════════════════════════════════
app.MapGet("/api/failsafe/scan", async (FailSafeService svc, AuditService audit, HttpContext ctx) =>
{
    audit.RecordFromHttp(ctx, "FailSafeScan");
    return Results.Ok(await svc.ScanAsync());
});

app.MapGet("/api/failsafe/status", (FailSafeService svc) =>
{
    var last = svc.GetLastScan();
    if (last != null) return Results.Ok(last);
    return Results.Ok(new { status = "NO_SCAN_YET", message = "No scan has been performed yet" });
});

app.MapGet("/api/failsafe/breakers", (FailSafeService svc) =>
    Results.Ok(svc.GetBreakers()));

app.MapPost("/api/failsafe/breaker/{name}/trip", (FailSafeService svc, string name, HttpContext ctx) =>
{
    var reason = ctx.Request.Query["reason"].FirstOrDefault() ?? "Manual trip";
    var actor = ctx.Request.Query["actor"].FirstOrDefault() ?? "Operator";
    var result = svc.TripBreaker(name, reason, actor);
    return result != null ? Results.Ok(result) : Results.NotFound(new { error = $"Breaker '{name}' not found" });
});

app.MapPost("/api/failsafe/breaker/{name}/reset", (FailSafeService svc, string name, HttpContext ctx) =>
{
    var actor = ctx.Request.Query["actor"].FirstOrDefault() ?? "Operator";
    var result = svc.ResetBreaker(name, actor);
    return result != null ? Results.Ok(result) : Results.NotFound(new { error = $"Breaker '{name}' not found" });
});

app.MapPost("/api/failsafe/kill-switch", (FailSafeService svc, HttpContext ctx) =>
{
    var reason = ctx.Request.Query["reason"].FirstOrDefault() ?? "Emergency kill switch";
    var actor = ctx.Request.Query["actor"].FirstOrDefault() ?? "Operator";
    svc.TripAll(reason, actor);
    return Results.Ok(new { success = true, message = "ALL circuit breakers tripped", reason, actor });
});

app.MapPost("/api/failsafe/reset-all", (FailSafeService svc, HttpContext ctx) =>
{
    var actor = ctx.Request.Query["actor"].FirstOrDefault() ?? "Operator";
    svc.ResetAll(actor);
    return Results.Ok(new { success = true, message = "All circuit breakers reset", actor });
});

app.MapGet("/api/failsafe/flagged", (FailSafeService svc, HttpContext ctx) =>
{
    var status = ctx.Request.Query["status"].FirstOrDefault();
    return Results.Ok(svc.GetFlaggedItems(status));
});

app.MapPost("/api/failsafe/flagged/{id:int}/approve", (FailSafeService svc, int id, HttpContext ctx) =>
{
    var actor = ctx.Request.Query["actor"].FirstOrDefault() ?? "Operator";
    var result = svc.ApproveFlag(id, actor);
    return result != null ? Results.Ok(result) : Results.NotFound(new { error = "Flag not found" });
});

app.MapPost("/api/failsafe/flagged/{id:int}/reject", (FailSafeService svc, int id, HttpContext ctx) =>
{
    var actor = ctx.Request.Query["actor"].FirstOrDefault() ?? "Operator";
    var note = ctx.Request.Query["note"].FirstOrDefault();
    var result = svc.RejectFlag(id, actor, note);
    return result != null ? Results.Ok(result) : Results.NotFound(new { error = "Flag not found" });
});

app.MapGet("/api/failsafe/history", (FailSafeService svc, HttpContext ctx) =>
{
    int last = int.TryParse(ctx.Request.Query["last"].FirstOrDefault(), out var n) ? n : 100;
    return Results.Ok(svc.GetTriggerHistory(last));
});

app.MapGet("/api/failsafe/config", (FailSafeService svc) =>
    Results.Ok(svc.Config));

app.MapPut("/api/failsafe/config", async (FailSafeService svc, HttpContext ctx) =>
{
    var config = await ctx.Request.ReadFromJsonAsync<FailSafeConfig>();
    if (config == null) return Results.BadRequest("Invalid config");
    svc.Config = config;
    return Results.Ok(new { success = true, config = svc.Config });
});

// ── Dashboard ──
app.MapGet("/", () => Results.Redirect("/index.html"));

app.Run();

// ── Request DTOs ──
record CreateIncidentRequest(string Title, string Description, string? Severity);
record UpdateIncidentRequest(string Status, string? Note);
record PostmortemRequest(string RootCause, string Resolution, string Prevention);
