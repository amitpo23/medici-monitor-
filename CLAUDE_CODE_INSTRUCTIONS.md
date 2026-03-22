# MediciMonitor — Implementation Instructions for Claude Code

## Project Context

MediciMonitor is a .NET 9.0 ASP.NET Minimal API monitoring dashboard for the Medici Booking Engine. It is deployed on Azure App Service (Linux). The project structure:

```
MediciMonitor/
├── Program.cs                  # DI registration + ~75 HTTP endpoints
├── DataService.cs              # All SQL queries (Azure SQL via ADO.NET)
├── Models.cs                   # 25+ DTOs
├── appsettings.json            # Configuration
├── Services/
│   ├── AlertingService.cs      # 19-rule alert engine
│   ├── AlertNotificationService.cs  # Background service bridging alerts → notifications
│   ├── AuditService.cs
│   ├── AzureMonitoringService.cs
│   ├── BusinessIntelligenceService.cs
│   ├── ClaudeAiService.cs
│   ├── DatabaseHealthService.cs
│   ├── EmergencyResponseService.cs
│   ├── FailSafeBackgroundService.cs
│   ├── FailSafeService.cs      # Kill-switch engine, 8 rules, circuit breakers
│   ├── HistoricalDataService.cs
│   ├── InMemoryLogProvider.cs
│   ├── IncidentManagementService.cs
│   ├── NotificationService.cs  # Multi-channel (Webhook, Email, Slack, Teams, WhatsApp)
│   ├── SlaTrackingService.cs
│   └── WebJobsMonitoringService.cs
└── wwwroot/
    └── index.html              # SPA dashboard (12 tabs)
```

**Design rules you MUST follow:**
- All services are singletons registered in Program.cs
- SQL queries use raw ADO.NET (Microsoft.Data.SqlClient) — no EF/ORM
- Every SQL query method is wrapped in try/catch (Safe() pattern in DataService)
- The system is READ-ONLY — never write to production tables (MED_Book, Med_Reservation etc.)
- New monitoring tables (FailSafe_Breakers etc.) are OK to create — they are Monitor-owned tables
- Use Hebrew for user-facing strings (alert messages, labels), English for code/comments
- Follow existing code patterns exactly (naming, spacing, structure)

---

## PHASE 1 — Critical Fixes (Do these FIRST)

### Task 1.1: Make Kill-Switch Actually Enforce via Shared DB Table

**Problem:** Circuit breakers in FailSafeService are in-memory only. The production backend (medici-backend) has no way to check them.

**What to do:**

1. **Create a new SQL migration method** in `DataService.cs` that creates a `Monitor_CircuitBreakers` table if it doesn't exist:

```sql
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Monitor_CircuitBreakers')
CREATE TABLE Monitor_CircuitBreakers (
    BreakerName NVARCHAR(50) PRIMARY KEY,
    IsOpen BIT NOT NULL DEFAULT 0,
    Label NVARCHAR(200),
    Reason NVARCHAR(500),
    TriggeredBy NVARCHAR(100),
    OpenedAt DATETIME2,
    ClosedAt DATETIME2,
    LastUpdated DATETIME2 DEFAULT GETUTCDATE()
)
```

2. **Add methods to DataService.cs:**

```csharp
public async Task EnsureMonitorTablesExist()
// Runs the CREATE TABLE IF NOT EXISTS above

public async Task SyncBreakerToDb(string name, bool isOpen, string? label, string? reason, string? triggeredBy)
// MERGE into Monitor_CircuitBreakers — upsert the breaker state

public async Task<Dictionary<string, bool>> GetBreakersFromDb()
// SELECT BreakerName, IsOpen FROM Monitor_CircuitBreakers
```

3. **Modify FailSafeService.cs:**
   - In `TripBreaker()`: After setting in-memory state, call `await _dataService.SyncBreakerToDb(...)`.
   - In `ResetBreaker()`: Same — sync to DB after reset.
   - In `TripAllBreakers()` and `ResetAll()`: Sync all breakers.
   - Inject `DataService` into FailSafeService constructor (it currently only has `IConfiguration`, `ILogger`, `NotificationService`).

4. **Add a public endpoint** in Program.cs for the backend to query:

```csharp
app.MapGet("/api/failsafe/breakers/check/{name}", async (DataService svc, string name) =>
{
    var breakers = await svc.GetBreakersFromDb();
    var isOpen = breakers.TryGetValue(name.ToUpper(), out var open) && open;
    return Results.Ok(new { breaker = name.ToUpper(), isOpen, timestamp = DateTime.UtcNow });
});

app.MapGet("/api/failsafe/breakers/check", async (DataService svc) =>
{
    var breakers = await svc.GetBreakersFromDb();
    return Results.Ok(new { breakers, timestamp = DateTime.UtcNow });
});
```

5. **Call EnsureMonitorTablesExist()** in Program.cs at startup, after building the app but before `app.Run()`.

**Important:** Do NOT modify the production tables. `Monitor_CircuitBreakers` is a new table owned by the Monitor.

---

### Task 1.2: Fix Alert Notification Cooldown

**Problem:** AlertingService.cs sends notifications on EVERY evaluation cycle without cooldown. This causes alert storms.

**What to do:**

1. **Add these fields** to `AlertingService` class (around line 22, near the existing `_snoozed` dictionary):

```csharp
private readonly Dictionary<string, DateTime> _lastNotifiedPerAlert = new();
```

2. **Modify the notification section** in `EvaluateAlerts()` method (lines 448-463). Replace the current notification block with:

```csharp
// Send notifications for new critical/warning alerts (with cooldown)
if (_notifications != null)
{
    var cooldownMinutes = _notifications.Config.CooldownMinutes;
    foreach (var a in alerts.Where(a => !a.IsAcknowledged && !a.IsSnoozed))
    {
        var minSev = _notifications.Config.MinSeverity;
        bool shouldNotify = a.Severity == "Critical" ||
            (a.Severity == "Warning" && minSev != "Critical") ||
            (a.Severity == "Info" && minSev == "Info");

        if (!shouldNotify) continue;

        // Check cooldown
        if (_lastNotifiedPerAlert.TryGetValue(a.Id, out var lastSent)
            && (DateTime.UtcNow - lastSent).TotalMinutes < cooldownMinutes)
            continue; // Still in cooldown

        _lastNotifiedPerAlert[a.Id] = DateTime.UtcNow;
        _ = _notifications.SendAsync(a.Title, a.Message, a.Severity, a.Category);
    }
}
```

This mirrors the proven pattern from FailSafeService.cs (lines 263-273).

---

### Task 1.3: Add Basic Authentication

**Problem:** All endpoints are publicly accessible with no authentication.

**What to do:**

1. **Add an API key middleware** approach (simpler than Azure AD for initial protection). Add a new file `Services/ApiKeyMiddleware.cs`:

```csharp
namespace MediciMonitor.Services;

public class ApiKeyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly string? _apiKey;
    private readonly HashSet<string> _publicPaths = new(StringComparer.OrdinalIgnoreCase)
    {
        "/healthz", "/readyz", "/index.html", "/", "/favicon.ico"
    };

    public ApiKeyMiddleware(RequestDelegate next, IConfiguration config)
    {
        _next = next;
        _apiKey = config["Security:ApiKey"];
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path.Value ?? "";

        // Allow public paths and static files
        if (_publicPaths.Contains(path) || path.StartsWith("/css") || path.StartsWith("/js") || path.StartsWith("/img"))
        {
            await _next(context);
            return;
        }

        // Allow if no API key is configured (backward compatible)
        if (string.IsNullOrEmpty(_apiKey))
        {
            await _next(context);
            return;
        }

        // Check header: X-API-Key
        if (context.Request.Headers.TryGetValue("X-API-Key", out var headerKey) && headerKey == _apiKey)
        {
            await _next(context);
            return;
        }

        // Check query: ?apikey=...
        if (context.Request.Query.TryGetValue("apikey", out var queryKey) && queryKey == _apiKey)
        {
            await _next(context);
            return;
        }

        context.Response.StatusCode = 401;
        await context.Response.WriteAsJsonAsync(new { error = "Unauthorized", message = "Valid API key required in X-API-Key header" });
    }
}
```

2. **Register in Program.cs** — add BEFORE `app.UseStaticFiles()`:

```csharp
app.UseMiddleware<MediciMonitor.Services.ApiKeyMiddleware>();
```

3. **Add to appsettings.json:**

```json
"Security": {
    "ApiKey": ""
}
```

When `ApiKey` is empty, no authentication is required (backward compatible). When set, all API endpoints require the key.

4. **Update index.html** — add API key support to the fetch calls. Find the existing `fetch()` calls and add a helper:

```javascript
// Add at the top of the <script> section:
const API_KEY = localStorage.getItem('medici_api_key') || '';
function apiFetch(url, options = {}) {
    if (API_KEY) {
        options.headers = { ...options.headers, 'X-API-Key': API_KEY };
    }
    return fetch(url, options);
}
```

Replace all `fetch('/api/...')` calls with `apiFetch('/api/...')`.

Add a settings modal in the dashboard for entering the API key.

---

## PHASE 2 — High Severity Fixes

### Task 2.1: Add State Persistence to Critical Services

**Problem:** Alert history, incidents, SLA data — all lost on restart.

**What to do:**

Follow the **exact pattern** from FailSafeService.cs (`LoadPersistedState()` + `SaveState()`):

1. **AlertingService.cs** — persist `_alertHistory`, `_acknowledged`, `_snoozed`, `_lastNotifiedPerAlert`:
   - Add a `_stateFilePath` field: `Path.Combine(AppContext.BaseDirectory, "alerting-state.json")`
   - Add `LoadPersistedState()` called in constructor
   - Add `SaveState()` called at end of `EvaluateAlerts()` with 1-minute interval check
   - Create a `AlertingPersistedState` class with the 4 dictionaries/lists

2. **IncidentManagementService.cs** — persist incidents list:
   - Same pattern. File: `incidents-state.json`

3. **SlaTrackingService.cs** — persist SLA measurements:
   - Same pattern. File: `sla-state.json`

**Important:** Use the same `TimeSpan.FromMinutes(1)` save interval to avoid excessive disk writes.

---

### Task 2.2: Add BuyRooms Process Health Monitoring

**Problem:** Only checks "last purchase time". Misses running-but-failing scenarios.

**What to do:**

1. **Add to DataService.cs** — new method in `GetFullStatus()` after `LoadBuyRoomsHeartbeat`:

```csharp
await Safe(() => LoadBuyRoomsFunnel(conn, s));
```

Add the method:

```csharp
private async Task LoadBuyRoomsFunnel(SqlConnection conn, SystemStatus s)
{
    // PreBook creation rate (last hour)
    using var cmd1 = new SqlCommand(
        "SELECT COUNT(*) FROM MED_PreBook WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())", conn);
    cmd1.CommandTimeout = 10;
    s.PreBooksLastHour = Convert.ToInt32(await cmd1.ExecuteScalarAsync() ?? 0);

    // Book creation rate (last hour)
    using var cmd2 = new SqlCommand(
        "SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())", conn);
    cmd2.CommandTimeout = 10;
    s.BooksLastHour = Convert.ToInt32(await cmd2.ExecuteScalarAsync() ?? 0);

    // Funnel conversion
    s.BuyRoomsFunnelRate = s.PreBooksLastHour > 0
        ? Math.Round((double)s.BooksLastHour / s.PreBooksLastHour * 100, 1)
        : 0;

    // PreBooks without matching Book (started but not completed)
    using var cmd3 = new SqlCommand(@"
        SELECT COUNT(*) FROM MED_PreBook p
        WHERE p.DateInsert >= DATEADD(HOUR, -2, GETDATE())
          AND NOT EXISTS (SELECT 1 FROM MED_Book b WHERE b.PreBookId = p.Id)", conn);
    cmd3.CommandTimeout = 10;
    s.OrphanedPreBooks = Convert.ToInt32(await cmd3.ExecuteScalarAsync() ?? 0);
}
```

2. **Add fields to SystemStatus** in Models.cs:

```csharp
public int PreBooksLastHour { get; set; }
public int BooksLastHour { get; set; }
public double BuyRoomsFunnelRate { get; set; }
public int OrphanedPreBooks { get; set; }
```

3. **Add alert rule** in AlertingService.cs after the existing BuyRooms check (rule 10):

```csharp
// 10b. BuyRooms funnel drop
var preBooks = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_PreBook WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())");
var books = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE DateInsert >= DATEADD(HOUR, -1, GETDATE())");
if (preBooks > 5 && books == 0)
    alerts.Add(new AlertInfo { Id = "BUYROOMS_FUNNEL_BROKEN", Title = "BuyRooms Funnel Broken",
        Message = $"{preBooks} PreBooks נוצרו בשעה האחרונה אבל 0 Books — BuyRooms כנראה תקוע בשלב ההזמנה",
        Severity = "Critical", Category = "System" });

// 10c. Orphaned PreBooks
var orphaned = await ScalarInt(conn, @"SELECT COUNT(*) FROM MED_PreBook p
    WHERE p.DateInsert >= DATEADD(HOUR, -2, GETDATE())
    AND NOT EXISTS (SELECT 1 FROM MED_Book b WHERE b.PreBookId = p.Id)");
if (orphaned > 10)
    alerts.Add(new AlertInfo { Id = "ORPHANED_PREBOOKS", Title = "Orphaned PreBooks",
        Message = $"{orphaned} PreBooks ללא Book תואם ב-2 שעות אחרונות — כשל חלקי ב-BuyRooms",
        Severity = "Warning", Category = "System" });
```

---

### Task 2.3: Add SalesOffice-to-Zenith Cross-Reference

**Problem:** No validation that completed SalesOffice orders actually created reservations.

**What to do:**

Add a new alert rule in `AlertingService.cs` after the existing SalesOffice rules (after rule 20):

```csharp
// 21. SalesOffice completed but no matching reservation
try
{
    string? soOrdTbl = null, soDetTbl = null;
    foreach (var t in new[] { "SalesOfficeOrders", "[SalesOffice.Orders]", "SalesOffice_Orders" })
    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); soOrdTbl = t; break; } catch { } }
    foreach (var t in new[] { "[SalesOffice.Details]", "SalesOfficeDetails", "SalesOffice_Details" })
    { try { await ScalarInt(conn, $"SELECT TOP 1 1 FROM {t}"); soDetTbl = t; break; } catch { } }

    if (soOrdTbl != null && soDetTbl != null)
    {
        var noReservation = await ScalarInt(conn,
            $@"SELECT COUNT(DISTINCT o.Id)
               FROM {soOrdTbl} o
               INNER JOIN {soDetTbl} d ON d.OrderId = o.Id
               LEFT JOIN Med_Reservation r ON r.HotelCode = CAST(d.HotelId AS NVARCHAR(20))
                   AND r.Datefrom = o.DateFrom AND r.Dateto = o.DateTo
               WHERE o.IsActive = 1
                 AND o.WebJobStatus LIKE 'Completed%'
                 AND o.DateInsert >= DATEADD(DAY, -7, GETDATE())
                 AND r.Id IS NULL");

        if (noReservation > 0)
            alerts.Add(new AlertInfo
            {
                Id = "SO_NO_RESERVATION",
                Title = "SalesOffice Without Reservation",
                Message = $"{noReservation} הזמנות SalesOffice הושלמו בשבוע האחרון ללא Reservation תואמת ב-Zenith — בדיקה נדרשת",
                Severity = noReservation > 10 ? "Critical" : "Warning",
                Category = "Business"
            });
    }
}
catch (Exception ex) { _logger.LogDebug("SalesOffice-Zenith cross-ref check skipped: {Err}", ex.Message); }
```

**Note:** The JOIN logic between SalesOffice.Details and Med_Reservation may need adjustment based on the actual column names. The query above assumes HotelId in Details matches HotelCode in Reservation, and dates align. If the join condition is different, adapt accordingly.

---

### Task 2.4: Add Queue/Push Retry Pattern Monitoring

**Problem:** Queue and Push count errors but don't detect retry loops (same item failing repeatedly).

**What to do:**

Add two new alert rules in AlertingService.cs:

```csharp
// 22. Queue retry loop detection
try
{
    var queueRetryLoop = await ScalarInt(conn,
        @"SELECT COUNT(*) FROM (
            SELECT [Key] FROM Queue
            WHERE Status = 'Error' AND CreatedOn >= DATEADD(HOUR, -6, GETDATE())
            GROUP BY [Key] HAVING COUNT(*) >= 3
        ) x");
    if (queueRetryLoop > 0)
        alerts.Add(new AlertInfo { Id = "QUEUE_RETRY_LOOP", Title = "Queue Retry Loop",
            Message = $"{queueRetryLoop} פריטים בתור נכשלים שוב ושוב (3+ כשלונות ב-6 שעות)",
            Severity = queueRetryLoop > 10 ? "Critical" : "Warning", Category = "Queue" });
}
catch (Exception ex) { _logger.LogDebug("Queue retry loop check skipped: {Err}", ex.Message); }

// 23. Push retry loop detection
try
{
    var pushRetryLoop = await ScalarInt(conn,
        @"SELECT COUNT(*) FROM (
            SELECT HotelId FROM Med_HotelsToPush
            WHERE IsActive = 0 AND Error IS NOT NULL AND Error != 'CancelBook'
              AND DateInsert >= DATEADD(HOUR, -6, GETDATE())
            GROUP BY HotelId HAVING COUNT(*) >= 3
        ) x");
    if (pushRetryLoop > 0)
        alerts.Add(new AlertInfo { Id = "PUSH_RETRY_LOOP", Title = "Push Retry Loop",
            Message = $"{pushRetryLoop} מלונות נכשלים ב-Push שוב ושוב (3+ כשלונות ב-6 שעות)",
            Severity = pushRetryLoop > 5 ? "Critical" : "Warning", Category = "Push" });
}
catch (Exception ex) { _logger.LogDebug("Push retry loop check skipped: {Err}", ex.Message); }
```

**Important:** The `Queue` table column for identifying unique items might be `[Key]`, `ItemId`, or another column. Check with: `SELECT TOP 5 * FROM Queue WHERE Status = 'Error'` and adjust the GROUP BY column.

---

## PHASE 3 — Medium Severity Fixes

### Task 3.1: Add SSL/DNS Alert Rules

Add to `AlertingService.cs` `EvaluateAlerts()` method, in the API health section (after rule 3):

```csharp
// 3b. SSL certificate expiry
try
{
    var sslResults = await _azure.CheckSslCertificates();
    foreach (var ssl in sslResults.Where(s => s.DaysUntilExpiry.HasValue))
    {
        if (ssl.DaysUntilExpiry <= 7)
            alerts.Add(new AlertInfo { Id = $"SSL_EXPIRY_{ssl.Domain}", Title = "SSL Certificate Expiring",
                Message = $"תעודת SSL של {ssl.Domain} פגה בעוד {ssl.DaysUntilExpiry} ימים!",
                Severity = "Critical", Category = "Infrastructure" });
        else if (ssl.DaysUntilExpiry <= 30)
            alerts.Add(new AlertInfo { Id = $"SSL_WARNING_{ssl.Domain}", Title = "SSL Certificate Warning",
                Message = $"תעודת SSL של {ssl.Domain} פגה בעוד {ssl.DaysUntilExpiry} ימים",
                Severity = "Warning", Category = "Infrastructure" });
    }
}
catch (Exception ex) { _logger.LogDebug("SSL expiry check skipped: {Err}", ex.Message); }

// 3c. DNS resolution failures
try
{
    var dnsResults = await _azure.CheckDnsResolution();
    var dnsFailed = dnsResults.Where(d => !d.IsHealthy).ToList();
    if (dnsFailed.Any())
        alerts.Add(new AlertInfo { Id = "DNS_FAILURE", Title = "DNS Resolution Failed",
            Message = $"כשל DNS ב-{dnsFailed.Count} דומיינים: {string.Join(", ", dnsFailed.Select(d => d.Domain))}",
            Severity = "Critical", Category = "Infrastructure" });
}
catch (Exception ex) { _logger.LogDebug("DNS check skipped: {Err}", ex.Message); }
```

**Note:** Check the actual method names and return types in `AzureMonitoringService.cs` — the above assumes `CheckSslCertificates()` and `CheckDnsResolution()` exist. Read the file first and adapt the method calls and property names.

---

### Task 3.2: Add BI Anomaly Alerts

Add to `AlertingService.cs` — requires injecting `BusinessIntelligenceService` or calling DataService directly:

```csharp
// 24. Conversion rate anomaly (compare today vs yesterday)
try
{
    var todaySold = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND IsSold = 1 AND DateInsert >= CAST(GETDATE() AS DATE)");
    var todayTotal = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND DateInsert >= CAST(GETDATE() AS DATE)");
    var yesterdaySold = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND IsSold = 1 AND DateInsert >= CAST(DATEADD(DAY,-1,GETDATE()) AS DATE) AND DateInsert < CAST(GETDATE() AS DATE)");
    var yesterdayTotal = await ScalarInt(conn, "SELECT COUNT(*) FROM MED_Book WHERE IsActive = 1 AND DateInsert >= CAST(DATEADD(DAY,-1,GETDATE()) AS DATE) AND DateInsert < CAST(GETDATE() AS DATE)");

    if (yesterdayTotal > 0 && todayTotal > 5 && DateTime.Now.Hour >= 14)
    {
        var todayRate = (double)todaySold / todayTotal;
        var yesterdayRate = (double)yesterdaySold / yesterdayTotal;
        if (yesterdayRate > 0 && todayRate < yesterdayRate * 0.5)
            alerts.Add(new AlertInfo { Id = "CONVERSION_DROP", Title = "Conversion Rate Drop",
                Message = $"שיעור המרה היום: {todayRate:P0} מול אתמול: {yesterdayRate:P0} — ירידה של {(1 - todayRate / yesterdayRate) * 100:F0}%",
                Severity = "Warning", Category = "Business" });
    }
}
catch (Exception ex) { _logger.LogDebug("Conversion anomaly check skipped: {Err}", ex.Message); }
```

---

### Task 3.3: Add Historical Trend Alerts

Modify `HistoricalDataService.cs` — add trend analysis after each snapshot capture:

```csharp
// After capturing a new snapshot, analyze last 4 snapshots for degradation
private void AnalyzeTrends()
{
    if (_snapshots.Count < 4) return;
    var recent = _snapshots.TakeLast(4).ToList();

    // Check for monotonically increasing error count
    bool errorsIncreasing = true;
    for (int i = 1; i < recent.Count; i++)
    {
        if (recent[i].TotalErrors <= recent[i-1].TotalErrors)
        { errorsIncreasing = false; break; }
    }

    if (errorsIncreasing)
    {
        // Emit a warning — store in a public property that AlertingService can read
        LastTrendWarning = $"שגיאות עולות ב-{recent.Count} snapshots רצופים: {string.Join(" → ", recent.Select(s => s.TotalErrors))}";
        LastTrendWarningTime = DateTime.UtcNow;
    }
}

public string? LastTrendWarning { get; private set; }
public DateTime? LastTrendWarningTime { get; private set; }
```

Then in AlertingService, add:

```csharp
// 25. Trend degradation from historical data
if (_historical?.LastTrendWarning != null
    && _historical.LastTrendWarningTime.HasValue
    && (DateTime.UtcNow - _historical.LastTrendWarningTime.Value).TotalHours < 1)
{
    alerts.Add(new AlertInfo { Id = "TREND_DEGRADATION", Title = "Trend Degradation Detected",
        Message = _historical.LastTrendWarning, Severity = "Warning", Category = "System" });
}
```

Inject `HistoricalDataService` into AlertingService constructor.

---

### Task 3.4: WebJobs Self-Health Monitoring

Add to `WebJobsMonitoringService.cs`:

```csharp
private int _consecutiveEmptyResults = 0;
public bool IsMonitoringHealthy => _consecutiveEmptyResults < 3;
public string? MonitoringIssue { get; private set; }
```

In `GetDashboardAsync()`, after the refresh attempt:

```csharp
if (dashboard.Apps.Count == 0)
{
    _consecutiveEmptyResults++;
    if (_consecutiveEmptyResults >= 3)
        MonitoringIssue = $"WebJobs monitoring returned 0 results {_consecutiveEmptyResults} times — Azure CLI may not be configured";
}
else
{
    _consecutiveEmptyResults = 0;
    MonitoringIssue = null;
}
```

Add alert in AlertingService:

```csharp
// 26. WebJobs monitoring self-health
if (_webJobs != null && !_webJobs.IsMonitoringHealthy)
    alerts.Add(new AlertInfo { Id = "WEBJOBS_MONITORING_DOWN", Title = "WebJobs Monitoring Down",
        Message = _webJobs.MonitoringIssue ?? "ניטור WebJobs לא מחזיר תוצאות",
        Severity = "Warning", Category = "Infrastructure" });
```

Inject `WebJobsMonitoringService` into AlertingService.

---

## PHASE 4 — Long-term Improvements

### Task 4.1: Migrate State to Persistent Storage

When ready to scale beyond single-instance:

1. Create a `Monitor_State` table in Azure SQL:
```sql
CREATE TABLE Monitor_State (
    ServiceName NVARCHAR(100),
    StateKey NVARCHAR(100),
    StateJson NVARCHAR(MAX),
    LastUpdated DATETIME2 DEFAULT GETUTCDATE(),
    PRIMARY KEY (ServiceName, StateKey)
)
```

2. Create a `StateStorageService` that reads/writes JSON blobs per service.

3. Replace file-based persistence with DB-based persistence in all services.

### Task 4.2: Dashboard Real-time Updates (WebSocket)

Replace polling with SignalR:

1. Add NuGet: `Microsoft.AspNetCore.SignalR`
2. Create a `MonitorHub` class
3. Background services push updates through the hub
4. Frontend subscribes via SignalR JS client

### Task 4.3: Frontend Modernization

The monolithic `index.html` should be split into a proper SPA framework. Recommended approach:

1. Create a `/frontend` directory with React or Vue
2. Keep the current index.html as-is for backward compatibility
3. Build the new frontend to `/wwwroot/v2/`
4. Add a redirect: `/v2` → new dashboard

---

## Important Notes for Claude Code

1. **Always read the target file BEFORE editing.** The code may have changed since this document was written.

2. **Test incrementally.** After each task, verify the project still builds: `dotnet build`

3. **Follow the existing Safe() pattern** for all new SQL queries:
```csharp
await Safe(() => YourNewMethod(conn, s));
```

4. **SQL injection safety:** For user-facing endpoints that accept parameters, always use parameterized queries (`@param`), never string concatenation.

5. **The frontend (index.html) is a single large file.** When editing it, be surgical — find the exact section and modify only what's needed.

6. **After ALL changes, run:** `dotnet build` to verify compilation.

7. **Create a git commit after each completed phase** with a descriptive message.

## Daily Dev Log
At the end of each work session, run `./dev-log.sh` to log what was done today.
This creates a `.dev-logs/YYYY-MM-DD.md` file, commits and pushes it.
A central collector agent aggregates all logs across machines and projects.
