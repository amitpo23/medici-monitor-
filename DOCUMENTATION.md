# MediciMonitor — תיעוד מלא v2.1

> **מהדורה:** 2.1 | **תאריך:** 2026-02-24 | **מחבר:** GitHub Copilot + Amit  
> **לינק ציבורי:** https://medici-monitor-dashboard.azurewebsites.net  
> **מטרה:** מערכת ניטור אחודה (Unified Operations Center) למערכת Medici Booking Engine

---

## תוכן עניינים

1. [סקירה כללית](#1-סקירה-כללית)
2. [ארכיטקטורה](#2-ארכיטקטורה)
3. [מבנה קבצים](#3-מבנה-קבצים)
4. [שירותים (Services)](#4-שירותים)
5. [API Endpoints](#5-api-endpoints)
6. [מודלים (Models)](#6-מודלים)
7. [Dashboard — 12 לשוניות](#7-dashboard)
8. [מערכת Logging](#8-מערכת-logging)
9. [Azure Deployment](#9-azure-deployment)
10. [הרצה מקומית](#10-הרצה-מקומית)
11. [שאילתות SQL](#11-שאילתות-sql)
12. [פתרון בעיות](#12-פתרון-בעיות)

---

## 1. סקירה כללית

**MediciMonitor** הוא פרויקט .NET 9.0 עצמאי (Standalone) שמספק:

- **ניטור בזמן אמת** של כל מערכת ההזמנות (Bookings, Cancellations, Pushes, Queue)
- **ניתוח עסקי** (BI Analytics) — Conversion, Revenue, Room Waste, Price Drift
- **ניטור Azure** — Health checks לכל ה-APIs, סטטוס משאבים, התראות
- **חירום** — הערכת סיכונים, פעולות חירום
- **היסטוריה** — Snapshots כל 15 דקות, מגמות, דוחות ביצועים
- **התראות** — 7 כללי התראה אוטומטיים
- **Logging** — In-memory ring buffer + rolling file

**קריאה בלבד** — המערכת רק קוראת מ-DB, לא כותבת, ולא משפיעה על הקוד הקיים.

---

## 2. ארכיטקטורה

```
┌─────────────────────────────────────────────────────────┐
│                  BROWSER (Dashboard)                    │
│            12-tab SPA  ·  index.html                    │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP (REST JSON)
┌────────────────────────▼────────────────────────────────┐
│              ASP.NET Minimal API (.NET 9.0)             │
│              Program.cs — 14 Routes                     │
├─────────────┬───────────┬───────────┬───────────────────┤
│ DataService │ AzureMon  │ BI Svc    │ Emergency Svc     │
│ (16 SQL     │ (Health   │ (Period   │ (Risk + Actions)  │
│  queries)   │  checks)  │  analytics│                   │
├─────────────┼───────────┼───────────┼───────────────────┤
│ Historical  │ Alerting  │ InMemory  │                   │
│ (Snapshots) │ (7 Rules) │ LogProv.  │                   │
└──────┬──────┴─────┬─────┴───────────┴───────────────────┘
       │            │
       ▼            ▼
┌──────────┐  ┌──────────────────┐
│ Azure SQL│  │ Azure CLI / HTTP │
│ medici-db│  │ (az resource,    │
│          │  │  az webapp, etc) │
└──────────┘  └──────────────────┘
```

**Technology Stack:**
- Runtime: .NET 9.0 (ASP.NET Minimal API)
- Database: Azure SQL (Microsoft.Data.SqlClient 5.2.0)
- HTTP Client: RestSharp 112.1.0
- Frontend: Vanilla HTML/CSS/JS (single file, no build step)
- Hosting: Azure App Service (Linux, Free F1 tier)

---

## 3. מבנה קבצים

```
MediciMonitor/
├── Program.cs                    # Entry point, DI, all 14 API routes
├── DataService.cs                # 16 SQL query methods (701 lines)
├── Models.cs                     # All DTOs — 25+ classes (367 lines)
├── MediciMonitor.csproj          # Project file (.NET 9.0)
├── appsettings.json              # Connection string config
├── CHANGELOG.md                  # Version history
├── DOCUMENTATION.md              # ← אתה כאן
│
├── Services/
│   ├── AzureMonitoringService.cs    # API health + Azure resources (226 lines)
│   ├── BusinessIntelligenceService.cs # BI analytics (203 lines)
│   ├── EmergencyResponseService.cs  # Emergency response (195 lines)
│   ├── HistoricalDataService.cs     # Snapshots + trends (252 lines)
│   ├── AlertingService.cs           # Alert rules engine (147 lines)
│   └── InMemoryLogProvider.cs       # Ring buffer logging (185 lines)
│
├── wwwroot/
│   └── index.html                   # Full 12-tab dashboard (1463 lines)
│
├── pub/                             # Published Release output
├── deploy.zip                       # Azure deployment package
└── bin/ obj/                        # Build artifacts
```

**סה"כ:** ~2,500 שורות קוד C# + 1,463 שורות HTML/JS

---

## 4. שירותים (Services)

### 4.1 DataService (DataService.cs)

> **תפקיד:** כל השאילתות ל-Azure SQL. כל method עצמאי עם try/catch.

| Method | מה שואל | טבלאות |
|--------|---------|--------|
| `GetFullStatus()` | Main — מפעיל את כל ה-16 queries | — |
| `LoadBookingSummary` | סה"כ פעילות, תקועות, עתידיות | MED_Book |
| `LoadStuckCancellations` | TOP 50 תקועות + פרטי שגיאה | MED_Book, MED_CancelBookError |
| `LoadCancelStats` | ביטולים/שגיאות 24 שעות | MED_CancelBook, MED_CancelBookError |
| `LoadRecentCancelErrors` | TOP 25 שגיאות ביטול | MED_CancelBookError |
| `LoadBookingErrors` | TOP 25 שגיאות הזמנה + KPI 24h | MED_BookError |
| `LoadPushStatus` | Push פעילים/כושלים + TOP 25 | Med_HotelsToPush |
| `LoadQueueStatus` | Queue pending/errors + TOP 20 | Queue |
| `LoadBackOfficeErrors` | TOP 25 שגיאות BackOffice | BackOfficeOptLog |
| `LoadActiveBookingsByHotel` | הזמנות לפי מלון | MED_Book, Med_Hotels |
| `LoadSalesOfficeStatus` | SalesOffice KPIs + stuck | SalesOfficeOrders / variants |
| `LoadOpportunitiesAndRooms` | הזדמנויות + חדרים היום | BackOfficeOPT, MED_Book |
| `LoadReservations` | NEW: הזמנות Zenith | Med_Reservation, Cancel, Modify |
| `LoadRoomWaste` | NEW: חדרים לא נמכרים | MED_Book, Med_Hotels |
| `LoadConversionRevenue` | NEW: P&L + conversion לפי מלון | MED_Book, Med_Hotels |
| `LoadPriceDrift` | NEW: שינויי מחיר ספק | MED_Book, Med_Hotels |
| `LoadBuyRoomsHeartbeat` | NEW: heartbeat BuyRooms | MED_Book, MED_PreBook |

**חשוב:** כל method עטוף ב-`Safe()` — כשל אחד לא מוריד את כל ה-Dashboard.

### 4.2 AzureMonitoringService (Services/AzureMonitoringService.cs)

> **תפקיד:** בדיקת בריאות APIs + סטטוס Azure דרך CLI.

**Endpoints נבדקים:**
1. `medici-backend.azurewebsites.net/healthcheck` — Production Backend
2. `medici-backend.azurewebsites.net/ZenithApi/HelloZenith` — Zenith API
3. `medici-backend-dev-*.azurewebsites.net/` — Dev Backend
4. `medici-backend.azurewebsites.net/swagger` — API Docs
5. `login.microsoftonline.com` — Azure AD
6. TCP check: `medici-sql-server.database.windows.net:1433` — SQL Server

**Azure CLI Commands:**
- `az resource list` — כל המשאבים
- `az webapp list` — סטטוס Web Apps
- `az sql db list` — סטטוס Databases
- `az monitor activity-log list` — התראות
- `az monitor metrics list` — מדדי ביצועים

### 4.3 BusinessIntelligenceService (Services/BusinessIntelligenceService.cs)

> **תפקיד:** BI Analytics לפי תקופה + חיזויים + תובנות.

**Periods:** today, yesterday, week, month

**מה מחושב:**
- Total/Successful/Failed bookings
- Success rate
- Errors & Cancel errors
- Revenue from reservations
- Hourly breakdown (peak hour detection)
- Top 10 errors
- Predictive alerts (performance degradation, error patterns, low activity)
- Insights & Recommendations (Hebrew)

### 4.4 EmergencyResponseService (Services/EmergencyResponseService.cs)

> **תפקיד:** הערכת סיכונים + 6 פעולות חירום.

**Severity Levels:**
| Level | Status | תיאור |
|-------|--------|--------|
| 5 | CRITICAL | דרושה פעולה מיידית |
| 4 | MAJOR | דרושה תשומת לב דחופה |
| 3 | MODERATE | ניטור צמוד |
| 2 | MINOR | פעולה תקינה |
| 1 | OPTIMAL | כל המערכות בריאות |

**פעולות חירום:**
1. `RESTART_MONITORING` — אתחול שירותי ניטור
2. `TEST_ALL_CONNECTIONS` — בדיקת כל החיבורים
3. `HEALTH_CHECK_CYCLE` — 3 סבבי בדיקת בריאות
4. `CLEAR_TEMP_CACHE` — ניקוי מטמון
5. `EMERGENCY_BACKUP` — גיבוי חירום לדיסק
6. `NOTIFY_ADMIN` — התראה למנהל (דורש אישור)

### 4.5 HistoricalDataService (Services/HistoricalDataService.cs)

> **תפקיד:** Snapshots אוטומטיים כל 15 דקות + ניתוח מגמות.

**Auto-Capture:** מופעל אוטומטית ב-startup, כל 15 דקות שומר snapshot ל-JSON.

**נתיבי אחסון:**
- Azure: `/home/MediciMonitor/HistoricalData/`
- Local: `%LocalAppData%/MediciMonitor/HistoricalData/`
- Fallback: `%TEMP%/`

**Snapshots כוללים:** BookingsCount, ErrorsCount, CancelErrors, ApiHealthRatio, AvgResponseTime, DbConnected, OverallStatus

**Trend Analysis:** חישוב מגמה (עולה/יורדת/יציבה) על בסיס שליש ראשון מול שליש אחרון.

### 4.6 AlertingService (Services/AlertingService.cs)

> **תפקיד:** 7 כללי התראה עם severity.

| Rule ID | Title | Condition | Severity |
|---------|-------|-----------|----------|
| DB_DOWN | DB Down | לא ניתן להתחבר | Critical |
| API_DOWN | API Down | Endpoints לא פעילים | Critical |
| SLOW_API | Slow APIs | Response > 5 שניות | Warning |
| STUCK_CANCEL | Stuck | > 10 תקועות | Warning |
| ERR_SPIKE | Error Spike | > 5 שגיאות/שעה | Warning |
| NO_BOOKINGS | No Bookings | 0 הזמנות בשעות עבודה | Info |
| QUEUE_ERR | Queue Errors | > 3 שגיאות Queue/שעה | Warning |

### 4.7 InMemoryLogProvider (Services/InMemoryLogProvider.cs)

> **תפקיד:** Ring buffer (2000 entries) + Rolling file log + ILogger integration.

**Classes:**
- `LogEntry` — Timestamp, Level, Category, Message, Exception
- `LogBuffer` — Thread-safe ring buffer with Query support
- `InMemoryLoggerProvider` — ILoggerProvider implementation
- `InMemoryLogger` — ILogger with buffer + file write

**Query Support:** Filter by level, category, search text, last N entries, since DateTime.

---

## 5. API Endpoints

### Business Data
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/api/status` | **Main endpoint** — כל ה-KPIs + רשימות מפורטות (16 queries) |

### Azure Monitoring
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/api/azure/health` | בדיקת בריאות 5 APIs + SQL TCP |
| GET | `/api/azure/resources` | סטטוס משאבי Azure (via CLI) |
| GET | `/api/azure/alerts` | Activity Log alerts |
| GET | `/api/azure/performance` | CPU, Memory, Requests metrics |

### Business Intelligence
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/api/bi/{period?}` | BI — today/yesterday/week/month |

### Emergency
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/api/emergency/status` | מצב חירום + severity |
| POST | `/api/emergency/action/{type}?confirmed` | ביצוע פעולת חירום |

### Historical
| Method | Route | תיאור |
|--------|-------|--------|
| POST | `/api/history/snapshot` | צילום מצב ידני |
| GET | `/api/history/trends/{period?}` | מגמות — 1h/6h/24h/7d/30d |
| GET | `/api/history/report/{period?}` | דוח ביצועים |

### Alerting
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/api/alerts` | כל ההתראות הפעילות |
| GET | `/api/alerts/summary` | התראות + סיכום טקסט |

### Logs
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/api/logs?level=&category=&search=&last=&since=` | חיפוש לוגים |
| GET | `/api/logs/stats` | סטטיסטיקות לוגים |

### Dashboard
| Method | Route | תיאור |
|--------|-------|--------|
| GET | `/` | Redirect → `/index.html` |

---

## 6. מודלים (Models)

### Business Models
| Model | תיאור |
|-------|--------|
| `SystemStatus` | Main DTO — כל ה-KPIs + רשימות (80+ שדות) |
| `StuckBookingInfo` | הזמנה תקועה + פרטי שגיאה |
| `CancelErrorInfo` | שגיאת ביטול |
| `BookingErrorInfo` | שגיאת הזמנה |
| `PushFailureInfo` | כשל Push |
| `QueueErrorInfo` | שגיאת Queue |
| `BackOfficeErrorInfo` | שגיאת BackOffice |
| `SalesOfficeOrderInfo` | הזמנת SalesOffice |
| `ActiveBookingSummary` | סיכום הזמנות לפי מלון |
| `ReservationInfo` | הזמנת Zenith (New/Cancel/Modify) |
| `RoomWasteInfo` | חדר לא נמכר |
| `PriceDriftInfo` | שינוי מחיר ספק |
| `ConversionByHotelInfo` | Conversion לפי מלון |

### Azure/BI/Emergency Models
| Model | תיאור |
|-------|--------|
| `ApiHealthStatus` | בריאות endpoint |
| `AzureResourceStatus` | סטטוס משאב Azure |
| `AzureAlert` | התראת Azure |
| `AzurePerformanceMetrics` | מדדי ביצועים |
| `BIMetrics` | מדדי BI |
| `HourlyBI` | פירוט שעתי |
| `PredictiveAlert` | התראה חזויה |
| `EmergencyStatus` | מצב חירום |
| `EmergencyAction` | פעולת חירום |
| `HistoricalSnapshot` | צילום מצב |
| `AlertInfo` | התראה |

---

## 7. Dashboard — 12 לשוניות

| # | Tab | ID | מה מציג |
|---|-----|-----|---------|
| 1 | **Overview** | panel-overview | KPI cards: Active/Stuck/Upcoming/Future bookings, Errors, Push/Queue, SalesOffice + טבלאות מפורטות |
| 2 | **Reservations** | panel-reservations | Zenith cockpit: Today/Week bookings, cancels, modifies, revenue + טבלת הזמנות אחרונות |
| 3 | **Room Waste** | panel-roomwaste | חדרים לא נמכרים: Total/Value/Expiring 24h/48h + טבלה עם countdown |
| 4 | **Conversion** | panel-conversion | P&L: Bought/Sold/Rate/BoughtValue/SoldValue/Profit + per-hotel table |
| 5 | **Price Drift** | panel-pricedrift | סטיות מחיר: Count/TotalImpact + top drifts table |
| 6 | **Errors** | panel-errors | Booking/Cancel/BackOffice/Push/Queue errors — all tables |
| 7 | **Azure Monitor** | panel-azure | API health matrix, Azure resources, alerts, performance |
| 8 | **BI Analytics** | panel-bi | Period selector, KPIs, hourly chart, top errors, insights, predictions |
| 9 | **Emergency** | panel-emergency | Severity indicator, critical issues, 6 action buttons, API health detail |
| 10 | **Historical** | panel-historical | Snapshot capture, trend charts, period selector |
| 11 | **Alerting** | panel-alerting | Active alerts by severity, summary text |
| 12 | **Logs** | panel-logs | Real-time logs viewer with level/category/search filters, stats |

---

## 8. מערכת Logging

### ארכיטקטורה
```
ILogger → InMemoryLogger → LogBuffer (ring buffer, 2000 entries)
                         → WriteToFile (rolling daily log)
```

### API
```
GET /api/logs?level=Error&category=DataService&search=timeout&last=50&since=2026-02-24
GET /api/logs/stats
```

### נתיבי Log Files
- **Azure:** `/home/LogFiles/MediciMonitor/medici-monitor-YYYYMMDD.log`
- **Local:** `{AppDir}/Logs/medici-monitor-YYYYMMDD.log`

### מה נרשם
- כל שגיאות SQL מ-DataService (method name + error message)
- Azure CLI errors
- Emergency actions
- Auto-capture events
- BI analysis events
- HTTP middleware events

---

## 9. Azure Deployment

### פרטי Deployment
| Parameter | Value |
|-----------|-------|
| **URL** | https://medici-monitor-dashboard.azurewebsites.net |
| **App Name** | medici-monitor-dashboard |
| **Resource Group** | Medici-RG |
| **Location** | West Europe |
| **SKU** | F1 (Free tier) |
| **OS** | Linux |
| **Runtime** | DOTNETCORE:9.0 |
| **Startup** | `dotnet MediciMonitor.dll` |

### App Settings
| Setting | Value |
|---------|-------|
| `WEBSITE_RUN_FROM_PACKAGE` | 1 |
| `SCM_DO_BUILD_DURING_DEPLOYMENT` | false |
| `WEBSITES_PORT` | 8080 |

### Connection String
| Name | Type | Value |
|------|------|-------|
| SqlServer | SQLAzure | `Server=tcp:medici-sql-server.database.windows.net,1433;Initial Catalog=medici-db;User Id=medici_sql_admin;Password=@Amit2025;Encrypt=True;...` |

### פקודות Deploy
```powershell
# 1. Build
cd "C:\Users\97250\Desktop\booking engine\MediciMonitor"
dotnet publish -c Release -o ./pub

# 2. Zip
Remove-Item deploy.zip -ErrorAction SilentlyContinue
Compress-Archive -Path ./pub/* -DestinationPath deploy.zip -Force

# 3. Deploy
az webapp deploy --resource-group Medici-RG --name medici-monitor-dashboard --src-path deploy.zip --type zip --clean true

# 4. Restart (if needed)
az webapp restart --name medici-monitor-dashboard --resource-group Medici-RG
```

### בעיות ידועות ופתרונות

| בעיה | סיבה | פתרון |
|------|------|--------|
| 503 Service Unavailable | App crash at startup | בדוק Docker logs ב-Kudu |
| Read-only filesystem | `WEBSITE_RUN_FROM_PACKAGE=1` | כתיבה רק ל-`/home/` |
| QuotaExceeded (F1) | CPU limit 60 min/day | שדרג ל-B1 ($13/mo) |
| Startup timeout | Container takes >30s | Endpoint `/api/logtest` מוחזר מהר |

---

## 10. הרצה מקומית

```powershell
# Prerequisites: .NET 9.0 SDK

cd "C:\Users\97250\Desktop\booking engine\MediciMonitor"

# Run (port 5066)
dotnet run

# Or build + run
dotnet build
dotnet run --no-build

# Access
# Dashboard: http://localhost:5066
# API:      http://localhost:5066/api/status
# Logs:     http://localhost:5066/api/logs/stats
```

**Local Connection:** Uses `appsettings.json` connection string directly.  
**Azure Connection:** Uses Environment connection string set via `az webapp config connection-string set`.

---

## 11. שאילתות SQL

### טבלאות שנשאלות (Read Only)

| טבלה | שימוש |
|------|-------|
| `MED_Book` | הזמנות פעילות, P&L, waste, drift, heartbeat |
| `MED_BookError` | שגיאות הזמנה |
| `MED_CancelBook` | ביטולים מוצלחים |
| `MED_CancelBookError` | שגיאות ביטול |
| `MED_PreBook` | BuyRooms heartbeat |
| `Med_Hotels` | שמות מלונות (JOIN) |
| `Med_HotelsToPush` | Push operations |
| `Queue` | Queue status |
| `BackOfficeOPT` | הזדמנויות |
| `BackOfficeOptLog` | שגיאות BackOffice |
| `SalesOfficeOrders` | SalesOffice status (3 variant names) |
| `Med_Reservation` | הזמנות Zenith (New) |
| `Med_ReservationCancel` | ביטולי Zenith |
| `Med_ReservationModify` | שינויי Zenith |

### Timeouts
- Default: 30 seconds
- SalesOffice: 5 seconds (quick fail)
- Reservation: 15 seconds
- Room Waste: 15 seconds
- Conversion: 30 seconds

---

## 12. פתרון בעיות

### שגיאת חיבור DB
```powershell
# בדוק connection string
az webapp config connection-string list --name medici-monitor-dashboard --resource-group Medici-RG
```

### שגיאת 503
```powershell
# הורד Docker logs
az webapp log download --name medici-monitor-dashboard --resource-group Medici-RG --log-file logs.zip

# חפש "Unhandled exception" ב-*default_docker.log
```

### Quota חרוגה (F1)
```powershell
# בדוק מצב
az webapp show --name medici-monitor-dashboard --resource-group Medici-RG --query "{state:state,usageState:usageState}"

# שדרג אם צריך
az appservice plan update --name medici-monitor-dashboard --resource-group Medici-RG --sku B1
```

### הרצה מקומית לא עובדת
```powershell
# בדוק שיש .NET 9
dotnet --version

# בדוק שהקובץ appsettings.json קיים עם connection string
cat appsettings.json
```

---

## נספח: גיבוי ושחזור

### גיבוי
```powershell
# גיבוי קוד מקור
Compress-Archive -Path Program.cs,DataService.cs,Models.cs,MediciMonitor.csproj,appsettings.json,CHANGELOG.md,Services,wwwroot -DestinationPath "MediciMonitor-Backup-$(Get-Date -Format 'yyyyMMdd').zip"
```

### שחזור
```powershell
# שחזור מגיבוי
Expand-Archive -Path MediciMonitor-Backup-YYYYMMDD.zip -DestinationPath ./MediciMonitor

# Deploy לאחר שחזור
cd MediciMonitor
dotnet publish -c Release -o ./pub
Compress-Archive -Path ./pub/* -DestinationPath deploy.zip
az webapp deploy --resource-group Medici-RG --name medici-monitor-dashboard --src-path deploy.zip --type zip --clean true
```

---

*תיעוד זה נוצר אוטומטית ב-2026-02-24. גרסה 2.1.*
