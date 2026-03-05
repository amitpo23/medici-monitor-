# MediciMonitor — Changelog

## v2.1.3 — 2026-03-05 — Deep SalesOrder Process Mapping

### מה נוסף

- הרחבת `/api/salesorder/diagnostics` למיפוי רחב ומעמיק של כל ה-flow:
	- Pipeline stages מלאים עם רמת סיכון לכל שלב.
	- Breakdown סיבות (Running ללא תוצאה, Stale, Callback issues, Failed/DateRange וכו').
	- Throughput שעתי 24h (Created/Completed/Failed + Details + Callback errors).
- endpoint חדש: `/api/salesorder/trace/{orderId}` ל-trace הזמנה בודדת מקצה לקצה.
- הרחבת לשונית `SalesOrder Monitor` ב-UI עם:
	- טבלת שלבי תהליך,
	- טבלת breakdown סיבות,
	- טבלת throughput 24h,
	- כלי Trace אינטראקטיבי לפי OrderId.

## v2.1.1 — 2026-03-03 — Alerting Hardening & Ops Runbook

### מה נוסף

- חיבור `Notifications` ו-`AlertThresholds` מ-`appsettings` בזמן Startup.
- כלל התראה חדש: `CANCEL_RETRY_LOOP` (Critical) לזיהוי לולאות כשל בביטולים.
- כלל התראה חדש: `SOLD_CANCEL_RISK` (Warning/Critical) להזמנות Sold בסיכון ביטול.
- פעולת חירום `NOTIFY_ADMIN` שולחת לערוצי התראה פעילים במקום לוג בלבד.

### קבצים חדשים

- `appsettings.Production.template.json` — תבנית Production מלאה עם placeholders.
- `NOTIFICATIONS_RUNBOOK.md` — מדריך הפעלה ואימות התראות (Email/Slack/Teams/Webhook).

### הערה תפעולית

- בדיקת build מקומית תלויה ב-`.NET SDK` מותקן (בטרמינל הנוכחי `dotnet` לא זמין).

## v2.1.2 — 2026-03-05 — Standalone SalesOrder Monitor

### מה נוסף

- endpoint חדש: `/api/salesorder/diagnostics` עם תמונת מצב מלאה ל-SalesOrder.
- פאנל עצמאי חדש ב-Dashboard: `SalesOrder Monitor` עם:
	- Running/Pending מול תוצאות מיפוי בזמן אמת.
	- Completed ללא Mapping.
	- Failures + סיבה לכל רשומה.
- כלל התראה חדש: `SO_RUNNING_NO_RESULT` לזיהוי הזמנות רצות ללא תוצאה מעבר לסף זמן.

## v2.0.0 — 2026-02-24 — Unified Operations Center

### מה חדש?
גרסה זו ממזגת את כל יכולות הניטור, BI, חירום, היסטוריה והתראות ממערכת **Medici-Control-Panel** לתוך MediciMonitor.  
MediciMonitor הופך כעת ל**מערכת ניטור אחודה ויחידה** עבור כל תשתית Medici.

---

### שירותים חדשים (5 Services)

| Service | קובץ | יכולות |
|---------|------|--------|
| **AzureMonitoringService** | `Services/AzureMonitoringService.cs` | בדיקת בריאות 5 Endpoints + SQL TCP, סטטוס משאבי Azure (CLI), התראות, מדדי ביצועים |
| **BusinessIntelligenceService** | `Services/BusinessIntelligenceService.cs` | BI Analytics לפי תקופה (today/yesterday/week/month), חיזויים, תובנות, המלצות |
| **EmergencyResponseService** | `Services/EmergencyResponseService.cs` | הערכת סיכונים (severity 0-5), 6 פעולות חירום |
| **HistoricalDataService** | `Services/HistoricalDataService.cs` | Snapshots אוטומטיים כל 15 דקות, ניתוח מגמות, דוחות ביצועים |
| **AlertingService** | `Services/AlertingService.cs` | 7 כללי התראה (DB, API, Slow, Stuck, Errors, No Bookings, Queue) |

### API Endpoints חדשים (12)

| Endpoint | Method | תיאור |
|----------|--------|--------|
| `/api/azure/health` | GET | בדיקת בריאות כל ה-APIs + SQL |
| `/api/azure/resources` | GET | סטטוס משאבי Azure (דורש Azure CLI) |
| `/api/azure/alerts` | GET | התראות מ-Azure Activity Log |
| `/api/azure/performance` | GET | מדדי ביצועים |
| `/api/bi/{period?}` | GET | BI Analytics — period: today/yesterday/week/month |
| `/api/emergency/status` | GET | מצב חירום נוכחי עם severity |
| `/api/emergency/action/{type}` | POST | ביצוע פעולת חירום (TEST_ALL_CONNECTIONS, HEALTH_CHECK_CYCLE, RESTART_MONITORING, CLEAR_TEMP_CACHE, EMERGENCY_BACKUP, NOTIFY_ADMIN) |
| `/api/history/snapshot` | POST | צילום מצב ידני |
| `/api/history/trends/{period?}` | GET | ניתוח מגמות — period: 1h/6h/24h/7d/30d |
| `/api/history/report/{period?}` | GET | דוח ביצועים מפורט |
| `/api/alerts` | GET | כל ההתראות הפעילות |
| `/api/alerts/summary` | GET | סיכום התראות טקסטואלי |

### מודלים חדשים (11 Models)

`ApiHealthStatus`, `AzureResourceStatus`, `AzureAlert`, `AzurePerformanceMetrics`,  
`BIMetrics`, `HourlyBI`, `PredictiveAlert`, `EmergencyStatus`, `EmergencyAction`,  
`HistoricalSnapshot`, `AlertInfo`

### Dashboard — 5 לשוניות חדשות

| Tab | מזהה | תוכן |
|-----|------|-------|
| **Azure Monitor** | `panel-azure` | KPI cards, טבלת בריאות API, משאבי Azure, התראות |
| **BI Analytics** | `panel-bi` | KPIs ביצועיים, גרף שעתי, שגיאות מובילות, תובנות, חיזויים |
| **Emergency** | `panel-emergency` | מצב חומרה, בעיות קריטיות, 6 כפתורי פעולה, בריאות API מפורטת |
| **Historical** | `panel-history` | מגמות לפי תקופה, צילום מצב ידני, דוח ביצועים |
| **Alerting** | `panel-alerting` | התראות פעילות עם חומרה, סיכום טקסטואלי |

### NuGet חדש

- **RestSharp 112.1.0** — לבדיקות HTTP Health Check

### תיקון באג
- `HistoricalDataService.Cleanup()` — הוסר `async` מיותר (CS1998 warning)

---

## v1.0.0 — 2026-02-24 — Initial Standalone Release

### מנוע
- .NET 9.0 Minimal API
- Microsoft.Data.SqlClient 5.2.0
- Self-contained HTML dashboard (wwwroot/index.html)
- Port 5066

### שירותים
- **DataService** — 16 שאילתות SQL (הזמנות, ביטולים, Push, Queue, SalesOffice, BI)

### API Endpoint
- `/api/status` — GET — כל הנתונים העסקיים ב-JSON אחד

### Dashboard — 6 לשוניות
1. **Overview** — KPIs, Heartbeat, SalesOffice, Stuck, Hotel Chart
2. **Reservations** — כל ההזמנות/ביטולים/שינויים
3. **Room Waste** — חדרים לא נמכרים + דחיפות
4. **Conversion** — המרה, רווח/הפסד לפי מלון
5. **Price Drift** — סטיות מחיר
6. **Errors** — שגיאות ביטול, הזמנות, Push, BackOffice, Queue

### 13 Model Classes
`DashboardData`, `StuckBooking`, `ActiveByHotel`, `SalesOfficeOrder`,  
`ReservationItem`, `WasteRoom`, `ConversionByHotel`, `PriceDriftItem`,  
`CancelErrorItem`, `BookingErrorItem`, `PushErrorItem`, `BackOfficeErrorItem`, `QueueErrorItem`

---

## קבצים בפרויקט

```
MediciMonitor/
├── MediciMonitor.csproj          # .NET 9.0, SqlClient, RestSharp
├── Program.cs                     # Entry point, DI, 13 API routes
├── appsettings.json               # Connection string
├── DataService.cs                 # 16 SQL query methods
├── Models.cs                      # 24 model classes (13 + 11)
├── Services/
│   ├── AzureMonitoringService.cs  # API health, Azure resources, alerts
│   ├── BusinessIntelligenceService.cs  # BI analytics
│   ├── EmergencyResponseService.cs     # Risk assessment, emergency actions
│   ├── HistoricalDataService.cs        # Auto-snapshots, trends
│   └── AlertingService.cs              # 7 alert rules
├── wwwroot/
│   └── index.html                 # Self-contained dashboard (11 tabs)
└── CHANGELOG.md                   # This file
```

## חיבורים

| משאב | כתובת |
|------|--------|
| Azure SQL | `medici-sql-server.database.windows.net` |
| Database | `medici-db` |
| Auth | SQL Authentication (`medici_sql_admin`) |
| Port | `http://localhost:5066` |

## הפעלה

```bash
cd MediciMonitor
dotnet run --urls=http://localhost:5066
```
