# Azure Deploy Checklist — MediciMonitor

תאריך עדכון: 2026-03-05

## 1) לפני פריסה

- ודא שהקוד כולל את:
  - `CANCEL_RETRY_LOOP`
  - `SOLD_CANCEL_RISK`
  - נתיבים: `/api/alerts/thresholds`, `/api/notifications/config`
- עדכן `appsettings`/App Settings עם `Notifications` ו-`AlertThresholds`.
- שמור סודות (`SmtpPass`, Webhooks) רק ב-Azure App Settings או Key Vault.

## 2) פריסה

- פרוס את ה-build האחרון ל-App Service.
- בצע Restart ל-App Service.

## 3) בדיקות Smoke אחרי פריסה

הרץ:

`bash scripts/monitor-smoke-test.sh https://medici-monitor-dashboard.azurewebsites.net`

תוצאה תקינה צפויה:

- `GET /healthz` -> `200`
- `GET /api/alerts` -> `200`
- `GET /api/alerts/thresholds` -> `200`
- `GET /api/notifications/config` -> `200`

אם אחד מהנתיבים מחזיר `404`, לרוב זו אינדיקציה שגרסה ישנה עדיין רצה.

## 4) אימות התראות בפועל

לאחר שהנתיבים תקינים:

- הפעל test notification: `POST /api/notifications/test`
- אמת שהגיעה הודעה במייל/Slack.
- הפעל חירום יזום: `POST /api/emergency/action/NOTIFY_ADMIN?confirmed=true`

## 5) אם עדיין לא מעודכן

- ודא slot/source נכון בפריסה.
- ודא שה-artifact תואם לקומיט האחרון.
- ודא שלא קיים cache/sticky session לגרסה קודמת.
- בצע restart נוסף ובדוק שוב עם סקריפט smoke.
