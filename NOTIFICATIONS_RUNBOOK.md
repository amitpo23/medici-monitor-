# MediciMonitor Notifications Runbook

תאריך עדכון: 2026-03-03

## 1) מטרת המסמך

להפעיל התראות אמיתיות (Email/Slack/Teams/Webhook), לוודא שההתראות אכן נשלחות, ולהבטיח שמקרי כשל כמו retry loop או sold bookings בסיכון לא "נופלים בין הכיסאות".

---

## 2) קבצי קונפיגורציה

- תבנית Production: `appsettings.Production.template.json`
- קובץ פעיל בסביבה: `appsettings.json` או משתני סביבה מקבילים ב-Azure App Service

מומלץ לא לשמור סיסמאות בקובץ Git. ב-Production להשתמש ב-Application Settings/Key Vault.

---

## 3) Minimum Config (כדי לקבל התראות אמיתיות)

### Email (SMTP)

להגדיר:

- `Notifications.EmailEnabled = true`
- `Notifications.SmtpHost`
- `Notifications.SmtpPort`
- `Notifications.SmtpUser`
- `Notifications.SmtpPass`
- `Notifications.SmtpFrom`
- `Notifications.EmailRecipients` (רשימה מופרדת בפסיקים)

דוגמה לנמענים:

`zvi.g@medicihotels.com,ops@medicihotels.com`

### Slack (מומלץ בנוסף לאימייל)

- `Notifications.SlackEnabled = true`
- `Notifications.SlackWebhookUrl = https://hooks.slack.com/services/...`

### סף התראות

- `Notifications.MinSeverity = Warning`  
  (Critical + Warning יישלחו)

---

## 4) התראות חדשות שמכסות את הכשל שתואר

1. `CANCEL_RETRY_LOOP` (Critical)  
   מזהה מצב שבו אותן הזמנות נכשלות שוב ושוב ב-`MED_CancelBookError` וחוסמות את מחזורי ה-WebJob.

2. `SOLD_CANCEL_RISK` (Warning/Critical)  
   מזהה הזמנות Sold פעילות שעברו/מתקרבות ל-`CancellationTo` ללא record הצלחה ב-`MED_CancelBook`.

Thresholds ניתנים לכיול תחת `AlertThresholds`.

---

## 5) בדיקות הפעלה (Post-Deploy Checklist)

להריץ לפי הסדר:

1. קבלת קונפיגורציה פעילה:

```bash
curl -s https://<monitor-host>/api/notifications/config
```

2. בדיקת שליחת הודעת test:

```bash
curl -s -X POST https://<monitor-host>/api/notifications/test
```

תוצאה צפויה: `Channels` עם לפחות ערוץ אחד מצליח מעבר ל-`Log`.

3. בדיקת סטטוס התראות:

```bash
curl -s https://<monitor-host>/api/alerts
```

4. בדיקת היסטוריית התראות שנשלחו:

```bash
curl -s "https://<monitor-host>/api/notifications/history?last=20"
```

5. בדיקת פעולת חירום אמיתית:

```bash
curl -s -X POST "https://<monitor-host>/api/emergency/action/NOTIFY_ADMIN?confirmed=true"
```

תוצאה צפויה: `Success=true` וערוצי שליחה מוצלחים.

---

## 6) Troubleshooting מהיר

### `Success=false` ב-`/api/notifications/test`

- לבדוק `SmtpHost/SmtpPort/SmtpUser/SmtpPass`
- לוודא שהשרת SMTP מאפשר relay/login מה-App Service
- לבדוק שרשימת `EmailRecipients` לא ריקה

### Slack לא שולח

- לוודא שה-Webhook תקין ולא בוטל
- לוודא ש-`SlackEnabled=true`

### עדיין אין התראות למרות שיש Alerts

- לבדוק `Notifications.MinSeverity`
- לבדוק שההתראה לא ב-ack/snooze
- לבדוק שקריאה ל-`/api/alerts` אכן מחזירה alerts פעילים

---

## 7) המלצת תפעול

- להפעיל **גם Email וגם Slack**
- להגדיר לפחות 2 נמענים אנושיים קבועים + ערוץ צוות
- לבצע בדיקת `POST /api/notifications/test` פעם ביום (או אחרי כל שינוי קונפיגורציה)
