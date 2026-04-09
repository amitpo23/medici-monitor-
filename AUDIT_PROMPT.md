# MediciMonitor — Audit & Verification Prompt

העתק את כל הפרומפט הזה ל-Claude Code והרץ אותו. הוא יבדוק את כל הממצאים מהניתוח.

---

```
אתה מבצע ביקורת אימות (verification audit) על מערכת MediciMonitor. בוצע ניתוח מקיף שזיהה 32 המלצות ב-7 תחומים. המשימה שלך: לסרוק את הקוד בעצמך ולאמת או להפריך כל ממצא. אל תשנה שום דבר — רק בדוק ודווח.

## הוראות

סרוק את כל הקבצים בפרויקט ובדוק כל אחד מהממצאים הבאים. לכל ממצא כתוב: ✅ מאומת / ❌ לא מדויק / ⚠️ חלקית — עם הסבר קצר וציטוט שורה רלוונטית מהקוד.

---

## ממצא 1: Fallback חסר להודעות לא מזוהות
**טענה:** כשהמשתמש שולח הודעה שלא תואמת לשום pattern ב-HandleNaturalLanguage, ההודעה נבלעת בשקט ואין תגובה.
**מה לבדוק:** בקובץ `Services/TelegramBotService.cs`, פונקציית `HandleNaturalLanguage`. האם יש default/else/fallback בסוף הפונקציה? האם יש תגובה למשתמש כשכלום לא תואם?

## ממצא 2: chatId יחיד ללא Multi-Chat
**טענה:** הבוט עובד עם `Notifications:TelegramChatId` יחיד. אין תמיכה בקבוצות מרובות עם הרשאות שונות.
**מה לבדוק:** ב-`TelegramBotService.cs`, האם `_chatId` הוא string יחיד? האם `SendToGroup` שולח תמיד ל-`_chatId` כברירת מחדל? האם יש מנגנון multi-chat?

## ממצא 3: Authentication מבוסס first_name בלבד
**טענה:** זיהוי המשתמש הוא `from.first_name` בלבד, ללא בדיקת `userId`. אין whitelist של משתמשים מורשים. אין Role-Based Access.
**מה לבדוק:** ב-`PollCommands`, מה מאוחסן ב-`from`? האם יש בדיקת `user_id`? האם יש dictionary של roles? האם פקודות רגישות (killswitch, pnl, cancel) בודקות הרשאות?

## ממצא 4: Kill Switch לא מסונכרן ל-Backend
**טענה:** Circuit Breakers ב-`FailSafeService` הם in-memory בלבד. ה-Backend (medici-backend) לא יכול לבדוק את מצב ה-breakers לפני ביצוע פעולות.
**מה לבדוק:** ב-`Services/FailSafeService.cs`, האם `TripBreaker` כותב ל-DB? האם יש טבלת `Monitor_CircuitBreakers`? ב-`DataService.cs`, האם יש methods של `SyncBreakerToDb` או דומה? ב-`Program.cs`, האם יש endpoint `/api/failsafe/breakers/check` שה-backend יכול לקרוא ממנו?

## ממצא 5: רק 4 מתוך 9 סוכנים מנוטרים פרואקטיבית
**טענה:** `AgentProactivityService` מריץ `CheckShimon`, `CheckAmir`, `CheckMichael`, `CheckAryeh` בלבד. אין Check ל-יוסי, יעל, רוני, דני, גבי.
**מה לבדוק:** ב-`Services/AgentProactivityService.cs`, פונקציית `ExecuteAsync`. אילו Check methods נקראים? האם יש methods עבור 5 הסוכנים הנותרים?

## ממצא 6: אין מעקב למידה והתקדמות סוכנים
**טענה:** אין היסטוריית שינויים של KPIs של סוכנים. ה-state tracking ב-AgentProactivityService שומר רק ערך אחרון (lastThreatLevel_, lastMissRate_, וכו') ללא היסטוריה.
**מה לבדוק:** ב-`AgentProactivityService.cs`, האם יש שמירה ל-DB או לקובץ של שינויים? האם יש טבלת Monitor_AgentHistory? האם יש פקודת /agent_progress בבוט?

## ממצא 7: Bot State לא persistent
**טענה:** המשתנים `_watchEnabled`, `_muteUntil`, `_pauseUntil`, `_scheduledCommands`, `_oncallName` ב-TelegramBotService הם in-memory בלבד. Restart מאפס הכל.
**מה לבדוק:** ב-`TelegramBotService.cs`, האם יש שמירה/טעינה של המשתנים הללו ל-file או DB? האם יש constructor שטוען state? השווה ל-`FailSafeService` שכן שומר state ל-`failsafe-state.json`.

## ממצא 8: TelegramBotService הוא God Class
**טענה:** הקובץ הוא 2,643 שורות עם 67+ handlers, dashboards, agent chat, ועוד — הכל בקובץ אחד.
**מה לבדוק:** ספור את מספר השורות בקובץ. ספור כמה methods יש. האם יש חלוקה ל-partial classes או modules?

## ממצא 9: התראות בטלגרם חד-כיווניות
**טענה:** כשנשלחת התראה לטלגרם, אין כפתורי Acknowledge או Snooze בהודעה עצמה. צריך לכתוב /alerts ולחפש.
**מה לבדוק:** ב-`Services/AlertNotificationService.cs` או `NotificationService.cs`, כשנשלחת התראה לטלגרם — האם יש inline_keyboard עם כפתורי Ack/Snooze? או שזו הודעת טקסט רגילה?

## ממצא 10: Agent Chat — RouteToAgent מבוסס keyword בסיסי
**טענה:** הפונקציה `RouteToAgent` ב-TelegramBotService מבוססת על keyword matching פשוט ולא על הבנת הקשר או AI.
**מה לבדוק:** ב-`TelegramBotService.cs`, פונקציית `RouteToAgent`. האם זה if/switch על מילות מפתח? או שיש AI-based routing?

## ממצא 11: שיחות סוכנים — Conversation Timeout 30 דקות
**טענה:** שיחה עם סוכן נסגרת אוטומטית אחרי 30 דקות ללא פעילות, ואין סיכום אוטומטי כשהשיחה נסגרת.
**מה לבדוק:** ב-`ClaudeAiService.cs`, הקבוע `ConversationTimeout`. מה הערך? מה קורה כשהשיחה פגה? האם יש שמירת סיכום?

## ממצא 12: דשבורדים טקסטיים בלבד
**טענה:** 3 הדשבורדים (Agents, Sales, Risks) נשלחים כטקסט Markdown בלבד, ללא גרפים או תמונות.
**מה לבדוק:** ב-`TelegramBotService.cs`, פונקציות `SendDashboardAgents`, `SendDashboardSales`, `SendDashboardRisks`. האם יש sendPhoto? או רק sendMessage עם טקסט?

## ממצא 13: Telegram Polling כל 30 שניות
**טענה:** הבוט משתמש ב-getUpdates polling כל 30 שניות, לא ב-Webhook.
**מה לבדוק:** ב-`ExecuteAsync`, האם יש `Task.Delay(TimeSpan.FromSeconds(30))`? האם יש הגדרת Webhook? האם יש endpoint שמקבל updates מטלגרם?

## ממצא 14: Quiet Hours קיימים בNotificationService אבל לא בבוט
**טענה:** `NotificationService` תומך ב-Quiet Hours (23:00-07:00), אבל הדשבורדים השעתיים ודוחות אוטומטיים נשלחים 24/7.
**מה לבדוק:** ב-`TelegramBotService.cs`, האם יש בדיקת quiet hours לפני שליחת דשבורדים אוטומטיים בשעות 23:00-07:00? או שהם נשלחים תמיד?

## ממצא 15: /cancel לא מבטל בפועל
**טענה:** פקודת `/cancel` רק רושמת בקשת ביטול ב-Audit אבל לא שולחת Cancel request ל-Innstant API.
**מה לבדוק:** ב-`TelegramBotService.cs`, פונקציית `HandleCancelBooking`. האם יש קריאה ל-API חיצוני? או שזה רק audit log + הודעה?

---

## פורמט הדוח

כתוב דוח בפורמט הבא:

```
# דוח אימות ביקורת — MediciMonitor
תאריך: [תאריך]

## תוצאות

| # | ממצא | סטטוס | הערות |
|---|-------|--------|-------|
| 1 | Fallback חסר | ✅/❌/⚠️ | [ציטוט קוד] |
| ... | ... | ... | ... |

## סיכום
- מאומתים: X/15
- לא מדויקים: X/15
- חלקיים: X/15

## ממצאים נוספים
[אם מצאת בעיות שלא זוהו בניתוח המקורי, רשום אותן כאן]
```

חשוב: קרא את הקבצים בעצמך, אל תסתמך על תיאורים. ציין מספרי שורות ספציפיים.
```
