# MediciMonitor v2.3 — Changelog

## Context
Improvements based on audit findings and operational experience.
No changes to .csproj, WebService backend, or medici-hotels project files.

---

## Changes

### 1. TelegramBotService — Partial Class Split (Finding #8)
**Problem:** 2,600+ lines, 66 methods, single file — unmaintainable.
**Fix:** Split into partial classes by domain:
- `TelegramBotService.cs` — Core: polling, ExecuteAsync, SendToGroup, state
- `TelegramBotService.Commands.cs` — All /command handlers
- `TelegramBotService.Dashboards.cs` — 3 dashboards + hourly/daily/weekly reports
- `TelegramBotService.AgentChat.cs` — Agent detection, routing, chat, /team, callbacks
- `TelegramBotService.NaturalLanguage.cs` — HandleNaturalLanguage, kill switch patterns

**Risk:** Zero — partial classes compile to same class, no behavior change.

### 2. Add 3 Missing Proactive Agents (Finding #5 continued)
**Problem:** רוני, דני, גבי still not monitored proactively.
**Fix:** Add CheckRoni (completion status changes), CheckDani (coordinator health), CheckGabi (autofix activity).
**File:** `Services/AgentProactivityService.cs`

### 3. Agent History Tracking (Finding #6)
**Problem:** No historical trend of agent KPIs — only last value tracked.
**Fix:** New lightweight file-based history: `agent-history.json` — saves daily snapshots of key metrics per agent.
**File:** `Services/AgentProactivityService.cs` — new SaveDailySnapshot method

### 4. Conversation Expiry Notification (Finding #11 continued)
**Problem:** When conversation expires, user gets no message.
**Fix:** On next message after expiry, tell user "השיחה עם X פגה, פותח שיחה חדשה".
**File:** `Services/TelegramBotService.AgentChat.cs`

---

## Not Changed (by design)
- MediciMonitor.csproj — no changes
- WebService/backend code — no changes
- medici-hotels project — no changes
- appsettings.json structure — no changes (only runtime config)
