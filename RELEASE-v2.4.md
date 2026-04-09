# MediciMonitor v2.4 — Release Notes

**Date:** 2026-04-09
**Commits:** 10 (b0b6e9c → 09b7606)

---

## Overview

Major overhaul of agent communication, real-time alerts, and Telegram UX.
The system is now production-ready for daily use with minimal noise.

---

## New Features

### Agent Chat System
- `/team` command — 9 agent buttons with role descriptions
- Instant agent reports (~100ms, no Claude API needed)
- Claude-powered deep conversations via "שאל שאלה" button
- Agent handoff — when agent mentions another, switch buttons appear
- Agent name detection — write "שמעון מה המצב?" directly
- All agents know the full team directory and can redirect

### Real-Time Alerts
- Critical events (kill switch, breaker trips, FailSafe) → Telegram immediately
- Reconciliation mismatches → instant notification
- **Once per day per alert type** — no spam, no duplicates
- Cooldown persisted to file — survives restarts

### Proactive Agents (9/9)
- שמעון: threat level changes
- אמיר: miss rate thresholds (80%, 90%), failed orders
- מיכאל: new mapping gaps
- יוסי: pricing cycle completions
- יעל: monitor health changes
- רוני: completion cycle completions
- דני: coordinator report updates
- גבי: autofix cycle completions
- אריה: morning summary at 10:00 Israel time

### Dynamic Monitoring
- CRITICAL → scan every 10 min (was 30)
- WARNING → every 20 min
- OK → every 30 min
- 3 consecutive failures → ESCALATION alert

### Enhanced Dashboards (hourly at :03)
- Agent dashboard: per-agent key metrics (miss rate, threat level, gaps)
- Sales dashboard: miss rate, scans data, per-hotel rooms
- Risk dashboard: FailSafe violations, data freshness, trend

---

## UX Improvements

### Conversation Protection
- Auto-mute: all push notifications suppressed during active agent chat
- All services check `IsConversationMuted` before sending to Telegram
- "סיים" button restores notifications

### Simplified Navigation
- `/team` → button → instant report → optional Claude chat
- No complex keyword routing — buttons only
- Fallback for unrecognized messages: 4 action buttons
- Quiet hours (23:00-07:00 Israel) — no dashboards at night

### Reliability
- Bot state persisted to `bot-state.json` (watch, mute, pause, oncall, schedule)
- Alert cooldown persisted to `notification-cooldown.json`
- OAuth auto-refresh every 5 min (30 min before expiry + keychain fallback)
- Conversation timeout extended to 60 minutes

---

## Security
- User ID whitelist for sensitive commands (killswitch, cancel, trip, reset, pause)
- Configured in `Telegram:AuthorizedUsers` (appsettings.json)

---

## Architecture
- TelegramBotService split into 4 partial classes:
  - `.cs` (413 lines) — core, polling, state
  - `.Commands.cs` (1,008 lines) — command handlers
  - `.Dashboards.cs` (656 lines) — reports & dashboards
  - `.AgentChat.cs` (~350 lines) — agent chat, callbacks, keyboards
- AgentProactivityService — background service, template-based (no Claude)
- Daily KPI snapshots saved to `agent-history.json` (90 day retention)

---

## Files Changed
```
Services/TelegramBotService.cs              — core (partial)
Services/TelegramBotService.Commands.cs     — NEW
Services/TelegramBotService.Dashboards.cs   — NEW
Services/TelegramBotService.AgentChat.cs    — NEW
Services/ClaudeAiService.cs                 — agent chat + OAuth refresh
Services/NotificationService.cs             — Telegram alerts + cooldown
Services/SystemMonitorBackgroundService.cs  — dynamic intervals + mute
Services/FailSafeBackgroundService.cs       — dynamic intervals
Services/FailSafeService.cs                 — LastScanResult exposed
Services/ReconciliationBackgroundService.cs — critical mismatch alerts
Services/AgentProactivityService.cs         — NEW (9 agents)
Program.cs                                  — service registration + Telegram config
appsettings.json                            — AuthorizedUsers, Haiku model
```

## Configuration
```json
{
  "Telegram": { "AuthorizedUsers": "6608306461" },
  "Claude": { "Model": "claude-haiku-4-5-20251001" }
}
```
