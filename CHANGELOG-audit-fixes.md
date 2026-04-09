# MediciMonitor — Audit Fixes Changelog

## Version: post-audit-v1 (2026-04-09)

Based on audit report verifying 15 findings (14 confirmed, 1 incorrect).
Fixes below target HIGH and MEDIUM priority items only.
**Rule: no changes to source code of WebServices or medici-backend.**

---

### HIGH Priority Fixes

#### Fix #1 — Fallback for unrecognized messages (Audit Finding 1)
**Problem:** Messages that don't match any pattern in HandleNaturalLanguage are silently swallowed.
**Fix:** Add a default response at the end of HandleNaturalLanguage suggesting /team or /help.
**File:** `Services/TelegramBotService.cs` — end of HandleNaturalLanguage method
**Risk:** None — additive only

#### Fix #14 — Quiet hours for dashboards (Audit Finding 14)
**Problem:** Hourly dashboards send 24/7, including 23:00-07:00 Israel time.
**Fix:** Add quiet hours check before dashboard dispatch in ExecuteAsync loop.
**File:** `Services/TelegramBotService.cs` — dashboard block (~line 127)
**Risk:** None — dashboards skip during quiet hours, manual /dashboard still works

#### Fix #7 — Persistent bot state (Audit Finding 7)
**Problem:** _watchEnabled, _muteUntil, _pauseUntil, _scheduledCommands, _oncallName reset on restart.
**Fix:** Save/load to `bot-state.json` (same pattern as FailSafeService's failsafe-state.json).
**File:** `Services/TelegramBotService.cs` — new SaveBotState/LoadBotState methods
**Risk:** Low — graceful fallback if file missing/corrupt

#### Fix #9 — Ack/Snooze buttons on critical alerts (Audit Finding 9)
**Problem:** Critical alerts sent as plain text, no actionable buttons.
**Fix:** Add inline_keyboard with [Ack] [Snooze 1h] buttons to SendTelegram in NotificationService.
**File:** `Services/NotificationService.cs` — SendTelegram method
**File:** `Services/TelegramBotService.cs` — HandleCallbackQuery for ack:/snooze: callbacks
**Risk:** Low — additive, existing alerts unchanged for non-critical

### MEDIUM Priority Fixes

#### Fix #5 — Add 5 missing proactive agents (Audit Finding 5)
**Problem:** Only שמעון/אמיר/מיכאל/אריה are monitored. Missing: יוסי/יעל/רוני/דני/גבי.
**Fix:** Add CheckYossi (pricing updates), CheckYael (monitor health changes), CheckRoni (completion status), CheckDani (coordinator health), CheckGabi (autofix activity).
**File:** `Services/AgentProactivityService.cs`
**Risk:** None — additive, template-based, no Claude calls

#### Fix #3 — User ID whitelist (Audit Finding 3)
**Problem:** Any user who knows the PIN can execute sensitive commands.
**Fix:** Add authorized_users list in appsettings.json. Check user_id before sensitive commands.
**File:** `Services/TelegramBotService.cs` — PollCommands, extract user_id, check whitelist
**File:** `appsettings.json` — new AuthorizedUsers array
**Risk:** Low — only blocks unauthorized users, existing authorized users unaffected

#### Fix #11 — Extend conversation timeout + closing message (Audit Finding 11)
**Problem:** Conversations expire silently after 30 minutes.
**Fix:** Extend to 60 minutes. Send closing message when conversation expires during next interaction.
**File:** `Services/ClaudeAiService.cs` — ConversationTimeout, cleanup logic
**Risk:** None — longer timeout, better UX

---

### NOT fixing (by design)
- Finding 4 (Kill Switch DB sync) — already implemented correctly
- Finding 8 (God Class) — refactoring risk too high for current phase
- Finding 10 (keyword routing) — mitigated by /team buttons UI
- Finding 12 (text-only dashboards) — nice to have, not critical
- Finding 13 (polling vs webhook) — works fine for current scale
- Finding 15 (/cancel) — requires backend integration, out of scope
