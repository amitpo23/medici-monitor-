#!/usr/bin/env bash
# dev-log.sh — Run at end of session to log what was done today
# Usage: ./dev-log.sh [project-dir]
# Can be called from any machine. Commits & pushes a daily log entry.
set -uo pipefail

PROJECT_DIR="${1:-$(pwd)}"
cd "$PROJECT_DIR" || exit 1

PROJECT=$(basename "$PROJECT_DIR")
TODAY=$(date '+%Y-%m-%d')
NOW=$(date '+%Y-%m-%d %H:%M')
HOSTNAME=$(hostname -s 2>/dev/null || echo "unknown")
USER=$(whoami)
LOG_DIR=".dev-logs"
LOG_FILE="$LOG_DIR/$TODAY.md"

mkdir -p "$LOG_DIR"

# ─── Gather data ───

BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")

# Today's commits
COMMITS=$(git log --since="$TODAY 00:00" --oneline --all 2>/dev/null || echo "")
COMMIT_COUNT=$(echo "$COMMITS" | grep -c . 2>/dev/null || echo "0")

# Files changed today
FILES_CHANGED=$(git diff --stat HEAD~${COMMIT_COUNT:-1} HEAD 2>/dev/null | tail -1 || echo "no changes")

# Uncommitted work
UNCOMMITTED=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
UNCOMMITTED_FILES=$(git status --short 2>/dev/null | head -10)

# Features and fixes
FEATURES=$(git log --since="$TODAY 00:00" --oneline --grep="feat" -i 2>/dev/null || echo "")
FIXES=$(git log --since="$TODAY 00:00" --oneline --grep="fix" -i 2>/dev/null || echo "")

# ─── Write log entry ───

# Append to today's log (supports multiple entries per day from different machines)
cat >> "$LOG_FILE" << ENTRY

---
## Session: $NOW
**Machine:** $HOSTNAME ($USER)
**Branch:** $BRANCH
**Commits today:** $COMMIT_COUNT

### What was done
$(if [ -n "$COMMITS" ] && [ "$COMMITS" != "" ]; then
  echo "$COMMITS" | while read line; do echo "- $line"; done
else
  echo "- No commits today"
fi)

### Features added
$(if [ -n "$FEATURES" ] && [ "$FEATURES" != "" ]; then
  echo "$FEATURES" | while read line; do echo "- $line"; done
else
  echo "- None"
fi)

### Bugs fixed
$(if [ -n "$FIXES" ] && [ "$FIXES" != "" ]; then
  echo "$FIXES" | while read line; do echo "- $line"; done
else
  echo "- None"
fi)

### Stats
- Files changed: $FILES_CHANGED
- Uncommitted files: $UNCOMMITTED

### Uncommitted work
$(if [ "$UNCOMMITTED" -gt 0 ]; then
  echo "\`\`\`"
  echo "$UNCOMMITTED_FILES"
  echo "\`\`\`"
else
  echo "All clean ✅"
fi)

ENTRY

# ─── Add header if it's a new file ───
if ! head -1 "$LOG_FILE" | grep -q "^# "; then
  TEMP=$(mktemp)
  echo "# $PROJECT — Daily Log $TODAY" > "$TEMP"
  echo "" >> "$TEMP"
  cat "$LOG_FILE" >> "$TEMP"
  mv "$TEMP" "$LOG_FILE"
fi

# ─── Commit & push ───
git add "$LOG_DIR/" 2>/dev/null
git commit -m "log: daily dev log $TODAY ($HOSTNAME)" -- "$LOG_DIR/" 2>/dev/null || true
git push origin "$BRANCH" 2>/dev/null || echo "⚠️  Push failed — will sync later"

echo "✅ Logged to $LOG_FILE ($COMMIT_COUNT commits, $UNCOMMITTED uncommitted)"
